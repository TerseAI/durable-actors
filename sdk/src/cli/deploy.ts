import { Command } from "commander"
import { spawn } from "node:child_process"
import { randomUUID } from "node:crypto"
import { mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import { z } from "zod"

import { fetchRuntimeExecutablePath } from "../runtimeInstaller.js"

import { type ControlPlaneOptions, createControlPlaneClient } from "./control-plane.js"

interface DeployOptions extends ControlPlaneOptions {
    image: string
    revision?: string
    config?: string
    secret: string[]
    region: string
}

function registerDeployCommand(program: Command): void {
    program
        .command("deploy [entrypoint]")
        .description("Compile and publish customer code, then register it with the generic Bun/Rust runtime")
        .requiredOption("--image <reference>", "published generic runtime image reference")
        .option("--revision <revision>", "code revision (defaults to a new generated ID)")
        .option("--config <file>", "local TypeScript configuration file")
        .option("--region <region>", "region for publishing the code snapshot", "north-america-east")
        .option("--url <origin>", "control-plane origin (or DURABLE_OBJECT_CONTROL_PLANE_URL)")
        .option("--api-key <key>", "admin API key (or DURABLE_OBJECT_API_KEY)")
        .option(
            "--secret <name>",
            "Modal secret reference (uses an on-demand sandbox)",
            (value: string, previous: string[]) => [...previous, value],
            []
        )
        .action(deploy)
}

async function deploy(entrypoint = "src/durable-objects.ts", options: DeployOptions): Promise<void> {
    const client = createControlPlaneClient(options, fetch)
    const specification = deploymentSchema.parse({
        codeRevision: options.revision ?? randomUUID(),
        imageRef: options.image,
        workingDirectory: "/customer",
        actorEntrypoint: "actors.mjs",
        secretRefs: options.secret
    })
    const { ActorCompiler } = await import("../compiler/actor-compiler.js")
    const { buildActor } = await import("../compiler/actor-build.js")
    const { parsePublicContract } = await import("../compiler/validate-public-contract.js")
    const contract = parsePublicContract(
        new ActorCompiler().compileContract(entrypoint, { configFile: options.config })
    )
    const directory = await mkdtemp(path.join(tmpdir(), "little-actors-deploy-"))
    try {
        const codePath = path.join(directory, "actors.mjs")
        await buildActor(entrypoint, codePath, { configFile: options.config })
        const codeSnapshot = await publishCode(options.image, options.region, codePath)
        const reply = z
            .object({ changed: z.boolean() })
            .parse(await client.registerDeployment({ ...specification, codeSnapshot, contract }))
        console.log(
            `${reply.changed ? "Registered" : "Already registered"} revision ${specification.codeRevision} with ${contract.actors.length} public actor contract(s).`
        )
    } finally {
        await rm(directory, { recursive: true, force: true })
    }
}

async function publishCode(imageRef: string, canonicalRegion: string, codePath: string): Promise<string> {
    const command =
        process.env.DURABLE_OBJECT_SANDBOX_COMMAND ??
        path.join(path.dirname(await fetchRuntimeExecutablePath()), "little-actors-modal-go")
    const result = await exchange(command, {
        operation: "publish_code",
        request: { imageRef, canonicalRegion, codePath }
    })
    return z.object({ codeSnapshot: z.string().startsWith("im-") }).parse(result).codeSnapshot
}

function exchange(command: string, request: unknown): Promise<unknown> {
    return new Promise((resolve, reject) => {
        const child = spawn(command, [], { stdio: ["pipe", "pipe", "pipe"] })
        const timer = setTimeout(() => {
            child.kill()
            reject(new Error("code publication timed out"))
        }, 120_000)
        let output = ""
        let stderr = ""
        child.stdout.on("data", chunk => {
            output += String(chunk)
            if (Buffer.byteLength(output) > 1024 * 1024) {
                child.kill()
                reject(new Error("provider response exceeds size limit"))
            }
        })
        child.stderr.on("data", chunk => {
            stderr = (stderr + String(chunk)).slice(-8192)
        })
        child.once("error", error => {
            clearTimeout(timer)
            reject(error)
        })
        child.stdin.on("error", reject)
        child.once("close", code => {
            clearTimeout(timer)
            try {
                if (code !== 0) throw new Error(`code publisher exited with ${code}: ${stderr}`)
                const reply = z
                    .discriminatedUnion("status", [
                        z.object({ status: z.literal("success"), result: z.unknown() }),
                        z.object({ status: z.literal("failure"), error: z.string() })
                    ])
                    .parse(JSON.parse(output))
                if (reply.status === "failure") throw new Error(reply.error)
                resolve(reply.result)
            } catch (error) {
                reject(error)
            }
        })
        child.stdin.end(JSON.stringify(request))
    })
}

const component = z.string().regex(/^[A-Za-z0-9._-]+$/u)
const deploymentSchema = z.object({
    codeRevision: component.max(128),
    imageRef: z.string().startsWith("im-").max(255),
    workingDirectory: z.literal("/customer"),
    actorEntrypoint: z.literal("actors.mjs"),
    secretRefs: z.array(component.max(255)).max(16)
})

export { registerDeployCommand }
