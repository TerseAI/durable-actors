import { Command } from "commander"
import path from "node:path"
import { z } from "zod"

import { ControlPlaneClient, type ControlPlaneOptions } from "./control-plane.js"

interface DeployOptions extends ControlPlaneOptions {
    image: string
    revision: string
    workingDirectory: string
    config?: string
    actorEntrypoint?: string
    secret: string[]
    socketGatewayUrl?: string
    warmRegion?: string
}

function registerDeployCommand(program: Command): void {
    program
        .command("deploy [entrypoint]")
        .description("Register a built actor image and automatically publish its public API")
        .requiredOption("--image <reference>", "already-built provider image reference")
        .requiredOption("--revision <revision>", "code revision corresponding to the image and local source")
        .requiredOption("--working-directory <path>", "absolute actor project directory inside the image")
        .option(
            "--actor-entrypoint <path>",
            "actor file inside the image (defaults to the local source's relative path)"
        )
        .option("--config <file>", "local TypeScript configuration file")
        .option("--url <origin>", "control-plane origin (or DURABLE_OBJECT_CONTROL_PLANE_URL)")
        .option("--api-key <key>", "admin API key (or DURABLE_OBJECT_API_KEY)")
        .option("--namespace <id>", "deployment namespace (or DURABLE_OBJECT_NAMESPACE_ID)")
        .option(
            "--secret <name>",
            "provider secret reference (repeatable)",
            (value: string, previous: string[]) => [...previous, value],
            []
        )
        .option("--socket-gateway-url <origin>", "separate socket delivery origin")
        .option("--warm-region <region>", "request background image warmup in this region")
        .action(deploy)
}

async function deploy(entrypoint = "src/durable-objects.ts", options: DeployOptions): Promise<void> {
    const client = new ControlPlaneClient(options, fetch)
    const specification = deploymentSpecification(entrypoint, options)
    const { ActorCompiler } = await import("../compiler/actor-compiler.js")
    const { parsePublicContract } = await import("../compiler/validate-public-contract.js")
    const contract = parsePublicContract(
        new ActorCompiler().compileContract(entrypoint, { configFile: options.config })
    )
    const reply = z
        .object({ changed: z.boolean() })
        .parse(await client.json("PUT", "deployment", { ...specification, contract }))
    console.log(
        `${reply.changed ? "Registered" : "Already registered"} revision ${specification.codeRevision} with ${contract.actors.length} public actor contract(s).`
    )
}

function deploymentSpecification(entrypoint: string, options: DeployOptions) {
    const relative = path.relative(process.cwd(), path.resolve(entrypoint))
    if (!options.actorEntrypoint && (relative === ".." || relative.startsWith(`..${path.sep}`)))
        throw new Error("Source outside the project requires --actor-entrypoint to specify its path inside the image.")
    return deploymentSchema.parse({
        codeRevision: options.revision,
        imageRef: options.image,
        workingDirectory: options.workingDirectory,
        actorEntrypoint: options.actorEntrypoint ?? relative.split(path.sep).join("/"),
        secretRefs: options.secret,
        socketGatewayUrl: options.socketGatewayUrl,
        warmRegion: options.warmRegion
    })
}

const component = z.string().regex(/^[A-Za-z0-9._-]+$/u)
const deploymentSchema = z.object({
    codeRevision: component.max(128),
    imageRef: z.string().min(1).max(255),
    workingDirectory: z.string().startsWith("/").max(1024),
    actorEntrypoint: z.string().min(1).max(1024),
    secretRefs: z.array(component.max(255)).max(16),
    socketGatewayUrl: z.string().url().optional(),
    warmRegion: component.optional()
})

export { registerDeployCommand }
