import { Command, InvalidArgumentError, Option } from "commander"
import { mkdtemp, realpath, rm, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import { fileURLToPath } from "node:url"
import { z } from "zod"

import { projectIdSchema } from "../actor/identity.js"
import { configuredSettings } from "../client/clientSettings.js"
import { fetchRuntimeExecutablePath } from "../runtimeInstaller.js"

import { type ActorSourceWatcher, watchActorSources } from "./actor-source-watcher.js"
import { ControlPlaneClient } from "./control-plane.js"
import { runtimeConnection, runtimeEnvironment, startRustRuntime } from "./rust-runtime.js"

interface DevOptions {
    projectId: string
    apiKey?: string
    port: number
    project: string
    entrypoint: string
    storage: "local" | "gcs"
    dataDir?: string
    watch: boolean
}

export function registerDevCommand(program: Command): void {
    program
        .command("dev")
        .description("Run local actors and reload code changes")
        .option("--no-watch", "disable automatic code reload")
        .addOption(
            new Option("--port <number>", "loopback port (0 selects a free port)")
                .env("DURABLE_ACTORS_PORT")
                .argParser(portNumber)
                .default(7100)
        )
        .addHelpText("after", "\nConfigure development in .env; DURABLE_ACTORS_PROJECT_ID defaults to local.")
        .action(async (options: { watch: boolean; port: number }) => {
            process.exitCode = await runDev(developmentOptions(options, process.env))
        })
}

function developmentOptions(options: { watch: boolean; port: number }, environment: NodeJS.ProcessEnv): DevOptions {
    const env = developmentEnvironment.parse(environment)
    return {
        ...options,
        projectId: env.DURABLE_ACTORS_PROJECT_ID,
        apiKey: env.DURABLE_ACTORS_SECRET,
        project: env.DURABLE_ACTORS_PROJECT,
        entrypoint: env.DURABLE_ACTORS_ENTRYPOINT,
        dataDir: env.DURABLE_ACTORS_DATA_DIR,
        storage: env.DURABLE_ACTORS_STORAGE
    }
}

function portNumber(value: string): number {
    const port = Number(value)
    if (!/^\d+$/u.test(value) || !Number.isSafeInteger(port) || port < 0 || port > 65535)
        throw new InvalidArgumentError("Port must be an integer from 0 to 65535.")
    return port
}

const developmentEnvironment = z.object({
    DURABLE_ACTORS_PROJECT_ID: projectIdSchema.default("local"),
    DURABLE_ACTORS_SECRET: z.string().optional(),
    DURABLE_ACTORS_PROJECT: z.string().min(1).default("."),
    DURABLE_ACTORS_ENTRYPOINT: z.string().min(1).default("src/durable-objects.ts"),
    DURABLE_ACTORS_DATA_DIR: z.string().min(1).optional(),
    DURABLE_ACTORS_STORAGE: z.enum(["local", "gcs"]).default("local")
})

async function runDev(options: DevOptions): Promise<number> {
    const project = await realpath(options.project)
    const contract = await compileContract(project, options.entrypoint)
    const directory = await mkdtemp(path.join(tmpdir(), "durable-actors-contract-"))
    try {
        const file = path.join(directory, "contract.json")
        await writeFile(file, JSON.stringify(contract))
        return await runDevRuntime(options, project, file)
    } finally {
        await rm(directory, { recursive: true, force: true })
    }
}

async function runDevRuntime(options: DevOptions, project: string, contractFile: string): Promise<number> {
    const executable = await fetchRuntimeExecutablePath()
    const runtime = startRustRuntime(
        executable,
        [...devArguments(options), "--contract", contractFile, "--ready-fd", "3"],
        runtimeEnvironment(executable),
        true,
        true
    )
    const connection = runtimeConnection(runtime.readiness!, runtime.exited)
    const settings = connection.then(value =>
        configuredSettings(
            z.object({ projectId: z.string(), controlPlaneUrl: z.string(), apiKey: z.string() }).parse(value)
        )
    )
    const client = settings.then(settings => new ControlPlaneClient(settings, fetch))
    void client.catch(() => {})
    let watcher: ActorSourceWatcher | undefined
    try {
        if (options.watch) {
            watcher = await watchActorSources({ projectDirectory: project, dataDirectory: options.dataDir }, async () =>
                publishLocalContract(options, project, await client)
            )
        }
        await client
        return await runtime.exited
    } catch (error) {
        runtime.child.kill("SIGTERM")
        await runtime.exited.catch(() => {})
        throw error
    } finally {
        await watcher?.close()
    }
}

async function publishLocalContract(
    options: DevOptions,
    project: string,
    client: Pick<ControlPlaneClient, "registerDeployment">
): Promise<void> {
    const contract = await compileContract(project, options.entrypoint)
    await client.registerDeployment({
        imageRef: "local",
        workingDirectory: project,
        actorEntrypoint: options.entrypoint,
        secretRefs: [],
        contract
    })
    console.log("Updated local actors.")
}

async function compileContract(project: string, entrypoint: string) {
    const { ActorCompiler } = await import("../compiler/actor-compiler.js")
    const { parsePublicContract } = await import("../compiler/validate-public-contract.js")
    return parsePublicContract(new ActorCompiler().compileContract(path.resolve(project, entrypoint)))
}

function devArguments(options: DevOptions): string[] {
    const args = [
        "dev",
        "--project-id",
        options.projectId,
        "--project",
        options.project,
        "--port",
        String(options.port),
        "--entrypoint",
        options.entrypoint,
        "--storage",
        options.storage,
        "--sdk-host",
        fileURLToPath(new URL("../host.js", import.meta.url))
    ]
    if (options.apiKey) args.push("--api-key", options.apiKey)
    if (options.dataDir) args.push("--data-dir", options.dataDir)
    return args
}

export { type DevOptions, runDev }
