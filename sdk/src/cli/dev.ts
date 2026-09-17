import { Command, InvalidArgumentError, Option } from "commander"
import { randomUUID } from "node:crypto"
import { mkdtemp, realpath, rm, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"

import { configuredSettings } from "../client/clientSettings.js"
import { fetchRuntimeExecutablePath } from "../runtimeInstaller.js"

import { type ActorSourceWatcher, watchActorSources } from "./actor-source-watcher.js"
import { ControlPlaneClient } from "./control-plane.js"
import { runtimeConnection, runtimeEnvironment, startRustRuntime } from "./rust-runtime.js"

interface DevOptions {
    port: number
    project: string
    entrypoint: string
    storage: "local" | "gcs"
    dataDir?: string
}

function registerDevCommand(program: Command): void {
    program
        .command("dev")
        .description("Start local actors with automatic SQLite and file storage")
        .option("--project <directory>", "actor project directory", ".")
        .option("--port <number>", "loopback port (0 selects a free port)", portNumber, 7100)
        .option("--entrypoint <file>", "actor source file, relative to the project", "src/durable-objects.ts")
        .option("--data-dir <directory>", "state directory (default: <project>/.little-actors)")
        .addOption(
            new Option("--storage <backend>", "where to save actor snapshots")
                .choices(["local", "gcs"])
                .default("local")
        )
        .action(async options => {
            process.exitCode = await runDev(options)
        })
}

function portNumber(value: string): number {
    const port = Number(value)
    if (!/^\d+$/u.test(value) || !Number.isSafeInteger(port) || port < 0 || port > 65535)
        throw new InvalidArgumentError("Port must be an integer from 0 to 65535.")
    return port
}

async function runDev(options: DevOptions): Promise<number> {
    const project = await realpath(options.project)
    const contract = await compileContract(project, options.entrypoint)
    const directory = await mkdtemp(path.join(tmpdir(), "little-actors-contract-"))
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
    const client = runtimeConnection(runtime.readiness!, runtime.exited).then(
        connection => new ControlPlaneClient(configuredSettings(connection), fetch)
    )
    void client.catch(() => {})
    let watcher: ActorSourceWatcher | undefined
    try {
        watcher = await watchActorSources({ projectDirectory: project, dataDirectory: options.dataDir }, async () =>
            publishLocalContract(options, project, await client)
        )
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
    const codeRevision = randomUUID()
    await client.registerDeployment({
        codeRevision,
        imageRef: "local",
        workingDirectory: project,
        actorEntrypoint: options.entrypoint,
        secretRefs: [],
        contract
    })
    console.log(`Updated local actor revision ${codeRevision}.`)
}

async function compileContract(project: string, entrypoint: string) {
    const { ActorCompiler } = await import("../compiler/actor-compiler.js")
    const { parsePublicContract } = await import("../compiler/validate-public-contract.js")
    return parsePublicContract(new ActorCompiler().compileContract(path.resolve(project, entrypoint)))
}

function devArguments(options: DevOptions): string[] {
    const args = [
        "dev",
        "--project",
        options.project,
        "--port",
        String(options.port),
        "--entrypoint",
        options.entrypoint,
        "--storage",
        options.storage
    ]
    if (options.dataDir) args.push("--data-dir", options.dataDir)
    return args
}

export { registerDevCommand }
