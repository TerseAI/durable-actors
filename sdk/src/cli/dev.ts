import { Command, InvalidArgumentError, Option } from "commander"
import { realpath, stat } from "node:fs/promises"
import path from "node:path"
import { fileURLToPath } from "node:url"
import { z } from "zod"

import { projectIdSchema } from "../actor/identity.js"
import { projectSdkModule } from "../projectSdk.js"
import { fetchRuntimeExecutablePath } from "../runtimeInstaller.js"

import { pythonExecutable } from "./python.js"
import { runtimeConnection, runtimeEnvironment, startRustRuntime } from "./rust-runtime.js"

interface DevOptions {
    /** Module URL from which to resolve the SDK dependency when embedded in a wrapper. */
    sdkResolveFrom?: string
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
            new Option("--port <number>", "localhost port (0 selects a free port)")
                .env("DURABLE_ACTORS_PORT")
                .argParser(portNumber)
                .default(7100)
        )
        .addHelpText(
            "after",
            `
Run from your actor project directory; dev finds src/actors.ts, src/actors.py, actors.ts, or actors.py.
No configuration is required. Optional overrides in .env:
  DURABLE_ACTORS_PROJECT     project directory (default: current directory)
  DURABLE_ACTORS_ENTRYPOINT  actor source file, relative to the project (default: discover actors.ts or actors.py in src/ or the project root)

Python projects: uvx --from 'durable-actors[cli]' durable-actors init my-project
DURABLE_ACTORS_PYTHON selects an interpreter; otherwise dev uses the project .venv.

Create a project with: durable-actors init my-project`
        )
        .action(async (options: { watch: boolean; port: number }) => {
            process.exitCode = await runDev(await developmentOptions(options, process.env))
        })
}

async function developmentOptions(
    options: { watch: boolean; port: number },
    environment: NodeJS.ProcessEnv
): Promise<DevOptions> {
    const env = developmentEnvironment.parse(environment)
    return {
        ...options,
        projectId: env.DURABLE_ACTORS_PROJECT_ID,
        apiKey: env.DURABLE_ACTORS_SECRET,
        project: env.DURABLE_ACTORS_PROJECT,
        entrypoint: env.DURABLE_ACTORS_ENTRYPOINT ?? (await defaultEntrypoint(env.DURABLE_ACTORS_PROJECT)),
        dataDir: env.DURABLE_ACTORS_DATA_DIR,
        storage: env.DURABLE_ACTORS_STORAGE
    }
}

async function defaultEntrypoint(project: string): Promise<string> {
    const found: string[] = []
    for (const entrypoint of DEFAULT_ENTRYPOINTS)
        if ((await developmentPathStats(path.resolve(project, entrypoint)))?.isFile()) found.push(entrypoint)
    if (found.length > 1)
        throw new Error(
            `Multiple actor sources found in ${path.resolve(project)}: ${found.join(", ")}. Set DURABLE_ACTORS_ENTRYPOINT to choose one.`
        )
    return found[0] ?? DEFAULT_ENTRYPOINTS[0]
}

const DEFAULT_ENTRYPOINTS = ["src/actors.ts", "src/actors.py", "actors.ts", "actors.py"] as const

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
    DURABLE_ACTORS_ENTRYPOINT: z.string().min(1).optional(),
    DURABLE_ACTORS_DATA_DIR: z.string().min(1).optional(),
    DURABLE_ACTORS_STORAGE: z.enum(["local", "gcs"]).default("local")
})

async function runDev(options: DevOptions): Promise<number> {
    const project = await developmentProject(options)
    if (options.entrypoint.endsWith(".py")) return runDevRuntime(options, project)
    const local = await projectSdkModule(project, "./cli/dev.js", import.meta.url, options.sdkResolveFrom)
    if (local !== undefined) return (await import(local)).runDev({ ...options, project })
    return runDevRuntime(options, project)
}

async function developmentProject(options: DevOptions): Promise<string> {
    const directory = path.resolve(options.project)
    if (!(await developmentPathStats(directory))?.isDirectory())
        throw new Error(
            `No actor project directory found at ${directory}.\n` +
                "Run from your actor project directory, or set DURABLE_ACTORS_PROJECT in .env.\n" +
                "Create a project with: durable-actors init my-project"
        )
    const project = await realpath(directory)
    const entrypoint = path.resolve(project, options.entrypoint)
    if (!(await developmentPathStats(entrypoint))?.isFile())
        throw new Error(
            `No actor source file found at ${entrypoint}.\n` +
                "Run from your actor project directory, or set DURABLE_ACTORS_PROJECT and DURABLE_ACTORS_ENTRYPOINT in .env.\n" +
                "Create a project with: durable-actors init my-project"
        )
    return project
}

async function developmentPathStats(candidate: string) {
    return stat(candidate).catch((error: NodeJS.ErrnoException) => {
        if (error.code === "ENOENT" || error.code === "ENOTDIR") return undefined
        throw error
    })
}

async function runDevRuntime(options: DevOptions, project: string): Promise<number> {
    const python = options.entrypoint.endsWith(".py") ? await pythonExecutable(project) : undefined
    const executable = await fetchRuntimeExecutablePath()
    const runtime = startRustRuntime(
        executable,
        [...devArguments(options), "--ready-fd", "3"],
        { ...runtimeEnvironment(executable), ...(python ? { DURABLE_ACTORS_PYTHON: python } : {}) },
        true,
        true
    )
    try {
        await runtimeConnection(runtime.readiness!, runtime.exited)
        return await runtime.exited
    } catch (error) {
        runtime.child.kill("SIGTERM")
        await runtime.exited.catch(() => {})
        throw error
    }
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
    if (options.watch) args.push("--watch")
    if (options.apiKey) args.push("--api-key", options.apiKey)
    if (options.dataDir) args.push("--data-dir", options.dataDir)
    return args
}

export { type DevOptions, runDev }
