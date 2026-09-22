import { Command, InvalidArgumentError, Option } from "commander"
import { z } from "zod"

import { projectIdSchema } from "../actor/identity.js"
import { fetchRuntimeExecutablePath } from "../runtimeInstaller.js"

import { type DevOptions, runDev } from "./dev.js"
import { runtimeEnvironment, startRustRuntime } from "./rust-runtime.js"

interface StartOptions {
    dev?: boolean
    watch: boolean
    port: number
}

function registerStartCommand(program: Command): void {
    program
        .command("start")
        .description("Start the runtime; use --dev for local development")
        .option("--dev", "run locally and reload code changes")
        .optionsGroup("Development options (require --dev):")
        .option("--no-watch", "disable automatic code reload")
        .addOption(
            new Option("--port <number>", "loopback port (0 selects a free port)")
                .env("DURABLE_OBJECT_PORT")
                .argParser(portNumber)
                .default(7100)
        )
        .addHelpText("after", "\nLocal development requires DURABLE_OBJECT_PROJECT_ID in .env or the environment.")
        .action(async (options: StartOptions, command: Command) => {
            process.exitCode = await runStart(options, command)
        })
}

async function runStart(options: StartOptions, command: Command): Promise<number> {
    if (options.dev) return runDev(developmentOptions(options, process.env))
    const devOption = command.options.find(
        option => option.attributeName() !== "dev" && command.getOptionValueSource(option.attributeName()) === "cli"
    )
    if (devOption) throw new Error(`${devOption.long} requires --dev.`)
    const executable = await fetchRuntimeExecutablePath()
    return startRustRuntime(executable, [], runtimeEnvironment(executable), true).exited
}

function developmentOptions(options: StartOptions, environment: NodeJS.ProcessEnv): DevOptions {
    const env = developmentEnvironment.parse(environment)
    return {
        watch: options.watch,
        port: options.port,
        projectId: env.DURABLE_OBJECT_PROJECT_ID,
        apiKey: env.DURABLE_OBJECT_API_KEY,
        project: env.DURABLE_OBJECT_PROJECT,
        entrypoint: env.DURABLE_OBJECT_ENTRYPOINT,
        dataDir: env.DURABLE_OBJECT_DATA_DIR,
        storage: env.DURABLE_OBJECT_STORAGE
    }
}

function portNumber(value: string): number {
    const port = Number(value)
    if (!/^\d+$/u.test(value) || !Number.isSafeInteger(port) || port < 0 || port > 65535)
        throw new InvalidArgumentError("Port must be an integer from 0 to 65535.")
    return port
}

const developmentEnvironment = z.object({
    DURABLE_OBJECT_PROJECT_ID: projectIdSchema,
    DURABLE_OBJECT_API_KEY: z.string().optional(),
    DURABLE_OBJECT_PROJECT: z.string().min(1).default("."),
    DURABLE_OBJECT_ENTRYPOINT: z.string().min(1).default("src/durable-objects.ts"),
    DURABLE_OBJECT_DATA_DIR: z.string().min(1).optional(),
    DURABLE_OBJECT_STORAGE: z.enum(["local", "gcs"]).default("local")
})

export { registerStartCommand }
