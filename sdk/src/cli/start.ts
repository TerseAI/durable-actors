import { Command, InvalidArgumentError, Option } from "commander"

import { fetchRuntimeExecutablePath } from "../runtimeInstaller.js"

import { type DevOptions, runDev } from "./dev.js"
import { runtimeEnvironment, startRustRuntime } from "./rust-runtime.js"

interface StartOptions extends Omit<DevOptions, "projectId"> {
    dev?: boolean
    projectId?: string
}

function registerStartCommand(program: Command): void {
    program
        .command("start")
        .description("Start the runtime; use --dev for local development")
        .option("--dev", "run locally and reload code changes")
        .optionsGroup("Development options (require --dev):")
        .addOption(new Option("--project-id <id>", "actor project ID (required)").env("DURABLE_OBJECT_PROJECT_ID"))
        .option("--no-watch", "disable automatic code reload")
        .addOption(
            new Option("--api-key <key>", "API key for local clients (generated when omitted)").env(
                "DURABLE_OBJECT_API_KEY"
            )
        )
        .addOption(
            new Option("--project <directory>", "actor project directory").env("DURABLE_OBJECT_PROJECT").default(".")
        )
        .addOption(
            new Option("--port <number>", "loopback port (0 selects a free port)")
                .env("DURABLE_OBJECT_PORT")
                .argParser(portNumber)
                .default(7100)
        )
        .addOption(
            new Option("--entrypoint <file>", "actor source file, relative to the project")
                .env("DURABLE_OBJECT_ENTRYPOINT")
                .default("src/durable-objects.ts")
        )
        .addOption(
            new Option("--data-dir <directory>", "state directory (default: <project>/.little-actors)").env(
                "DURABLE_OBJECT_DATA_DIR"
            )
        )
        .addOption(
            new Option("--storage <backend>", "where to save actor state")
                .env("DURABLE_OBJECT_STORAGE")
                .choices(["local", "gcs"])
                .default("local")
        )
        .action(async (options: StartOptions, command: Command) => {
            process.exitCode = await runStart(options, command)
        })
}

async function runStart(options: StartOptions, command: Command): Promise<number> {
    if (options.dev) {
        if (!options.projectId)
            throw new Error("--project-id is required with --dev; set it explicitly or set DURABLE_OBJECT_PROJECT_ID.")
        return runDev({ ...options, projectId: options.projectId })
    }
    const devOption = command.options.find(
        option => option.attributeName() !== "dev" && command.getOptionValueSource(option.attributeName()) === "cli"
    )
    if (devOption) throw new Error(`${devOption.long} requires --dev.`)
    const executable = await fetchRuntimeExecutablePath()
    return startRustRuntime(executable, [], runtimeEnvironment(executable), true).exited
}

function portNumber(value: string): number {
    const port = Number(value)
    if (!/^\d+$/u.test(value) || !Number.isSafeInteger(port) || port < 0 || port > 65535)
        throw new InvalidArgumentError("Port must be an integer from 0 to 65535.")
    return port
}

export { registerStartCommand }
