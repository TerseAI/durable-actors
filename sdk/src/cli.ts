#!/usr/bin/env node
import { Command, InvalidArgumentError, Option } from "commander"
import { spawn } from "node:child_process"
import { randomUUID } from "node:crypto"
import { cp, mkdir, mkdtemp, readFile, rename, rm, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"

import { ControlPlaneClient } from "./cli/control-plane.js"
import { registerDeployCommand } from "./cli/deploy.js"
import { registerGenerateCommand } from "./cli/generate.js"
import { registerObjectCommands } from "./cli/objects.js"
import { configuredSettings } from "./client/clientSettings.js"
import { runtimeExecutable } from "./runtimeInstaller.js"

interface DevOptions {
    port: number
    project: string
    entrypoint: string
    storage: "local" | "gcs"
    dataDir?: string
}

try {
    const program = new Command()
        .name("little-actors")
        .description("Run durable TypeScript actors locally or in the cloud")
        .version(await version())
        .enablePositionalOptions()
        .addHelpCommand(false)
        .showHelpAfterError()
    program
        .command("init <directory>")
        .description("Create an Express and React app from a bundled template")
        .addOption(
            new Option("--template <name>", "example app").choices(["chat", "ai-chat", "documents"]).default("chat")
        )
        .action(initializeProject)
    registerGenerateCommand(program)
    registerDeployCommand(program)
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
    program
        .command("token")
        .description("Print a one-hour local session token for tools such as wscat")
        .option("--data-dir <directory>", "runtime state directory", ".little-actors")
        .action(async options => {
            const { token } = await localSession(options.dataDir)
            console.log(token)
        })
    program
        .command("start")
        .description("Start the packaged runtime using your self-hosting environment settings")
        .action(async () => {
            process.exitCode = await runRuntime([])
        })
    registerObjectCommands(program)
    if (process.argv.length === 2) program.help()
    await program.parseAsync(process.argv)
} catch (error) {
    console.error(error instanceof Error ? error.message : String(error))
    process.exitCode = 1
}

async function initializeProject(directory: string, options: { template: string }): Promise<void> {
    const destination = path.resolve(directory)
    await mkdir(destination).catch((error: NodeJS.ErrnoException) => {
        if (error.code === "EEXIST") throw new Error(`${destination} already exists. Choose a new directory.`)
        throw error
    })
    try {
        await cp(new URL(`./templates/${options.template}/`, import.meta.url), destination, {
            recursive: true,
            force: false,
            errorOnExist: true
        })
        await rename(path.join(destination, "gitignore"), path.join(destination, ".gitignore"))
    } catch (error) {
        await rm(destination, { recursive: true, force: true })
        throw error
    }
    console.log(`Created ${options.template} app in ${destination}.

From that directory, run:
  npm install${options.template === "ai-chat" ? "\n  cp .env.example .env\n  # Add your OpenAI API key to .env" : "\n  npx little-actors generate"}
  npx little-actors dev

In another terminal, from the same directory:
  npm run dev

Open http://127.0.0.1:3000. The README walks through the app.`)
}

function portNumber(value: string): number {
    const port = Number(value)
    if (!/^\d+$/u.test(value) || !Number.isSafeInteger(port) || port < 0 || port > 65535)
        throw new InvalidArgumentError("Port must be an integer from 0 to 65535.")
    return port
}

async function runDev(options: DevOptions): Promise<number> {
    const { ActorCompiler } = await import("./compiler/actor-compiler.js")
    const { parsePublicContract } = await import("./compiler/validate-public-contract.js")
    const contract = parsePublicContract(
        new ActorCompiler().compileContract(path.resolve(options.project, options.entrypoint))
    )
    const directory = await mkdtemp(path.join(tmpdir(), "little-actors-contract-"))
    try {
        const file = path.join(directory, "contract.json")
        await writeFile(file, JSON.stringify(contract))
        return await runRuntime([...devArguments(options), "--contract", file])
    } finally {
        await rm(directory, { recursive: true, force: true })
    }
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

async function runRuntime(args: string[]): Promise<number> {
    const executable = await runtimeExecutable()
    return runProcess(
        executable,
        args,
        {
            ...process.env,
            PATH: `${path.dirname(executable)}${path.delimiter}${process.env.PATH ?? ""}`,
            DURABLE_OBJECT_PROCESS_ROLE: process.env.DURABLE_OBJECT_PROCESS_ROLE ?? "control_plane",
            DURABLE_OBJECT_PARENT_LIFETIME_STDIN: "1"
        },
        true
    )
}

async function localSession(directory: string) {
    const connection = await localConnection(directory)
    const client = new ControlPlaneClient(configuredSettings(connection), fetch)
    const { token } = (await client.issueSessionToken({
        executionId: `local-${randomUUID()}`,
        deadlineUnixMs: Date.now() + 3_600_000,
        storageRegion: connection.storageRegion
    })) as { token: string }
    return { connection, token }
}

async function localConnection(directory: string) {
    return readFile(path.resolve(directory, "runtime.json"), "utf8")
        .then(JSON.parse)
        .catch(() => {
            throw new Error(
                "No local runtime found. Start `npx little-actors dev` in this project first; use the same --data-dir for both commands."
            )
        })
}

async function version(): Promise<string> {
    return JSON.parse(await readFile(new URL("../package.json", import.meta.url), "utf8")).version
}

function runProcess(command: string, args: string[], env: NodeJS.ProcessEnv, parentLifetime = false): Promise<number> {
    return new Promise((resolve, reject) => {
        const child = spawn(command, args, { env, stdio: [parentLifetime ? "pipe" : "inherit", "inherit", "inherit"] })
        const interrupt = () => child.kill("SIGINT")
        const terminate = () => child.kill("SIGTERM")
        const parentClosed = () => child.stdin?.end()
        process.on("SIGINT", interrupt)
        process.on("SIGTERM", terminate)
        if (parentLifetime && process.env.DURABLE_OBJECT_PARENT_LIFETIME_STDIN) {
            process.stdin.resume()
            process.stdin.on("end", parentClosed)
        }
        const cleanup = () => {
            process.off("SIGINT", interrupt)
            process.off("SIGTERM", terminate)
            process.stdin.off("end", parentClosed)
            if (parentLifetime) process.stdin.pause()
        }
        child.once("error", error => {
            cleanup()
            reject(error)
        })
        child.once("exit", (code, signal) => {
            cleanup()
            resolve(code ?? (signal === "SIGINT" ? 130 : 1))
        })
    })
}
