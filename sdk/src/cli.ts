#!/usr/bin/env node
import { Command, Option } from "commander"
import { cp, mkdir, readFile, rename, rm } from "node:fs/promises"
import path from "node:path"

import { registerDeployCommand } from "./cli/deploy.js"
import { registerDevCommand } from "./cli/dev.js"
import { registerGenerateCommand } from "./cli/generate.js"
import { registerObjectCommands } from "./cli/objects.js"
import { runtimeEnvironment, startRustRuntime } from "./cli/rust-runtime.js"
import { fetchRuntimeExecutablePath } from "./runtimeInstaller.js"

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
    program
        .command("build [entrypoint]")
        .description("Build an actor runtime artifact with embedded persistence and socket schemas")
        .option("--out-file <file>", "actor runtime artifact", "dist/actors.mjs")
        .option("--config <file>", "TypeScript configuration file")
        .action(async (entrypoint: string | undefined, options: { outFile: string; config?: string }) => {
            const { buildActor } = await import("./compiler/actor-build.js")
            await buildActor(entrypoint ?? "src/durable-objects.ts", path.resolve(options.outFile), {
                configFile: options.config
            })
        })
    registerGenerateCommand(program)
    registerDeployCommand(program)
    registerDevCommand(program)
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
  export DURABLE_OBJECT_API_KEY=local-dev-key
  npx little-actors dev

In another terminal, from the same directory:
  export DURABLE_OBJECT_API_KEY=local-dev-key
  npm run dev

Open http://127.0.0.1:3000. The README walks through the app.`)
}

async function runRuntime(args: string[]): Promise<number> {
    const executable = await fetchRuntimeExecutablePath()
    return runProcess(executable, args, runtimeEnvironment(executable), true)
}

async function version(): Promise<string> {
    return JSON.parse(await readFile(new URL("../package.json", import.meta.url), "utf8")).version
}

function runProcess(command: string, args: string[], env: NodeJS.ProcessEnv, parentLifetime = false): Promise<number> {
    return startRustRuntime(command, args, env, parentLifetime).exited
}
