#!/usr/bin/env node
import { Command, Option } from "commander"
import { config } from "dotenv"
import { cp, mkdir, readFile, rename, rm } from "node:fs/promises"
import path from "node:path"
import { styleText } from "node:util"

import { registerDeployCommand } from "./cli/deploy.js"
import { registerDevCommand } from "./cli/dev.js"
import { registerGenerateCommand } from "./cli/generate.js"
import { registerObjectCommands } from "./cli/objects.js"
import { registerObserveCommand } from "./cli/observe.js"
import { runtimeEnvironment, startRustRuntime } from "./cli/rust-runtime.js"
import { actorEnvironment } from "./environment.js"
import { fetchRuntimeExecutablePath } from "./runtimeInstaller.js"

try {
    config({ quiet: true })
    Object.assign(process.env, actorEnvironment(process.env))
    const program = new Command()
        .name("durable-actors")
        .description("Run durable TypeScript actors locally or in the cloud")
        .version(await version())
        .enablePositionalOptions()
        .addHelpCommand(false)
        .showHelpAfterError()
    program
        .command("init <directory>")
        .description("Create a standalone actor project")
        .addOption(
            new Option("--template <name>", "project template")
                .choices(["actor", "chat", "ai-chat", "documents"])
                .default("actor")
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
    registerObserveCommand(program)
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
            force: false
        })
        await rename(path.join(destination, "gitignore"), path.join(destination, ".gitignore"))
    } catch (error) {
        await rm(destination, { recursive: true, force: true })
        throw error
    }
    console.log(projectInstructions(destination, options.template))
}

function projectInstructions(destination: string, template: string): string {
    if (template === "actor") return actorProjectInstructions(destination)

    return `Created ${template} app in ${destination}.

From that directory, run:
  npm install${template === "ai-chat" ? "\n  cp .env.example .env\n  # Add your OpenAI API key to .env" : "\n  npx durable-actors generate"}
  npx durable-actors dev

Add the connection settings printed by durable-actors dev to .env, then in another terminal:
  npm run dev

Open http://127.0.0.1:3000. The README walks through the app.`
}

function actorProjectInstructions(destination: string): string {
    const directory = path.relative(process.cwd(), destination) || "."
    const quoted = `'${directory.replaceAll("'", "'\\''")}'`
    return `
${styleText(["bold", "cyan"], "durable actors")} ${styleText("dim", "/ new project")}

  ${styleText("green", "✓")} ${styleText("bold", path.basename(destination))} is ready.
    ${styleText("dim", destination)}

  ${styleText("bold", "Start here")}
    ${styleText("cyan", `cd -- ${quoted}`)}
    ${styleText("cyan", "pnpm install")}

  ${styleText("bold", "Start the actor server")}
    ${styleText("cyan", "durable-actors dev")}

  ${styleText("bold", "Make it yours")}
    Your first actor is a counter that remembers.
    Edit ${styleText("cyan", "src/durable-objects.ts")} to make it your own.

  ${styleText("bold", "Connect your app")}
    Follow dev's .env and generate instructions
    in your separate application project.
`
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
