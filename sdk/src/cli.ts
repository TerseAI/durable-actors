#!/usr/bin/env node
import { Command, Option } from "commander"
import { config } from "dotenv"
import { cp, mkdir, readFile, rename, rm } from "node:fs/promises"
import path from "node:path"

import { registerDeployCommand } from "./cli/deploy.js"
import { registerGenerateCommand } from "./cli/generate.js"
import { registerObserveCommand } from "./cli/observe.js"
import { registerStartCommand } from "./cli/start.js"

try {
    config({ quiet: true })
    const program = new Command()
        .name("little-actors")
        .description("Run durable actors")
        .version(await version())
        .enablePositionalOptions()
        .addHelpCommand(false)
        .showHelpAfterError()
    program
        .command("init <directory>")
        .description("Start from a sample project")
        .addOption(
            new Option("--template <name>", "example app").choices(["chat", "ai-chat", "documents"]).default("chat")
        )
        .action(initializeProject)
    registerStartCommand(program)
    registerGenerateCommand(program)
    registerDeployCommand(program)
    registerObserveCommand(program)
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
    console.log(`Created ${options.template} app in ${destination}.

From that directory, run:
  npm install
  cp .env.example .env${options.template === "ai-chat" ? "\n  # Add your OpenAI API key to .env" : ""}
  npm run dev:actors

In another terminal in the same directory, run:
  npm run dev

Open http://127.0.0.1:3000. The README walks through the app.`)
}

async function version(): Promise<string> {
    return JSON.parse(await readFile(new URL("../package.json", import.meta.url), "utf8")).version
}
