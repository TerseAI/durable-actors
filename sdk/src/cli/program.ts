import { Command, Option } from "commander"
import { cp, mkdir, readFile, rename, rm } from "node:fs/promises"
import path from "node:path"
import { styleText } from "node:util"

import { registerDevCommand } from "./dev.js"
import { registerGenerateCommand } from "./generate.js"
import { registerObserveCommand } from "./observe.js"
import { registerStartCommand } from "./start.js"

export async function runCli(args: string[]): Promise<void> {
    const program = new Command()
        .name("durable-actors")
        .description("Run durable TypeScript actors locally or in the cloud")
        .version(await version())
        .enablePositionalOptions()
        .addHelpCommand(false)
        .showHelpAfterError()
    program
        .command("init <directory>")
        .description("Create a sample actor project")
        .addOption(
            new Option("--template <name>", "project template")
                .choices(["actor", "chat", "ai-chat", "documents"])
                .default("actor")
        )
        .action(initializeProject)
    registerDevCommand(program)
    registerGenerateCommand(program)
    registerObserveCommand(program)
    registerStartCommand(program)
    if (args.length === 2) program.help()
    await program.parseAsync(args)
}

async function initializeProject(directory: string, options: { template: string }): Promise<void> {
    const destination = path.resolve(directory)
    await mkdir(destination).catch((error: NodeJS.ErrnoException) => {
        if (error.code === "EEXIST") throw new Error(`${destination} already exists. Choose a new directory.`)
        throw error
    })
    try {
        await cp(new URL(`../templates/${options.template}/`, import.meta.url), destination, {
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
  npm install
  cp .env.example .env${template === "ai-chat" ? "\n  # Add your OpenAI API key to .env" : ""}
  npm run dev:actors

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
    Edit ${styleText("cyan", "src/actors.ts")} to make it your own.

  ${styleText("bold", "Connect your app")}
    Follow dev's .env and generate instructions
    in your separate application project.
`
}

async function version(): Promise<string> {
    return JSON.parse(await readFile(new URL("../../package.json", import.meta.url), "utf8")).version
}
