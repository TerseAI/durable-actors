#!/usr/bin/env node
import { config } from "dotenv"

import { actorEnvironment } from "./environment.js"
import { projectSdkModule } from "./project-sdk.js"

try {
    config({ quiet: true })
    Object.assign(process.env, actorEnvironment(process.env))
    const development = process.argv[2] === "dev" && !process.argv.includes("--help") && !process.argv.includes("-h")
    const entrypoint = development
        ? await projectSdkModule(process.env.DURABLE_ACTORS_PROJECT ?? ".", "cli")
        : new URL("./cli/program.js", import.meta.url).href
    const { runCli } = await import(entrypoint)
    await runCli(process.argv)
} catch (error) {
    console.error(error instanceof Error ? error.message : String(error))
    process.exitCode = 1
}
