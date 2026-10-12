import { actors } from "../generated/index.js"

import { PiCli } from "./cli.js"

try {
    const cli = new PiCli(actors.PiAgent.get("demo"))
    console.log(await cli.prompt(process.argv.slice(2)))
} catch (error) {
    console.error(error instanceof Error ? error.message : String(error))
    process.exitCode = 1
}
