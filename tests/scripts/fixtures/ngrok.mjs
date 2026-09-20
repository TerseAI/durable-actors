#!/usr/bin/env node
import { mkdirSync } from "node:fs"

console.log(JSON.stringify({ pid: process.pid, args: process.argv.slice(2), tokenPresent: Boolean(process.env.NGROK_AUTHTOKEN) }))
if (process.env.FIXTURE_EXIT) {
    console.error("fixture authentication failure")
    process.exit(Number(process.env.FIXTURE_EXIT))
}
for (const signal of ["SIGINT", "SIGTERM"]) {
    process.on(signal, () => {
        console.log(`ngrok stopped: ${signal}`)
        process.exit(0)
    })
}
process.on("SIGUSR1", () => process.exit(7))
if (process.env.FIXTURE_ENV_DIRECTORY) mkdirSync(".env")
console.log(JSON.stringify({ lvl: "info", msg: "started tunnel", url: "https://fixture.ngrok.app" }))
setInterval(() => {}, 1000)
