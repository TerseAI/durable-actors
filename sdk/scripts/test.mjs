import { spawnSync } from "node:child_process"
import { readdir } from "node:fs/promises"

const files = (await readdir(".test-dist", { recursive: true }))
    .filter(file => file.endsWith(".test.js"))
    .map(file => `./.test-dist/${file}`)
const host = files.filter(file => file.includes("/host/"))
const tooling = files.filter(file => !file.includes("/host/"))
for (const [command, args] of [
    [process.execPath, ["--test", "--test-concurrency=1", ...tooling]],
    ["bun", ["test", "--timeout", "30000", ...host]]
]) {
    const result = spawnSync(command, args, { stdio: "inherit" })
    if (result.error) throw result.error
    if (result.status !== 0) process.exit(result.status ?? 1)
}
