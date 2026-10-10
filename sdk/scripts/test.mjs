import { spawnSync } from "node:child_process"
import { readdir } from "node:fs/promises"

const files = (await readdir(".test-dist", { recursive: true }))
    .filter(file => file.endsWith(".test.js"))
    .map(file => `./.test-dist/${file}`)
const result = spawnSync(process.execPath, ["test", "--parallel=1", "--timeout", "60000", ...files], {
    stdio: "inherit"
})
if (result.error) throw result.error
process.exit(result.status ?? 1)
