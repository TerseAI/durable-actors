import assert from "node:assert/strict"
import { execFile } from "node:child_process"
import { test } from "node:test"
import { fileURLToPath } from "node:url"
import { promisify } from "node:util"

const run = promisify(execFile)
const cli = fileURLToPath(new URL("../../../dist/cli.js", import.meta.url))

test("CLI help explains source defaults without requiring credentials", async () => {
    const { stdout } = await run(process.execPath, [cli, "generate", "--help"], {
        env: { ...process.env, DURABLE_ACTORS_PROJECT_ID: "", DURABLE_ACTORS_API_KEY: "" }
    })
    assert.match(stdout, /src\/actors\.ts/u)
    assert.match(stdout, /DURABLE_ACTORS_PROJECT_ID/u)
})
