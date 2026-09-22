import assert from "node:assert/strict"
import { execFile } from "node:child_process"
import { test } from "node:test"
import { fileURLToPath } from "node:url"
import { promisify } from "node:util"

const run = promisify(execFile)
const cli = fileURLToPath(new URL("../../../dist/cli.js", import.meta.url))

test("CLI help explains source defaults without requiring credentials", async () => {
    for (const command of ["generate", "deploy"]) {
        const { stdout } = await run(process.execPath, [cli, command, "--help"], {
            env: { ...process.env, DURABLE_OBJECT_PROJECT_ID: "", DURABLE_OBJECT_API_KEY: "" }
        })
        assert.match(stdout, /src\/durable-objects\.ts/u)
        assert.match(stdout, /DURABLE_ACTORS_PROJECT_ID/u)
    }
})
