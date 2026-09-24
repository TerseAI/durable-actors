import assert from "node:assert/strict"
import { execFile } from "node:child_process"
import { readFile } from "node:fs/promises"
import { test } from "node:test"
import { fileURLToPath } from "node:url"
import { promisify } from "node:util"

const run = promisify(execFile)
const cli = fileURLToPath(new URL("../../../dist/cli.js", import.meta.url))

test("the package exposes da with its installed version", async () => {
    const metadata = JSON.parse(await readFile(new URL("../../../package.json", import.meta.url), "utf8"))
    assert.equal(typeof metadata.bin.da, "string")
    const executable = fileURLToPath(new URL(`../../../${metadata.bin.da}`, import.meta.url))
    const { stdout } = await run(process.execPath, [executable, "--version"])
    assert.equal(stdout.trim(), metadata.version)
})

for (const args of [[], ["--help"]]) {
    test(`da displays help with ${args.length ? "--help" : "no arguments"}`, async () => {
        const { stdout } = await run(process.execPath, [cli, ...args])
        assert.match(stdout, /^Usage: da \[options\] \[command\]/u)
    })
}

test("CLI help explains server defaults and explicit source generation without requiring credentials", async () => {
    const { stdout } = await run(process.execPath, [cli, "generate", "--help"], {
        env: { ...process.env, DURABLE_ACTORS_PROJECT_ID: "", DURABLE_ACTORS_API_KEY: "" }
    })
    assert.match(stdout, /^Usage: da generate /u)
    assert.match(stdout, /--control-plane-url/u)
    assert.match(stdout, /http:\/\/127\.0\.0\.1:7100/u)
    assert.match(stdout, /source file/u)
    assert.match(stdout, /DURABLE_ACTORS_PROJECT_ID/u)
})

test("dev help explains the project directory, entrypoint, and setup", async () => {
    const { stdout } = await run(process.execPath, [cli, "dev", "--help"])
    assert.match(stdout, /current directory/u)
    assert.match(stdout, /src\/actors\.ts/u)
    assert.match(stdout, /DURABLE_ACTORS_PROJECT\b/u)
    assert.match(stdout, /DURABLE_ACTORS_ENTRYPOINT/u)
    assert.match(stdout, /^Usage: da dev /u)
    assert.match(stdout, /da init my-project/u)
})
