import assert from "node:assert/strict"
import { execFile } from "node:child_process"
import { chmod, mkdir, mkdtemp, readFile, realpath, rm, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import { test } from "node:test"
import { fileURLToPath } from "node:url"
import { promisify } from "node:util"

import { installSdk } from "../fixtures/installed-sdk.js"

const run = promisify(execFile)
const cli = fileURLToPath(new URL("../../../dist/cli.js", import.meta.url))

test("dev explains how to install a missing project SDK", async t => {
    const project = await mkdtemp(path.join(tmpdir(), "missing-sdk-"))
    t.after(() => rm(project, { recursive: true, force: true }))
    await writeFile(path.join(project, "actors.ts"), "export {}")
    await assert.rejects(
        run(process.execPath, [cli, "dev", "--no-watch"], {
            cwd: project,
            env: { ...process.env, DURABLE_ACTORS_PROJECT: project, DURABLE_ACTORS_ENTRYPOINT: "actors.ts" }
        }),
        /Run pnpm install in the actor project/
    )
})

test("global dev uses the selected project's SDK and native runtime version", async t => {
    const directory = await mkdtemp(path.join(tmpdir(), "global-dev-"))
    t.after(() => rm(directory, { recursive: true, force: true }))
    const project = path.join(directory, "actor's project")
    const sdk = await installSdk(project, "9.8.7")
    await writeFile(
        path.join(project, "actors.ts"),
        'import { Actor } from "durable-actors"; export class Counter extends Actor {}'
    )
    const cache = path.join(directory, "cache")
    const { version } = JSON.parse(await readFile(new URL("../../../package.json", import.meta.url), "utf8"))
    await cacheRuntime(cache, version)
    const binary = await cacheRuntime(cache, "9.8.7")
    const env = {
        ...process.env,
        DURABLE_ACTORS_BINARY: undefined,
        DURABLE_ACTORS_CACHE_DIR: cache,
        DURABLE_ACTORS_PROJECT: project,
        DURABLE_ACTORS_ENTRYPOINT: "actors.ts"
    }
    const { stdout } = await run(process.execPath, [cli, "dev", "--no-watch", "--port", "7300"], {
        cwd: directory,
        env,
        timeout: 15_000
    })
    const launched = JSON.parse(stdout)
    assert.equal(launched.binary, binary)
    const args: string[] = launched.args
    assert.equal(args[args.indexOf("--sdk-host") + 1], path.join(sdk, "dist/host.js"))
    assert.equal(args[args.indexOf("--project") + 1], await realpath(project))
    assert.equal(args[args.indexOf("--port") + 1], "7300")
})

async function cacheRuntime(cache: string, version: string): Promise<string> {
    const runtime = path.join(cache, version, `${process.platform}-${process.arch}`)
    await mkdir(runtime, { recursive: true })
    const binary = path.join(runtime, "durable-actors")
    await writeFile(
        binary,
        `#!${process.execPath}
require("node:fs").createWriteStream(null, { fd: 3 }).end(JSON.stringify({
    projectId: "local", controlPlaneUrl: "http://127.0.0.1:7100", apiKey: "test-key"
}))
console.log(JSON.stringify({ binary: __filename, args: process.argv.slice(2) }))
`
    )
    await chmod(binary, 0o755)
    await writeFile(path.join(runtime, "durable-actors-modal-go"), "unused provider")
    return realpath(binary)
}
