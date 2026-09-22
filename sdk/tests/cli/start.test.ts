import assert from "node:assert/strict"
import { execFile } from "node:child_process"
import { mkdir, mkdtemp, readFile, rm, symlink, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import { test } from "node:test"
import { fileURLToPath } from "node:url"
import { promisify } from "node:util"

const run = promisify(execFile)
const sdk = fileURLToPath(new URL("../../../", import.meta.url))
const cli = path.join(sdk, "dist/cli.js")

test("start launches the configured runtime without a local actor project and propagates its exit code", async t => {
    const directory = await mkdtemp(path.join(tmpdir(), "durable-actors-start-"))
    t.after(() => rm(directory, { recursive: true, force: true }))
    const executable = path.join(directory, "runtime.mjs")
    await writeFile(
        executable,
        `#!/usr/bin/env node
console.log(JSON.stringify({ args: process.argv.slice(2), role: process.env.DURABLE_OBJECT_PROCESS_ROLE }))
process.exitCode = Number(process.env.TEST_RUNTIME_EXIT_CODE ?? 0)
`,
        { mode: 0o755 }
    )
    const env = {
        ...process.env,
        DURABLE_OBJECT_BINARY: executable,
        DURABLE_OBJECT_PROJECT_ID: "",
        DURABLE_OBJECT_PROJECT: path.join(directory, "missing-project"),
        DURABLE_OBJECT_PROCESS_ROLE: "control_plane"
    }
    const { stdout } = await run(process.execPath, [cli, "start"], { cwd: directory, env })
    assert.deepEqual(JSON.parse(stdout), { args: [], role: "control_plane" })
    await assert.rejects(
        run(process.execPath, [cli, "start"], { cwd: directory, env: { ...env, TEST_RUNTIME_EXIT_CODE: "7" } }),
        { code: 7 }
    )
})

test("dev validates its configured storage and port before launching", async t => {
    const directory = await mkdtemp(path.join(tmpdir(), "durable-actors-dev-options-"))
    t.after(() => rm(directory, { recursive: true, force: true }))
    for (const settings of [{ DURABLE_ACTORS_STORAGE: "invalid" }, { DURABLE_ACTORS_PORT: "65536" }])
        await assert.rejects(
            run(process.execPath, [cli, "dev"], {
                cwd: directory,
                env: { ...process.env, DURABLE_ACTORS_BINARY: "missing-runtime", ...settings }
            }),
            /DURABLE_ACTORS_STORAGE|Port must be an integer/
        )
})

test("dev compiles the project contract before launching and cleans it up when the runtime exits", async t => {
    const directory = await mkdtemp(path.join(tmpdir(), "durable-actors-dev-"))
    t.after(() => rm(directory, { recursive: true, force: true }))
    const project = path.join(directory, "actor project")
    await mkdir(path.join(project, "node_modules"), { recursive: true })
    await symlink(sdk, path.join(project, "node_modules/durable-actors"), "dir")
    await writeFile(path.join(project, "package.json"), '{"type":"module"}')
    const source = path.join(project, "actors.ts")
    await writeFile(
        source,
        'import { Actor } from "durable-actors"; export class Room extends Actor { async hello(): Promise<string> { return "hi" } }\nthrow new Error("must not execute actor source")'
    )
    const executable = path.join(directory, "runtime.mjs")
    await writeFile(
        executable,
        `#!/usr/bin/env node
import { createWriteStream, readFileSync, appendFileSync } from "node:fs"
import { createServer } from "node:http"
const args = process.argv.slice(2)
const index = args.indexOf("--contract")
if (index < 0) throw new Error("No public contract supplied to runtime")
const file = args[index + 1]
let updates = 0
const server = createServer((_request, response) => { updates++; response.end("{}") })
let controlPlaneUrl = "http://127.0.0.1:7100"
if (process.env.TEST_WATCH_SOURCE) {
    await new Promise(resolve => server.listen(0, "127.0.0.1", resolve))
    controlPlaneUrl = "http://127.0.0.1:" + server.address().port
}
createWriteStream(null, { fd: 3 }).end(JSON.stringify({
    projectId: "default",
    pid: process.pid,
    controlPlaneUrl,
    apiKey: "test-key",
    storageRegion: "local"
}))
if (process.env.TEST_WATCH_SOURCE) {
    setTimeout(() => appendFileSync(process.env.TEST_WATCH_SOURCE, "\\n// source changed"), 400)
    setTimeout(() => { server.close(); console.log(JSON.stringify({ updates })) }, 2500)
} else {
    console.log(JSON.stringify({ args, file, contract: JSON.parse(readFileSync(file, "utf8")) }))
}
process.exitCode = Number(process.env.TEST_RUNTIME_EXIT_CODE ?? 0)
`,
        { mode: 0o755 }
    )
    const env = {
        ...process.env,
        DURABLE_OBJECT_PROJECT_ID: "default",
        DURABLE_OBJECT_BINARY: executable,
        DURABLE_OBJECT_API_KEY: "test-key",
        DURABLE_OBJECT_PROJECT: project,
        DURABLE_OBJECT_ENTRYPOINT: "actors.ts"
    }
    const args = [cli, "dev", "--port", "0"]
    const { stdout } = await run(process.execPath, args, { cwd: directory, env })
    const result = JSON.parse(stdout)
    assert.equal(result.contract.actors[0].actorName, "Room")
    assert.deepEqual(
        result.contract.actors[0].rpc.methods.map((method: { name: string }) => method.name),
        ["hello"]
    )
    assert.ok(result.args.includes(project))
    const sdkHostIndex = result.args.indexOf("--sdk-host")
    assert.notEqual(sdkHostIndex, -1, "development mode must provide its host module")
    assert.equal(result.args[sdkHostIndex + 1], path.join(sdk, "dist/host.js"))
    await assert.rejects(readFile(result.file), { code: "ENOENT" })

    const { DURABLE_OBJECT_PROJECT_ID, ...withoutProjectId } = env
    const local = await run(process.execPath, args, { cwd: directory, env: withoutProjectId })
    const localArgs = JSON.parse(local.stdout).args
    assert.equal(localArgs[localArgs.indexOf("--project-id") + 1], "local")
    const configuredArgs = result.args
    assert.equal(configuredArgs[configuredArgs.indexOf("--project-id") + 1], "default")

    const failure = await run(process.execPath, args, {
        cwd: directory,
        env: { ...env, TEST_RUNTIME_EXIT_CODE: "7" }
    }).then(
        () => assert.fail("runtime failure should propagate"),
        (error: Error & { code: number; stdout: string }) => error
    )
    assert.equal(failure.code, 7)
    await assert.rejects(readFile(JSON.parse(failure.stdout).file), { code: "ENOENT" })
    const watching = await run(process.execPath, args, {
        cwd: directory,
        env: { ...env, TEST_WATCH_SOURCE: source }
    })
    assert.ok(
        JSON.parse(watching.stdout.trim().split("\n").at(-1)!).updates > 0,
        "development mode watches sources by default"
    )
    const notWatching = await run(process.execPath, [...args, "--no-watch"], {
        cwd: directory,
        env: { ...env, TEST_WATCH_SOURCE: source }
    })
    assert.equal(JSON.parse(notWatching.stdout).updates, 0, "--no-watch prevents source-triggered redeployments")

    await writeFile(
        source,
        'import { Actor } from "durable-actors"; export class Room extends Actor { async hello(value: Date): Promise<Date> { return value } }'
    )
    await assert.rejects(run(process.execPath, args, { cwd: directory, env }), /JSON-compatible/)
})
