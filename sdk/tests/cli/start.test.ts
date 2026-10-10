import assert from "node:assert/strict"
import { execFile } from "node:child_process"
import { mkdir, mkdtemp, rm, symlink, writeFile } from "node:fs/promises"
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
        `#!/usr/bin/env bun
console.log(JSON.stringify({ args: process.argv.slice(2), role: process.env.DURABLE_ACTORS_PROCESS_ROLE }))
process.exitCode = Number(process.env.TEST_RUNTIME_EXIT_CODE ?? 0)
`,
        { mode: 0o755 }
    )
    const env = {
        ...process.env,
        DURABLE_ACTORS_BINARY: executable,
        DURABLE_ACTORS_PROJECT_ID: "",
        DURABLE_ACTORS_PROJECT: path.join(directory, "missing-project"),
        DURABLE_ACTORS_PROCESS_ROLE: "control_plane"
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

test("dev delegates actor sources and watch settings to the native runtime", async t => {
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
        `#!/usr/bin/env bun
import { createWriteStream } from "node:fs"
const args = process.argv.slice(2)
const controlPlaneUrl = "http://127.0.0.1:7100"
createWriteStream(null, { fd: 3 }).end(JSON.stringify({
    projectId: process.env.DURABLE_ACTORS_PROJECT_ID ?? "local",
    pid: process.pid,
    controlPlaneUrl,
    apiKey: process.env.DURABLE_ACTORS_SECRET ?? null,
    storageRegion: "local"
}))
console.log(JSON.stringify({ args }))
process.exitCode = Number(process.env.TEST_RUNTIME_EXIT_CODE ?? 0)
`,
        { mode: 0o755 }
    )
    const env = {
        ...process.env,
        DURABLE_ACTORS_PROJECT_ID: "default",
        DURABLE_ACTORS_BINARY: executable,
        DURABLE_ACTORS_SECRET: "test-key",
        DURABLE_ACTORS_PROJECT: project,
        DURABLE_ACTORS_ENTRYPOINT: "actors.ts"
    }
    const args = [cli, "dev", "--port", "0"]
    const { stdout } = await run(process.execPath, args, { cwd: directory, env })
    const result = JSON.parse(stdout)
    assert.equal(result.args[result.args.indexOf("--project") + 1], project)
    assert.equal(result.args[result.args.indexOf("--entrypoint") + 1], "actors.ts")
    const sdkHostIndex = result.args.indexOf("--sdk-host")
    assert.notEqual(sdkHostIndex, -1, "development mode must provide its host module")
    assert.equal(result.args[sdkHostIndex + 1], path.join(sdk, "dist/host.js"))

    const { DURABLE_ACTORS_PROJECT_ID, ...withoutProjectId } = env
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
    assert.ok(result.args.includes("--watch"), "development mode enables native watching")
    const notWatching = await run(process.execPath, [...args, "--no-watch"], { cwd: directory, env })
    assert.ok(!JSON.parse(notWatching.stdout).args.includes("--watch"))
})
