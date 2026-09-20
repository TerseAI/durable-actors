import assert from "node:assert/strict"
import { spawn } from "node:child_process"
import { once } from "node:events"
import { copyFile, mkdir, mkdtemp, readFile, realpath, rm, stat, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import test from "node:test"
import { fileURLToPath } from "node:url"

const script = fileURLToPath(new URL("../../scripts/tunnel.mjs", import.meta.url))

test("tunnel accepts NGROK_AUTH_TOKEN and a bare NGROK_DOMAIN", async t => {
    const run = await start(t, {}, "NGROK_AUTH_TOKEN=fixture-token\nNGROK_DOMAIN=chosen.ngrok.app\n")
    await run.ready()
    const invocation = JSON.parse(run.output().split("\n")[0])
    assert.equal(invocation.tokenPresent, true)
    assert.deepEqual(invocation.args.slice(-2), ["--url", "https://chosen.ngrok.app"])
    assert.doesNotMatch(run.output(), /fixture-token/)
})

test("NGROK_DOMAIN also accepts an HTTPS origin", async t => {
    const run = await start(t, { NGROK_DOMAIN: "https://chosen.ngrok.app" })
    await run.ready()
    assert.deepEqual(JSON.parse(run.output().split("\n")[0]).args.slice(-2), ["--url", "https://chosen.ngrok.app"])
})

test("tunnel updates existing URL assignments while preserving other settings and comments", async t => {
    const before = [
        "# local settings",
        "NGROK_AUTHTOKEN=fixture-token",
        "export DURABLE_OBJECT_CONTROL_PLANE_URL = 'https://old.test' # callback",
        'PRIVATE_KEY="first line',
        "DURABLE_OBJECT_CONTROL_PLANE_URL=inside-a-multiline-value",
        'last line"',
        "DURABLE_OBJECT_CONTROL_PLANE_URL=https://duplicate.test",
        "MODAL_TOKEN_SECRET=keep-this"
    ].join("\r\n")
    const run = await start(t, {}, before)
    await run.ready()
    assert.equal(await readFile(run.envFile, "utf8"), before.replace("'https://old.test'", "https://fixture.ngrok.app").replace("https://duplicate.test", "https://fixture.ngrok.app"))
})

test("tunnel appends a missing URL and creates .env when absent", async t => {
    for (const before of [undefined, "MODAL_TOKEN_ID=keep-this", "# settings\r\n"]) {
        const run = await start(t, {}, before)
        await run.ready()
        const newline = before?.includes("\r\n") ? "\r\n" : "\n"
        const prefix = before ? before + (before.endsWith("\n") ? "" : newline) : ""
        assert.equal(await readFile(run.envFile, "utf8"), `${prefix}DURABLE_OBJECT_CONTROL_PLANE_URL=https://fixture.ngrok.app${newline}`)
        if (before === undefined) assert.equal((await stat(run.envFile)).mode & 0o777, 0o600)
    }
})

test("a failed tunnel leaves .env unchanged", async t => {
    const before = "DURABLE_OBJECT_CONTROL_PLANE_URL=https://old.test\n"
    const run = await start(t, { FIXTURE_EXIT: "7" }, before)
    await run.closed
    assert.equal(await readFile(run.envFile, "utf8"), before)
})

test("a failed .env update stops ngrok and reports failure instead of readiness", async t => {
    const run = await start(t, { FIXTURE_ENV_DIRECTORY: "1" })
    assert.deepEqual(await run.closed, [1, null])
    assert.match(run.output(), /Could not update .env/)
    assert.match(run.output(), /ngrok stopped: SIGTERM/)
    assert.doesNotMatch(run.output(), /Tunnel ready/)
})

test("tunnel loads .env, forwards HTTP/2, and prints the discovered control-plane origin", async t => {
    const run = await start(t, {}, "NGROK_AUTHTOKEN=fixture-token\n")
    await run.ready()
    const invocation = JSON.parse(run.output().split("\n")[0])
    assert.deepEqual(invocation.args, ["http", "http://127.0.0.1:7100", "--upstream-protocol=http2", "--log=stdout", "--log-format=json", "--log-level=info"])
    assert.equal(invocation.tokenPresent, true)
    assert.match(run.output(), /export DURABLE_OBJECT_CONTROL_PLANE_URL=https:\/\/fixture.ngrok.app/)
    assert.doesNotMatch(run.output(), /fixture-token/)
    run.child.kill("SIGTERM")
    assert.deepEqual(await run.closed, [143, null])
    assert.match(run.output(), /ngrok stopped: SIGTERM/)
})

test("shell settings override .env and wildcard binds forward through loopback", async t => {
    const run = await start(t, { NGROK_URL: "https://chosen.ngrok.app", DURABLE_OBJECT_CONTROL_PLANE_BIND: "0.0.0.0:7200" }, "NGROK_URL=https://ignored.ngrok.app\n")
    await run.ready()
    const { args } = JSON.parse(run.output().split("\n")[0])
    assert.equal(args[1], "http://127.0.0.1:7200")
    assert.deepEqual(args.slice(-2), ["--url", "https://chosen.ngrok.app"])
    run.child.kill("SIGINT")
    assert.deepEqual(await run.closed, [130, null])
    assert.match(run.output(), /ngrok stopped: SIGINT/)
})

test("IPv6 wildcard binds forward through IPv6 loopback", async t => {
    const run = await start(t, { DURABLE_OBJECT_CONTROL_PLANE_BIND: "[::]:7300" })
    await run.ready()
    assert.equal(JSON.parse(run.output().split("\n")[0]).args[1], "http://[::1]:7300")
})

test("ngrok failures preserve their exit status and diagnostics", async t => {
    const run = await start(t, { FIXTURE_EXIT: "7" })
    assert.deepEqual(await run.closed, [7, null])
    assert.match(run.output(), /fixture authentication failure/)
    assert.doesNotMatch(run.output(), /export DURABLE_OBJECT_CONTROL_PLANE_URL/)
})

test("a missing ngrok executable has actionable diagnostics", async t => {
    const run = await start(t, { PATH: "/nonexistent" })
    assert.deepEqual(await run.closed, [1, null])
    assert.match(run.output(), /Install the ngrok CLI/)
})

test("invalid tunnel settings fail before starting ngrok", async t => {
    for (const env of [{ NGROK_URL: "http://insecure.test" }, { NGROK_URL: "https://example.com/path" }, { DURABLE_OBJECT_CONTROL_PLANE_BIND: "127.0.0.1:0" }]) {
        const run = await start(t, env)
        assert.deepEqual(await run.closed, [1, null])
        assert.doesNotMatch(run.output(), /tokenPresent/)
    }
})

test("cloud start passes the discovered URL to the runtime after saving .env", async t => {
    const run = await start(t, { DURABLE_OBJECT_CONTROL_PLANE_URL: "https://stale-shell.test" }, "MODAL_TOKEN_SECRET=fixture-secret\n", ["--start"])
    await run.ready('"runtimeStarted":true')
    const runtime = JSON.parse(
        run
            .output()
            .split("\n")
            .find(line => line.includes('"runtimeStarted":true'))
    )
    assert.deepEqual(runtime.args, ["start"])
    assert.equal(runtime.url, "https://fixture.ngrok.app")
    assert.equal(runtime.savedUrl, runtime.url)
    assert.equal(runtime.modalConfigured, true)
    assert.doesNotMatch(run.output(), /fixture-secret|Start or restart your control plane/)
})

test("cloud start stops both children on signals", async t => {
    for (const [signal, code] of [
        ["SIGINT", 130],
        ["SIGTERM", 143]
    ]) {
        const run = await start(t, {}, undefined, ["--start"])
        await run.ready('"runtimeStarted":true')
        run.child.kill(signal)
        assert.deepEqual(await run.closed, [code, null])
        assert.match(run.output(), /ngrok stopped: SIG/)
        assert.match(run.output(), /runtime stopped: SIG/)
    }
})

test("cloud start stops the runtime if ngrok fails after readiness", async t => {
    const run = await start(t, {}, undefined, ["--start"])
    await run.ready('"runtimeStarted":true')
    const ngrok = JSON.parse(run.output().split("\n")[0])
    process.kill(ngrok.pid, "SIGUSR1")
    assert.deepEqual(await run.closed, [7, null])
    assert.match(run.output(), /runtime stopped: SIGTERM/)
})

test("cloud start stops ngrok and preserves a runtime failure", async t => {
    const run = await start(t, {}, undefined, ["--start"])
    await run.ready('"runtimeStarted":true')
    const runtime = JSON.parse(
        run
            .output()
            .split("\n")
            .find(line => line.includes('"runtimeStarted":true'))
    )
    process.kill(runtime.pid, "SIGUSR1")
    assert.deepEqual(await run.closed, [9, null])
    assert.match(run.output(), /ngrok stopped: SIGTERM/)
})

test("cloud start never launches the runtime when tunnel setup fails", async t => {
    for (const env of [{ FIXTURE_EXIT: "7" }, { FIXTURE_ENV_DIRECTORY: "1" }]) {
        const run = await start(t, env, undefined, ["--start"])
        await run.closed
        assert.doesNotMatch(run.output(), /runtimeStarted/)
        assert.notEqual(run.child.exitCode, 0)
    }
})

async function start(t, env = {}, dotenv, args = []) {
    const cwd = await realpath(await mkdtemp(path.join(tmpdir(), "ldo-tunnel-")))
    const bin = path.join(cwd, "bin")
    await mkdir(bin)
    await copyFile(new URL("./fixtures/ngrok.mjs", import.meta.url), path.join(bin, "ngrok"))
    if (dotenv !== undefined) await writeFile(path.join(cwd, ".env"), dotenv)
    let executable = script
    if (args.includes("--start")) {
        await mkdir(path.join(cwd, "scripts"))
        await copyFile(script, path.join(cwd, "scripts/tunnel.mjs"))
        await mkdir(path.join(cwd, "sdk/dist"), { recursive: true })
        await copyFile(new URL("./fixtures/control-plane.mjs", import.meta.url), path.join(cwd, "sdk/dist/cli.js"))
        executable = path.join(cwd, "scripts/tunnel.mjs")
    }
    const child = spawn(process.execPath, [executable, ...args], {
        cwd,
        env: { PATH: `${bin}${path.delimiter}${process.env.PATH}`, ...env },
        stdio: ["ignore", "pipe", "pipe"]
    })
    const closed = once(child, "close")
    let output = ""
    for (const stream of [child.stdout, child.stderr]) stream.on("data", chunk => (output += chunk))
    t.after(async () => {
        if (child.exitCode === null && child.signalCode === null) child.kill("SIGTERM")
        await closed
        await rm(cwd, { recursive: true, force: true })
    })
    return {
        child,
        closed,
        envFile: path.join(cwd, ".env"),
        output: () => output,
        ready: async (marker = "export DURABLE_OBJECT_CONTROL_PLANE_URL=") => {
            const timeout = AbortSignal.timeout(5000)
            while (!output.includes(marker)) {
                await Promise.race([
                    once(child.stdout, "data", { signal: timeout }),
                    closed.then(() => {
                        throw new Error(`Tunnel exited before readiness: ${output}`)
                    })
                ])
            }
        }
    }
}
