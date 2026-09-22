import assert from "node:assert/strict"
import { execFile, spawn } from "node:child_process"
import { once } from "node:events"
import { chmod, mkdir, mkdtemp, readFile, rm, symlink, writeFile } from "node:fs/promises"
import { createServer } from "node:http"
import { tmpdir } from "node:os"
import path from "node:path"
import { createInterface } from "node:readline"
import { test } from "node:test"
import { fileURLToPath } from "node:url"
import { promisify } from "node:util"

const run = promisify(execFile)
const cli = fileURLToPath(new URL("../../../dist/cli.js", import.meta.url))

test("observe serves a local UI using environment settings", { timeout: 10_000 }, async t => {
    const requests: string[] = []
    const server = createServer((request, response) => {
        requests.push(request.url!)
        assert.equal(request.method, "GET")
        assert.equal(request.headers.authorization, "Bearer observe-key")
        response.setHeader("content-type", "application/json")
        response.end(JSON.stringify({}))
    })
    t.after(() => server.close())
    server.listen(0, "127.0.0.1")
    await once(server, "listening")
    const origin = `http://127.0.0.1:${(server.address() as { port: number }).port}`
    const env = {
        ...process.env,
        DURABLE_ACTORS_PROJECT_ID: "default",
        DURABLE_ACTORS_CONTROL_PLANE_URL: origin,
        DURABLE_ACTORS_API_KEY: "observe-key"
    }
    const child = spawn(process.execPath, [cli, "observe", "--no-open"], {
        env,
        stdio: ["ignore", "pipe", "pipe"]
    })
    t.after(() => child.kill())
    const exited = once(child, "exit")
    const lines = createInterface({ input: child.stdout })
    let url: string | undefined
    for await (const line of lines) {
        if (line.startsWith("Observe: ")) {
            url = line.slice("Observe: ".length)
            break
        }
    }
    assert.ok(url, "observer should print the local UI URL")
    assert.match(url, /^http:\/\/127\.0\.0\.1:\d+$/u)
    const response = await fetch(url)
    assert.equal(response.status, 200)
    assert.match(await response.text(), /src="\.\/app.js"/u)
    assert.deepEqual(await (await fetch(`${url}/api/observe/connection`)).json(), { connected: true })
    assert.doesNotMatch(await (await fetch(`${url}/app.js`)).text(), /observe-key/u)
    child.kill("SIGTERM")
    assert.deepEqual(await exited, [0, null])
    await assert.rejects(fetch(url))
    assert.deepEqual(requests, Array(2).fill("/v1/observe/actors"))
})

test("observe exits unsuccessfully without a greeting when authentication or transport fails", async t => {
    const server = createServer((_request, response) => {
        response.writeHead(401, { "content-type": "application/json" })
        response.end(JSON.stringify({ error: { message: "Unauthorized" } }))
    })
    t.after(() => server.close())
    server.listen(0, "127.0.0.1")
    await once(server, "listening")
    const origin = `http://127.0.0.1:${(server.address() as { port: number }).port}`
    const args = [cli, "observe"]
    const env = {
        ...process.env,
        DURABLE_ACTORS_PROJECT_ID: "default",
        DURABLE_ACTORS_CONTROL_PLANE_URL: origin,
        DURABLE_ACTORS_API_KEY: "wrong"
    }
    const failure = (message: RegExp) => (error: unknown) => {
        const result = error as Error & { code: number; stdout: string; stderr: string }
        assert.equal(result.code, 1)
        assert.equal(result.stdout, "")
        assert.match(result.stderr, message)
        return true
    }
    await assert.rejects(run(process.execPath, args, { env }), failure(/HTTP 401.*Unauthorized/u))
    await new Promise<void>((resolve, reject) => server.close(error => (error ? reject(error) : resolve())))
    await assert.rejects(run(process.execPath, args, { env }), failure(/Cannot complete GET \/v1\/observe\/actors/u))
})

test("init creates a complete chat app using the installed SDK version", async t => {
    const directory = await mkdtemp(path.join(tmpdir(), "durable-actors-init-"))
    t.after(() => rm(directory, { recursive: true, force: true }))
    await run(process.execPath, [cli, "init", "my chat", "--template", "chat"], { cwd: directory })
    const project = path.join(directory, "my chat")
    const metadata = JSON.parse(await readFile(path.join(project, "package.json"), "utf8"))
    const sdk = JSON.parse(await readFile(new URL("../../../package.json", import.meta.url), "utf8"))
    assert.equal(metadata.dependencies["durable-actors"], sdk.version)
    assert.match(await readFile(path.join(project, "src/actors.ts"), "utf8"), /extends Actor/)
    assert.match(await readFile(path.join(project, "src/backend.ts"), "utf8"), /actors\.ChatRoom\.prepareWebsocket/)
    assert.match(await readFile(path.join(project, "src/Chat.tsx"), "utf8"), /new WebSocket\(websocketUrl\)/)
    assert.match(await readFile(path.join(project, ".gitignore"), "utf8"), /\.durable-actors\//)
})

test("init refuses an existing directory and preserves its contents", async t => {
    const directory = await mkdtemp(path.join(tmpdir(), "durable-actors-init-existing-"))
    t.after(() => rm(directory, { recursive: true, force: true }))
    const project = path.join(directory, "chat")
    await mkdir(project)
    const file = path.join(project, "package.json")
    await writeFile(file, "existing app")
    await assert.rejects(run(process.execPath, [cli, "init", project]), /already exists/)
    assert.equal(await readFile(file, "utf8"), "existing app")
})

test("dev accepts configured keys without logging the generated key from readiness", async t => {
    const directory = await mkdtemp(path.join(tmpdir(), "durable-actors-dev-env-"))
    t.after(() => rm(directory, { recursive: true, force: true }))
    const project = path.join(directory, "actor-project")
    await mkdir(path.join(project, "node_modules"), { recursive: true })
    await symlink(path.resolve(path.dirname(cli), ".."), path.join(project, "node_modules/durable-actors"), "dir")
    await writeFile(path.join(project, "package.json"), '{"type":"module"}')
    await writeFile(
        path.join(project, "actors.ts"),
        'import { Actor } from "durable-actors"; export class Room extends Actor {}'
    )
    const binary = path.join(directory, "runtime")
    await writeFile(
        binary,
        `#!${process.execPath}
require("node:fs").createWriteStream(null, { fd: 3 }).end(JSON.stringify({
    pid: process.pid, projectId: "default", controlPlaneUrl: "http://127.0.0.1:7200", apiKey: "dev-key", storageRegion: "local"
}))
console.log(JSON.stringify(process.argv.slice(2)))
`
    )
    await chmod(binary, 0o755)
    const env = {
        ...process.env,
        DURABLE_ACTORS_PROJECT_ID: "default",
        DURABLE_ACTORS_BINARY: binary,
        DURABLE_ACTORS_API_KEY: "dev-key",
        DURABLE_ACTORS_PROJECT: project,
        DURABLE_ACTORS_PORT: "7200",
        DURABLE_ACTORS_ENTRYPOINT: "actors.ts",
        DURABLE_ACTORS_STORAGE: "gcs",
        DURABLE_ACTORS_DATA_DIR: "/tmp/actor-state"
    }
    const { stdout } = await run(process.execPath, [cli, "dev"], { env })
    assert.doesNotMatch(stdout, /export DURABLE_ACTORS_API_KEY=/u)
    assert.deepEqual(JSON.parse(stdout).slice(0, 17), [
        "dev",
        "--project-id",
        "default",
        "--project",
        env.DURABLE_ACTORS_PROJECT,
        "--port",
        "7200",
        "--entrypoint",
        "actors.ts",
        "--storage",
        "gcs",
        "--sdk-host",
        path.resolve(path.dirname(cli), "host.js"),
        "--api-key",
        "dev-key",
        "--data-dir",
        env.DURABLE_ACTORS_DATA_DIR
    ])
    const overridden = await run(process.execPath, [cli, "dev", "--port", "7300"], { env })
    const args: string[] = JSON.parse(overridden.stdout)
    assert.equal(args[args.indexOf("--port") + 1], "7300")
    assert.equal(args[args.indexOf("--storage") + 1], "gcs")
    assert.equal(args[args.indexOf("--api-key") + 1], "dev-key")
    const { DURABLE_ACTORS_API_KEY, ...withoutKey } = env
    const envFile = path.join(project, ".env")
    await writeFile(envFile, "DURABLE_ACTORS_API_KEY=env-file-key\n")
    const configuredFromFile = await run(process.execPath, [cli, "dev"], { cwd: project, env: withoutKey })
    assert.doesNotMatch(configuredFromFile.stdout, /export DURABLE_ACTORS_API_KEY=/u)
    assert.equal(JSON.parse(configuredFromFile.stdout).includes("env-file-key"), true)
    await rm(envFile)
    const generated = await run(process.execPath, [cli, "dev"], { cwd: project, env: withoutKey })
    const invocation = generated.stdout.split("\n").find(line => line.startsWith("["))
    assert.ok(invocation)
    assert.equal(JSON.parse(invocation).includes("--api-key"), false)
    assert.doesNotMatch(generated.stdout + generated.stderr, /dev-key/u)
})
