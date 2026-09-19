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

test("observe serves a local UI using environment settings or flag overrides", { timeout: 10_000 }, async t => {
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
        DURABLE_OBJECT_CONTROL_PLANE_URL: origin,
        DURABLE_OBJECT_API_KEY: "observe-key"
    }
    for (const args of [[], ["--url", origin, "--api-key", "observe-key"]]) {
        const child = spawn(process.execPath, [cli, "observe", "--no-open", ...args], {
            env: args.length
                ? {
                      ...env,
                      DURABLE_OBJECT_CONTROL_PLANE_URL: "http://unreachable.invalid",
                      DURABLE_OBJECT_API_KEY: "wrong"
                  }
                : env,
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
    }
    assert.deepEqual(requests, Array(4).fill("/v1/actors?limit=1"))
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
    const args = [cli, "observe", "--url", origin, "--api-key", "wrong"]
    const failure = (message: RegExp) => (error: unknown) => {
        const result = error as Error & { code: number; stdout: string; stderr: string }
        assert.equal(result.code, 1)
        assert.equal(result.stdout, "")
        assert.match(result.stderr, message)
        return true
    }
    await assert.rejects(run(process.execPath, args), failure(/HTTP 401.*Unauthorized/u))
    await new Promise<void>((resolve, reject) => server.close(error => (error ? reject(error) : resolve())))
    await assert.rejects(run(process.execPath, args), failure(/Cannot complete GET \/v1\/actors/u))
})

test("init creates a complete chat app using the installed SDK version", async t => {
    const directory = await mkdtemp(path.join(tmpdir(), "little-actors-init-"))
    t.after(() => rm(directory, { recursive: true, force: true }))
    const { stdout } = await run(process.execPath, [cli, "init", "my chat"], { cwd: directory })
    const project = path.join(directory, "my chat")
    const metadata = JSON.parse(await readFile(path.join(project, "package.json"), "utf8"))
    const sdk = JSON.parse(await readFile(new URL("../../../package.json", import.meta.url), "utf8"))
    assert.equal(metadata.dependencies["little-actors"], sdk.version)
    assert.match(await readFile(path.join(project, "src/durable-objects.ts"), "utf8"), /extends Actor/)
    assert.match(await readFile(path.join(project, "src/backend.ts"), "utf8"), /actors\.ChatRoom\.prepareWebsocket/)
    assert.match(await readFile(path.join(project, "src/Chat.tsx"), "utf8"), /new WebSocket\(websocketUrl\)/)
    assert.match(await readFile(path.join(project, ".gitignore"), "utf8"), /\.little-actors\//)
    assert.match(stdout, /npm install/)
    assert.match(stdout, /little-actors generate/)
})

test("init refuses an existing directory and preserves its contents", async t => {
    const directory = await mkdtemp(path.join(tmpdir(), "little-actors-init-existing-"))
    t.after(() => rm(directory, { recursive: true, force: true }))
    const project = path.join(directory, "chat")
    await mkdir(project)
    const file = path.join(project, "package.json")
    await writeFile(file, "existing app")
    await assert.rejects(run(process.execPath, [cli, "init", project]), /already exists/)
    assert.equal(await readFile(file, "utf8"), "existing app")
})

test("objects lists every page locally and inspects committed internal state", async t => {
    const requests: string[] = []
    const server = createServer((request, response) => {
        assert.equal(request.headers.authorization, "Bearer local-key")
        requests.push(request.url!)
        response.setHeader("content-type", "application/json")
        if (request.url!.includes("?include=state")) {
            response.end(JSON.stringify({ stateVersion: 7, state: { secret: "saved" } }))
        } else {
            const secondPage = request.url!.includes("after=")
            response.end(
                JSON.stringify({
                    actors: [
                        {
                            actorType: "Room",
                            actorId: secondPage ? "two" : "one",
                            stateVersion: 7
                        }
                    ],
                    nextCursor: secondPage ? null : "object.v1.local.Room.one"
                })
            )
        }
    })
    t.after(() => server.close())
    server.listen(0, "127.0.0.1")
    await once(server, "listening")
    const origin = `http://127.0.0.1:${(server.address() as { port: number }).port}`
    const env: NodeJS.ProcessEnv = {
        ...process.env,
        DURABLE_OBJECT_CONTROL_PLANE_URL: "",
        DURABLE_OBJECT_API_KEY: ""
    }
    const flags = ["--url", origin, "--api-key", "local-key"]
    const listed = await run(process.execPath, [cli, "objects", "list", ...flags, "--all", "--json"], { env })
    assert.deepEqual(
        JSON.parse(listed.stdout).map((object: { actorId: string }) => object.actorId),
        ["one", "two"]
    )
    assert.equal(requests[0], "/v1/actors?limit=500")
    assert.match(requests[1]!, /after=object.v1.local.Room.one/u)
    const inspected = await run(process.execPath, [cli, "objects", "inspect", "Room", "one", ...flags], { env })
    assert.deepEqual(JSON.parse(inspected.stdout).state, { secret: "saved" })
    assert.equal(requests[2], "/v1/actors/Room/one?include=state")
})

test("objects uses cloud credentials, and reports API errors", async t => {
    const requests: string[] = []
    const server = createServer((request, response) => {
        assert.equal(request.headers.authorization, "Bearer cloud-key")
        requests.push(request.url!)
        response.setHeader("content-type", "application/json")
        if (request.url!.includes("?include=state")) {
            response.statusCode = 404
            response.end(JSON.stringify({ error: { code: "not_found", message: "Object not found" } }))
        } else response.end(JSON.stringify({ actors: [], nextCursor: null }))
    })
    t.after(() => server.close())
    server.listen(0, "127.0.0.1")
    await once(server, "listening")
    const origin = `http://127.0.0.1:${(server.address() as { port: number }).port}`
    const env = { ...process.env, DURABLE_OBJECT_CONTROL_PLANE_URL: origin, DURABLE_OBJECT_API_KEY: "cloud-key" }
    const result = await run(process.execPath, [cli, "objects", "list"], { env })
    assert.match(result.stdout, /No saved objects/u)
    assert.equal(requests[0], "/v1/actors?limit=50")
    await assert.rejects(
        run(process.execPath, [cli, "objects", "inspect", "Room", "missing"], { env }),
        /Object not found/u
    )
    assert.equal(requests[1], "/v1/actors/Room/missing?include=state")
    await assert.rejects(
        run(process.execPath, [cli, "objects", "list", "--url", origin], {
            env: { ...env, DURABLE_OBJECT_API_KEY: "" }
        }),
        /API key/u
    )
    assert.equal(requests.length, 2)
})

test("objects limits rows by default and resumes a filtered page without fetching ahead", async t => {
    const requests: URL[] = []
    const objects = Array.from({ length: 55 }, (_, index) => ({
        actorType: "Room",
        actorId: String(index),
        homeRegion: "north-america-east",
        stateVersion: 1
    }))
    const server = createServer((request, response) => {
        const url = new URL(request.url!, "http://localhost")
        requests.push(url)
        const after = url.searchParams.get("after")
        const start = after ? objects.findIndex(object => `object.v3.Room:${object.actorId}` === after) + 1 : 0
        const end = Math.min(start + Number(url.searchParams.get("limit") ?? 100), objects.length)
        response.setHeader("content-type", "application/json")
        response.end(
            JSON.stringify({
                actors: objects.slice(start, end),
                nextCursor: end < objects.length ? `object.v3.Room:${objects[end - 1]!.actorId}` : null
            })
        )
    })
    t.after(() => server.close())
    server.listen(0, "127.0.0.1")
    await once(server, "listening")
    const env = {
        ...process.env,
        DURABLE_OBJECT_CONTROL_PLANE_URL: `http://127.0.0.1:${(server.address() as { port: number }).port}`,
        DURABLE_OBJECT_API_KEY: "cloud-key"
    }
    const args = [cli, "objects", "list"]
    const first = await run(process.execPath, args, { env })
    assert.equal(first.stdout.trim().split("\n").length, 51)
    assert.match(first.stderr, /--after 'object.v3.Room:49'/u)
    assert.equal(requests.length, 1)
    assert.equal(requests[0]!.searchParams.get("limit"), "50")

    const limited = await run(process.execPath, [...args, "--limit", "2", "--json"], { env })
    assert.deepEqual(JSON.parse(limited.stdout), objects.slice(0, 2))
    assert.match(limited.stderr, /--after 'object.v3.Room:1'/u)
    assert.equal(requests.length, 2)

    const last = await run(process.execPath, [...args, "--limit", "5", "--after", "object.v3.Room:49", "--json"], {
        env
    })
    assert.deepEqual(JSON.parse(last.stdout), objects.slice(50))
    assert.equal(last.stderr, "")
    assert.equal(requests.length, 3)
    assert.equal(requests[2]!.searchParams.get("after"), "object.v3.Room:49")
    assert.equal(requests[2]!.searchParams.get("limit"), "5")
})

test("objects rejects invalid limits and conflicting pagination flags before connecting", async () => {
    for (const value of ["0", "-1", "1.5", "501", "1e2", "abc"]) {
        await assert.rejects(
            run(process.execPath, [cli, "objects", "list", "--limit", value]),
            /Limit must be an integer from 1 to 500/u
        )
    }
    for (const flags of [
        ["--all", "--limit", "10"],
        ["--all", "--after", "cursor"]
    ]) {
        await assert.rejects(run(process.execPath, [cli, "objects", "list", ...flags]), /cannot be used with/u)
    }
})

test("dev accepts configured keys and prints an export command for a generated key", async t => {
    const directory = await mkdtemp(path.join(tmpdir(), "little-actors-dev-env-"))
    t.after(() => rm(directory, { recursive: true, force: true }))
    const project = path.join(directory, "actor-project")
    await mkdir(path.join(project, "node_modules"), { recursive: true })
    await symlink(path.resolve(path.dirname(cli), ".."), path.join(project, "node_modules/little-actors"), "dir")
    await writeFile(path.join(project, "package.json"), '{"type":"module"}')
    await writeFile(
        path.join(project, "actors.ts"),
        'import { Actor } from "little-actors"; export class Room extends Actor {}'
    )
    const binary = path.join(directory, "runtime")
    await writeFile(
        binary,
        `#!${process.execPath}
require("node:fs").createWriteStream(null, { fd: 3 }).end(JSON.stringify({
    pid: process.pid, controlPlaneUrl: "http://127.0.0.1:7200", apiKey: "dev-key", storageRegion: "local"
}))
console.log(JSON.stringify(process.argv.slice(2)))
`
    )
    await chmod(binary, 0o755)
    const env = {
        ...process.env,
        DURABLE_OBJECT_BINARY: binary,
        DURABLE_OBJECT_API_KEY: "dev-key",
        DURABLE_OBJECT_PROJECT: project,
        DURABLE_OBJECT_PORT: "7200",
        DURABLE_OBJECT_ENTRYPOINT: "actors.ts",
        DURABLE_OBJECT_STORAGE: "gcs",
        DURABLE_OBJECT_DATA_DIR: "/tmp/actor-state"
    }
    const { stdout } = await run(process.execPath, [cli, "dev"], { env })
    assert.doesNotMatch(stdout, /export DURABLE_OBJECT_API_KEY=/u)
    assert.deepEqual(JSON.parse(stdout).slice(0, 13), [
        "dev",
        "--project",
        env.DURABLE_OBJECT_PROJECT,
        "--port",
        "7200",
        "--entrypoint",
        "actors.ts",
        "--storage",
        "gcs",
        "--api-key",
        "dev-key",
        "--data-dir",
        env.DURABLE_OBJECT_DATA_DIR
    ])
    const overridden = await run(
        process.execPath,
        [cli, "dev", "--port", "7300", "--storage", "local", "--api-key", "flag-key"],
        { env }
    )
    const args: string[] = JSON.parse(overridden.stdout)
    assert.equal(args[args.indexOf("--port") + 1], "7300")
    assert.equal(args[args.indexOf("--storage") + 1], "local")
    assert.equal(args[args.indexOf("--api-key") + 1], "flag-key")
    const { DURABLE_OBJECT_API_KEY, ...withoutKey } = env
    const envFile = path.join(project, ".env")
    await writeFile(envFile, "DURABLE_OBJECT_API_KEY=env-file-key\n")
    const configuredFromFile = await run(process.execPath, [cli, "dev"], { cwd: project, env: withoutKey })
    assert.doesNotMatch(configuredFromFile.stdout, /export DURABLE_OBJECT_API_KEY=/u)
    assert.equal(JSON.parse(configuredFromFile.stdout).includes("env-file-key"), true)
    await rm(envFile)
    const generated = await run(process.execPath, [cli, "dev"], { cwd: project, env: withoutKey })
    const invocation = generated.stdout.split("\n").find(line => line.startsWith("["))
    assert.ok(invocation)
    assert.equal(JSON.parse(invocation).includes("--api-key"), false)
    assert.match(generated.stdout, /export DURABLE_OBJECT_API_KEY=dev-key/u)
})
