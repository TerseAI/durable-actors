import assert from "node:assert/strict"
import { execFile } from "node:child_process"
import { once } from "node:events"
import { chmod, mkdir, mkdtemp, readFile, rm, symlink, writeFile } from "node:fs/promises"
import { createServer } from "node:http"
import { tmpdir } from "node:os"
import path from "node:path"
import { test } from "node:test"
import { fileURLToPath } from "node:url"
import { promisify } from "node:util"

const run = promisify(execFile)
const cli = fileURLToPath(new URL("../../dist/cli.js", import.meta.url))

test("token uses the configured namespace and reports server errors without following redirects", async t => {
    const directory = await mkdtemp(path.join(tmpdir(), "little-actors-token-"))
    t.after(() => rm(directory, { recursive: true, force: true }))
    let status = 200
    const requests: string[] = []
    const server = createServer(async (request, response) => {
        requests.push(request.url!)
        assert.equal(request.method, "POST")
        assert.equal(request.headers.authorization, "Bearer local-key")
        const chunks: Buffer[] = []
        for await (const chunk of request) chunks.push(Buffer.from(chunk))
        const body = JSON.parse(Buffer.concat(chunks).toString())
        assert.equal(body.storageRegion, "us-east")
        assert.match(body.executionId, /^cli-/u)
        assert.ok(body.deadlineUnixMs > Date.now())
        response.writeHead(status, {
            "content-type": "application/json",
            ...(status === 307 ? { location: "/redirected" } : {})
        })
        response.end(
            JSON.stringify(
                status === 200 ? { token: "execution-token" } : { error: { message: "No deployment registered" } }
            )
        )
    })
    t.after(() => server.close())
    server.listen(0, "127.0.0.1")
    await once(server, "listening")
    const args = [
        cli,
        "token",
        "--url",
        `http://127.0.0.1:${(server.address() as { port: number }).port}`,
        "--api-key",
        "local-key",
        "--namespace",
        "team.prod",
        "--region",
        "us-east"
    ]
    assert.equal((await run(process.execPath, args)).stdout.trim(), "execution-token")
    status = 409
    await assert.rejects(run(process.execPath, args), /HTTP 409.*No deployment registered/u)
    status = 307
    await assert.rejects(run(process.execPath, args), /Cannot complete POST/u)
    assert.deepEqual(requests, Array(3).fill("/v1/namespaces/team.prod/session-scoped-token"))
})

test("init creates a complete chat app using the installed SDK version", async t => {
    const directory = await mkdtemp(path.join(tmpdir(), "little-actors-init-"))
    t.after(() => rm(directory, { recursive: true, force: true }))
    const { stdout } = await run(process.execPath, [cli, "init", "my chat"], { cwd: directory })
    const project = path.join(directory, "my chat")
    const metadata = JSON.parse(await readFile(path.join(project, "package.json"), "utf8"))
    const sdk = JSON.parse(await readFile(new URL("../../package.json", import.meta.url), "utf8"))
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
        if (request.url!.includes("/state")) {
            response.end(JSON.stringify({ namespaceId: "local", stateVersion: 7, state: { secret: "saved" } }))
        } else {
            const secondPage = request.url!.includes("after=")
            response.end(
                JSON.stringify({
                    objects: [
                        {
                            namespaceId: "local",
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
    delete env.DURABLE_OBJECT_NAMESPACE_ID
    const flags = ["--url", origin, "--api-key", "local-key"]
    const listed = await run(process.execPath, [cli, "objects", "list", ...flags, "--all", "--json"], { env })
    assert.deepEqual(
        JSON.parse(listed.stdout).map((object: { actorId: string }) => object.actorId),
        ["one", "two"]
    )
    assert.equal(requests[0], "/v1/objects?limit=500")
    assert.match(requests[1]!, /after=object.v1.local.Room.one/u)
    const inspected = await run(process.execPath, [cli, "objects", "inspect", "Room", "one", ...flags], { env })
    assert.deepEqual(JSON.parse(inspected.stdout).state, { secret: "saved" })
    assert.equal(requests[2], "/v1/actors/Room/one/state")
})

test("objects uses cloud credentials, filters namespaces, and reports API errors", async t => {
    const requests: string[] = []
    const server = createServer((request, response) => {
        assert.equal(request.headers.authorization, "Bearer cloud-key")
        requests.push(request.url!)
        response.setHeader("content-type", "application/json")
        if (request.url!.includes("/state")) {
            response.statusCode = 404
            response.end(JSON.stringify({ error: { code: "not_found", message: "Object not found" } }))
        } else response.end(JSON.stringify({ objects: [], nextCursor: null }))
    })
    t.after(() => server.close())
    server.listen(0, "127.0.0.1")
    await once(server, "listening")
    const origin = `http://127.0.0.1:${(server.address() as { port: number }).port}`
    const env = { ...process.env, DURABLE_OBJECT_CONTROL_PLANE_URL: origin, DURABLE_OBJECT_API_KEY: "cloud-key" }
    const result = await run(process.execPath, [cli, "objects", "list", "--namespace", "team.prod"], { env })
    assert.match(result.stdout, /No saved objects/u)
    assert.equal(requests[0], "/v1/objects?namespace=team.prod&limit=50")
    await assert.rejects(
        run(process.execPath, [cli, "objects", "inspect", "Room", "missing", "--namespace", "team.prod"], { env }),
        /Object not found/u
    )
    assert.equal(requests[1], "/v1/namespaces/team.prod/actors/Room/missing/state")
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
        objectId: `object.v1.team.prod.Room.${index}`,
        namespaceId: "team.prod",
        actorType: "Room",
        actorId: String(index),
        homeRegion: "north-america-east",
        stateVersion: 1
    }))
    const server = createServer((request, response) => {
        const url = new URL(request.url!, "http://localhost")
        requests.push(url)
        const after = url.searchParams.get("after")
        const start = after ? objects.findIndex(object => object.objectId === after) + 1 : 0
        const end = Math.min(start + Number(url.searchParams.get("limit") ?? 100), objects.length)
        response.setHeader("content-type", "application/json")
        response.end(
            JSON.stringify({
                objects: objects.slice(start, end),
                nextCursor: end < objects.length ? objects[end - 1]!.objectId : null
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
    const args = [cli, "objects", "list", "--namespace", "team.prod"]
    const first = await run(process.execPath, args, { env })
    assert.equal(first.stdout.trim().split("\n").length, 51)
    assert.match(first.stderr, /--after 'object.v1.team.prod.Room.49'/u)
    assert.equal(requests.length, 1)
    assert.equal(requests[0]!.searchParams.get("limit"), "50")

    const limited = await run(process.execPath, [...args, "--limit", "2", "--json"], { env })
    assert.deepEqual(JSON.parse(limited.stdout), objects.slice(0, 2))
    assert.match(limited.stderr, /--after 'object.v1.team.prod.Room.1'/u)
    assert.equal(requests.length, 2)

    const last = await run(process.execPath, [...args, "--limit", "5", "--after", objects[49]!.objectId, "--json"], {
        env
    })
    assert.deepEqual(JSON.parse(last.stdout), objects.slice(50))
    assert.equal(last.stderr, "")
    assert.equal(requests.length, 3)
    assert.equal(requests[2]!.searchParams.get("namespace"), "team.prod")
    assert.equal(requests[2]!.searchParams.get("after"), objects[49]!.objectId)
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

test("dev accepts environment configuration and explicit flags override it", async t => {
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
    pid: process.pid, controlPlaneUrl: "http://127.0.0.1:7200", namespaceId: "local", apiKey: "dev-key", storageRegion: "local"
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
    await assert.rejects(run(process.execPath, [cli, "dev"], { env: withoutKey }), /api-key/)
})

test("token uses environment settings and flags without a discovery file", async t => {
    const requests: string[] = []
    const server = createServer((request, response) => {
        requests.push(request.url!)
        assert.equal(request.headers.authorization, "Bearer cli-key")
        response.setHeader("content-type", "application/json")
        response.end(JSON.stringify({ token: "session-token" }))
    })
    t.after(() => server.close())
    server.listen(0, "127.0.0.1")
    await once(server, "listening")
    const origin = `http://127.0.0.1:${(server.address() as { port: number }).port}`
    const env = {
        ...process.env,
        DURABLE_OBJECT_CONTROL_PLANE_URL: origin,
        DURABLE_OBJECT_API_KEY: "cli-key",
        DURABLE_OBJECT_NAMESPACE_ID: "local"
    }
    const result = await run(process.execPath, [cli, "token"], { env })
    assert.equal(result.stdout.trim(), "session-token")
    const overridden = await run(
        process.execPath,
        [cli, "token", "--url", origin, "--api-key", "cli-key", "--namespace", "explicit"],
        {
            env: {
                ...env,
                DURABLE_OBJECT_CONTROL_PLANE_URL: "http://unreachable.invalid",
                DURABLE_OBJECT_API_KEY: "wrong"
            }
        }
    )
    assert.equal(overridden.stdout.trim(), "session-token")
    assert.deepEqual(requests, [
        "/v1/namespaces/local/session-scoped-token",
        "/v1/namespaces/explicit/session-scoped-token"
    ])
})
