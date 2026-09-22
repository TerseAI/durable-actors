import assert from "node:assert/strict"
import { execFile } from "node:child_process"
import { once } from "node:events"
import { mkdir, mkdtemp, readFile, readdir, rm, symlink, writeFile } from "node:fs/promises"
import { createServer } from "node:http"
import { tmpdir } from "node:os"
import path from "node:path"
import { test } from "node:test"
import { fileURLToPath } from "node:url"
import { promisify } from "node:util"

const run = promisify(execFile)
const sdk = fileURLToPath(new URL("../../../", import.meta.url))
const cli = path.join(sdk, "dist/cli.js")
const env = { ...process.env, DURABLE_OBJECT_PROJECT_ID: "default", DURABLE_OBJECT_API_KEY: "contract-key" }

test("generate uses environment settings and explicit flags without reading discovery files", async t => {
    const directory = await mkdtemp(path.join(tmpdir(), "durable-actors-generate-local-"))
    t.after(() => rm(directory, { recursive: true, force: true }))
    const contract = JSON.parse(await readFile(path.join(sdk, "tests/fixtures/public-contract.json"), "utf8"))
    const requests: string[] = []
    const server = createServer((request, response) => {
        requests.push(request.url!)
        assert.equal(request.headers.authorization, "Bearer local-key")
        response.end(
            JSON.stringify({
                codeRevision: "local-revision",
                contractHash: `sha256:${"a".repeat(64)}`,
                contract
            })
        )
    })
    t.after(() => server.close())
    server.listen(0, "127.0.0.1")
    await once(server, "listening")
    const origin = `http://127.0.0.1:${(server.address() as { port: number }).port}`
    await mkdir(path.join(directory, ".durable-actors"))
    await writeFile(
        path.join(directory, ".durable-actors/runtime.json"),
        JSON.stringify({
            projectId: "default",
            controlPlaneUrl: origin,
            apiKey: "local-key"
        })
    )
    const localEnv = {
        ...process.env,
        DURABLE_OBJECT_PROJECT_ID: "default",
        DURABLE_OBJECT_CONTROL_PLANE_URL: origin,
        DURABLE_OBJECT_API_KEY: "local-key"
    }
    const generate = (...args: string[]) =>
        run(process.execPath, [cli, "generate", "--url", ...args], { cwd: directory, env: localEnv })
    const result = await generate()
    assert.match(result.stdout, /local-revision/)
    assert.ok((await readdir(path.join(directory, "generated"))).includes("index.ts"))
    await run(
        process.execPath,
        [cli, "generate", "--url", origin, "--project-id", "selected-project", "--api-key", "local-key"],
        {
            cwd: directory,
            env: {
                ...localEnv,
                DURABLE_OBJECT_CONTROL_PLANE_URL: "http://unreachable.invalid",
                DURABLE_OBJECT_API_KEY: "wrong"
            }
        }
    )
    assert.deepEqual(requests, [
        "/v1/projects/default/deployment/contract",
        "/v1/projects/selected-project/deployment/contract"
    ])
    await run(process.execPath, [cli, "generate", "--url"], {
        cwd: directory,
        env: {
            ...localEnv,
            DURABLE_ACTORS_PROJECT_ID: "branded-project",
            DURABLE_ACTORS_API_KEY: "local-key",
            DURABLE_ACTORS_CONTROL_PLANE_URL: origin,
            DURABLE_OBJECT_API_KEY: "wrong",
            DURABLE_OBJECT_CONTROL_PLANE_URL: "http://unreachable.invalid"
        }
    })
    assert.equal(requests.at(-1), "/v1/projects/branded-project/deployment/contract")
    await assert.rejects(
        run(process.execPath, [cli, "generate", "--url"], {
            cwd: directory,
            env: { ...localEnv, DURABLE_OBJECT_API_KEY: "" }
        }),
        /shared secret/
    )
    assert.equal(requests.length, 3)
})

test("deploy publishes the inferred API directly and a separate consumer generates identical clients without contract files", async t => {
    const directory = await mkdtemp(path.join(tmpdir(), "durable-actors-generate-"))
    t.after(() => rm(directory, { recursive: true, force: true }))
    const author = path.join(directory, "author")
    await mkdir(path.join(author, "src"), { recursive: true })
    await mkdir(path.join(author, "node_modules"))
    await symlink(sdk, path.join(author, "node_modules/durable-actors"), "dir")
    await mkdir(path.join(author, "node_modules/private-data"))
    await writeFile(path.join(author, "node_modules/private-data/package.json"), '{"types":"index.d.ts"}')
    await writeFile(
        path.join(author, "node_modules/private-data/index.d.ts"),
        "export interface Message { text: string }"
    )
    await writeFile(path.join(author, "package.json"), '{"type":"module"}')
    await writeFile(
        path.join(author, "tsconfig.json"),
        JSON.stringify({
            compilerOptions: {
                target: "ES2022",
                module: "NodeNext",
                moduleResolution: "NodeNext",
                strict: true,
                skipLibCheck: true
            }
        })
    )
    await writeFile(
        path.join(author, "src/durable-objects.ts"),
        `
        import { Actor } from "durable-actors"
        import type { Message } from "private-data"
        export class ChatRoom extends Actor<{}, Message, Message> {
            async sendMessage(input: Message): Promise<Message> { return input }
        }
        throw new Error("do not execute actor source")
    `
    )
    await mkdir(path.join(author, "generated"))
    await writeFile(path.join(author, "generated/contract.json"), "old generated contract")
    await writeFile(path.join(author, "generated/contract-source.json"), "old generated provenance")
    await run(process.execPath, [cli, "generate"], { cwd: author, env })
    const local = path.join(author, "generated")
    const files = await readdir(local)
    for (const file of ["index.ts"]) assert.ok(files.includes(file), `missing ${file}`)
    const expected = new Map(
        await Promise.all(files.map(async file => [file, await readFile(path.join(local, file), "utf8")] as const))
    )
    assert.ok(files.every(file => file.endsWith(".ts")))
    let deployment: any
    const requests: string[] = []
    const publication = {
        codeRevision: "release-1",
        contractHash: `sha256:${"a".repeat(64)}`,
        contract: JSON.parse(
            (
                await run(
                    "bun",
                    [
                        path.join(sdk, "dist/compiler/deployment-build.js"),
                        author,
                        "src/durable-objects.ts",
                        path.join(author, "build")
                    ],
                    { env }
                )
            ).stdout
        )
    }
    const server = createServer(async (request, response) => {
        assert.equal(request.headers.authorization, "Bearer contract-key")
        if (request.method === "PUT") {
            assert.equal(request.url, "/v1/projects/default/deployment")
            const chunks: Buffer[] = []
            for await (const chunk of request) chunks.push(Buffer.from(chunk))
            deployment = JSON.parse(Buffer.concat(chunks).toString())
            response.end(JSON.stringify({ changed: true }))
            return
        }
        assert.equal(request.method, "GET")
        requests.push(request.url!)
        response.setHeader("content-type", "application/json")
        response.end(JSON.stringify(publication))
    })
    t.after(() => server.close())
    server.listen(0, "127.0.0.1")
    await once(server, "listening")
    const origin = `http://127.0.0.1:${(server.address() as { port: number }).port}`
    const beforeDeploy = await readdir(author)
    const deployed = await run(
        process.execPath,
        [cli, "deploy", "--url", origin, "--image", "im-chat", "--revision", "release-1", "--secret", "chat-secrets"],
        { cwd: author, env }
    )
    assert.match(deployed.stdout, /release-1/)
    assert.deepEqual(await readdir(author), beforeDeploy)
    const { contract } = publication
    assert.deepEqual(deployment, {
        codeRevision: "release-1",
        imageRef: "im-chat",
        workingDirectory: "/customer",
        actorEntrypoint: "src/durable-objects.ts",
        secretRefs: ["chat-secrets"]
    })
    assert.equal(contract.version, 1)
    assert.equal(contract.actors[0].actorName, "ChatRoom")
    assert.equal(contract.actors[0].rpc.methods[0].name, "sendMessage")
    await rm(author, { recursive: true })
    const result = await run(process.execPath, [cli, "generate", "--url", origin, "--revision", "release-1"], {
        cwd: directory,
        env
    })
    assert.deepEqual(requests, ["/v1/projects/default/deployment/contract?revision=release-1"])
    for (const [file, content] of expected)
        assert.equal(await readFile(path.join(directory, "generated", file), "utf8"), content)
    assert.deepEqual(await readdir(path.join(directory, "generated")), files)
    assert.match(result.stdout, /release-1/)

    await run(process.execPath, [cli, "generate", "--url", "--out-dir", "active"], {
        cwd: directory,
        env: { ...env, DURABLE_OBJECT_CONTROL_PLANE_URL: origin }
    })
    assert.equal(requests[1], "/v1/projects/default/deployment/contract")
})

test("generate rejects remote errors and invalid inputs before changing output", async t => {
    const directory = await mkdtemp(path.join(tmpdir(), "durable-actors-generate-errors-"))
    t.after(() => rm(directory, { recursive: true, force: true }))
    await mkdir(path.join(directory, "generated"))
    await writeFile(path.join(directory, "generated/index.ts"), "keep existing output")
    const contract = JSON.parse(await readFile(path.join(sdk, "tests/fixtures/public-contract.json"), "utf8"))
    let status = 200
    let body: unknown = {
        codeRevision: "r1",
        contractHash: `sha256:${"a".repeat(64)}`,
        contract
    }
    let requests = 0
    const server = createServer((_request, response) => {
        requests++
        response.writeHead(status, { "content-type": "application/json" })
        response.end(JSON.stringify(body))
    })
    t.after(() => server.close())
    server.listen(0, "127.0.0.1")
    await once(server, "listening")
    const origin = `http://127.0.0.1:${(server.address() as { port: number }).port}`
    const generate = (...args: string[]) =>
        run(process.execPath, [cli, "generate", "--url", origin, ...args], { cwd: directory, env })
    await assert.rejects(generate("actors.ts"), /entrypoint/)
    await assert.rejects(generate("--config", "tsconfig.json"), /config/)
    await assert.rejects(
        run(process.execPath, [cli, "generate", "--url", origin], {
            cwd: directory,
            env: { ...env, DURABLE_OBJECT_API_KEY: "" }
        }),
        /shared secret/
    )
    assert.equal(requests, 0)
    await assert.rejects(generate("--revision", "different"), /revision/)
    contract.version = 2
    await assert.rejects(generate(), /version/)
    contract.version = 1
    contract.actors[0].rpc.schema.definitions.Method_sendMessage_Parameter_0 = { $ref: "file:///private/data.json" }
    await assert.rejects(generate(), /local definitions/)
    status = 404
    body = { error: { code: "contract_not_found", message: "No public actor contract is published" } }
    await assert.rejects(generate(), /No public actor contract is published/)
    assert.deepEqual(await readdir(path.join(directory, "generated")), ["index.ts"])
    assert.equal(await readFile(path.join(directory, "generated/index.ts"), "utf8"), "keep existing output")
})
