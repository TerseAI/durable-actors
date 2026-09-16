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
const env = { ...process.env, DURABLE_OBJECT_API_KEY: "contract-key", DURABLE_OBJECT_NAMESPACE_ID: "" }

test("generate discovers the local runtime and keeps explicit remote settings separate", async t => {
    const directory = await mkdtemp(path.join(tmpdir(), "little-actors-generate-local-"))
    t.after(() => rm(directory, { recursive: true, force: true }))
    const contract = JSON.parse(await readFile(path.join(sdk, "fixtures/public-contract.json"), "utf8"))
    const requests: string[] = []
    const server = createServer((request, response) => {
        requests.push(request.url!)
        assert.equal(request.headers.authorization, "Bearer local-key")
        response.end(
            JSON.stringify({
                namespaceId: "local",
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
    await mkdir(path.join(directory, ".little-actors"))
    await writeFile(
        path.join(directory, ".little-actors/runtime.json"),
        JSON.stringify({
            controlPlaneUrl: origin,
            apiKey: "local-key",
            namespaceId: "local"
        })
    )
    const localEnv = { ...process.env }
    for (const key of ["DURABLE_OBJECT_API_KEY", "DURABLE_OBJECT_CONTROL_PLANE_URL", "DURABLE_OBJECT_NAMESPACE_ID"])
        delete localEnv[key]
    const generate = (...args: string[]) =>
        run(process.execPath, [cli, "generate", "--url", ...args], {
            cwd: directory,
            env: localEnv
        })
    const result = await generate()
    assert.match(result.stdout, /local-revision/)
    assert.ok((await readdir(path.join(directory, "generated"))).includes("backend.ts"))
    assert.deepEqual(requests, ["/v1/namespaces/local/contract"])
    await assert.rejects(generate(origin), /API key/)
    await assert.rejects(generate("--api-key", "remote-key"), /--url/)
    assert.equal(requests.length, 1)
})

test("deploy publishes the inferred API directly and a separate consumer generates identical clients without contract files", async t => {
    const directory = await mkdtemp(path.join(tmpdir(), "little-actors-generate-"))
    t.after(() => rm(directory, { recursive: true, force: true }))
    const author = path.join(directory, "author")
    await mkdir(path.join(author, "src"), { recursive: true })
    await mkdir(path.join(author, "node_modules"))
    await symlink(sdk, path.join(author, "node_modules/little-actors"), "dir")
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
        import { Actor } from "little-actors"
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
    for (const file of ["index.ts", "proxy.ts", "backend.ts", "ChatRoom.backend.ts"])
        assert.ok(files.includes(file), `missing ${file}`)
    const expected = new Map(
        await Promise.all(files.map(async file => [file, await readFile(path.join(local, file), "utf8")] as const))
    )
    assert.ok(files.every(file => file.endsWith(".ts")))
    let deployment: any
    const requests: string[] = []
    const publication = {
        namespaceId: "team.prod",
        codeRevision: "release-1",
        contractHash: `sha256:${"a".repeat(64)}`,
        contract: undefined
    }
    const server = createServer(async (request, response) => {
        assert.equal(request.headers.authorization, "Bearer contract-key")
        if (request.method === "PUT") {
            assert.equal(request.url, "/v1/namespaces/team.prod/deployment")
            const chunks: Buffer[] = []
            for await (const chunk of request) chunks.push(Buffer.from(chunk))
            deployment = JSON.parse(Buffer.concat(chunks).toString())
            publication.contract = deployment.contract
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
        [
            cli,
            "deploy",
            "--url",
            origin,
            "--namespace",
            "team.prod",
            "--image",
            "im-chat",
            "--revision",
            "release-1",
            "--working-directory",
            "/app",
            "--secret",
            "chat-secrets",
            "--socket-gateway-url",
            "https://gateway.example.com",
            "--warm-region",
            "us-east"
        ],
        { cwd: author, env }
    )
    assert.match(deployed.stdout, /release-1/)
    assert.deepEqual(await readdir(author), beforeDeploy)
    const { contract, ...specification } = deployment
    assert.deepEqual(specification, {
        codeRevision: "release-1",
        imageRef: "im-chat",
        workingDirectory: "/app",
        actorEntrypoint: "src/durable-objects.ts",
        secretRefs: ["chat-secrets"],
        socketGatewayUrl: "https://gateway.example.com",
        warmRegion: "us-east"
    })
    assert.equal(contract.version, 1)
    assert.equal(contract.actors[0].actorType, "ChatRoom")
    assert.equal(contract.actors[0].rpc.methods[0].name, "sendMessage")
    await rm(author, { recursive: true })
    const result = await run(
        process.execPath,
        [cli, "generate", "--url", origin, "--namespace", "team.prod", "--revision", "release-1"],
        { cwd: directory, env }
    )
    assert.deepEqual(requests, ["/v1/namespaces/team.prod/contract?revision=release-1"])
    for (const [file, content] of expected)
        assert.equal(await readFile(path.join(directory, "generated", file), "utf8"), content)
    assert.deepEqual(await readdir(path.join(directory, "generated")), files)
    assert.match(result.stdout, /release-1/)

    await run(process.execPath, [cli, "generate", "--url", "--out-dir", "active"], {
        cwd: directory,
        env: { ...env, DURABLE_OBJECT_CONTROL_PLANE_URL: origin }
    })
    assert.equal(requests[1], "/v1/contract")
})

test("generate rejects remote errors and invalid inputs before changing output", async t => {
    const directory = await mkdtemp(path.join(tmpdir(), "little-actors-generate-errors-"))
    t.after(() => rm(directory, { recursive: true, force: true }))
    await mkdir(path.join(directory, "generated"))
    await writeFile(path.join(directory, "generated/backend.ts"), "keep existing output")
    const contract = JSON.parse(await readFile(path.join(sdk, "fixtures/public-contract.json"), "utf8"))
    let status = 200
    let body: unknown = {
        namespaceId: "default",
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
        /API key/
    )
    assert.equal(requests, 0)
    await assert.rejects(generate("--revision", "different"), /revision/)
    await assert.rejects(generate("--namespace", "different"), /namespace/)
    contract.version = 2
    await assert.rejects(generate(), /version/)
    contract.version = 1
    contract.actors[0].rpc.schema.definitions.Method_sendMessage_Parameter_0 = { $ref: "file:///private/data.json" }
    await assert.rejects(generate(), /local definitions/)
    status = 404
    body = { error: { code: "contract_not_found", message: "No public actor contract is published" } }
    await assert.rejects(generate(), /No public actor contract is published/)
    assert.deepEqual(await readdir(path.join(directory, "generated")), ["backend.ts"])
    assert.equal(await readFile(path.join(directory, "generated/backend.ts"), "utf8"), "keep existing output")
})
