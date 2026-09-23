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
const env = { ...process.env, DURABLE_ACTORS_PROJECT_ID: "default", DURABLE_ACTORS_SECRET: "contract-key" }

test("generate --remote uses .env settings and exported environment overrides", async t => {
    const directory = await mkdtemp(path.join(tmpdir(), "durable-actors-generate-local-"))
    t.after(() => rm(directory, { recursive: true, force: true }))
    const contract = JSON.parse(await readFile(path.join(sdk, "tests/fixtures/public-contract.json"), "utf8"))
    const requests: string[] = []
    const server = createServer((request, response) => {
        requests.push(request.url!)
        assert.equal(request.headers.authorization, "Bearer local-key")
        response.end(
            JSON.stringify({
                contractHash: `sha256:${"a".repeat(64)}`,
                contract
            })
        )
    })
    t.after(() => server.close())
    server.listen(0, "127.0.0.1")
    await once(server, "listening")
    const origin = `http://127.0.0.1:${(server.address() as { port: number }).port}`
    const envFile = path.join(directory, ".env")
    await writeFile(
        envFile,
        `DURABLE_ACTORS_PROJECT_ID=default\nDURABLE_ACTORS_CONTROL_PLANE_URL=${origin}\nDURABLE_ACTORS_SECRET=local-key\n`
    )
    const fileEnv = { ...process.env }
    for (const key of ["DURABLE_ACTORS_PROJECT_ID", "DURABLE_ACTORS_CONTROL_PLANE_URL", "DURABLE_ACTORS_SECRET"])
        delete fileEnv[key]
    const localEnv = {
        ...process.env,
        DURABLE_ACTORS_PROJECT_ID: "default",
        DURABLE_ACTORS_CONTROL_PLANE_URL: origin,
        DURABLE_ACTORS_SECRET: "local-key"
    }
    const result = await run(process.execPath, [cli, "generate", "--remote"], { cwd: directory, env: fileEnv })
    assert.match(result.stdout, /Generated 1 actor contract/)
    assert.ok((await readdir(path.join(directory, "generated"))).includes("index.ts"))
    await writeFile(
        envFile,
        "DURABLE_ACTORS_PROJECT_ID=wrong\nDURABLE_ACTORS_CONTROL_PLANE_URL=http://unreachable.invalid\nDURABLE_ACTORS_SECRET=wrong\n"
    )
    await run(process.execPath, [cli, "generate", "--remote"], { cwd: directory, env: localEnv })
    assert.deepEqual(requests, Array(2).fill("/v1/projects/default/deployment/contract"))
    await assert.rejects(
        run(process.execPath, [cli, "generate", "--remote"], {
            cwd: directory,
            env: { ...localEnv, DURABLE_ACTORS_SECRET: "" }
        }),
        /apiKey/
    )
    assert.equal(requests.length, 2)
})

test("a separate consumer generates identical clients from the deployed contract without local actor source", async t => {
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
        path.join(author, "src/actors.ts"),
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
    await run(process.execPath, [cli, "generate"], {
        cwd: author,
        env: { ...env, DURABLE_ACTORS_CONTROL_PLANE_URL: "http://unreachable.invalid" }
    })
    const local = path.join(author, "generated")
    const files = await readdir(local)
    for (const file of ["index.ts"]) assert.ok(files.includes(file), `missing ${file}`)
    const expected = new Map(
        await Promise.all(files.map(async file => [file, await readFile(path.join(local, file), "utf8")] as const))
    )
    assert.ok(files.every(file => file.endsWith(".ts")))
    const requests: string[] = []
    const publication = {
        contractHash: `sha256:${"a".repeat(64)}`,
        contract: JSON.parse(
            (
                await run(
                    "bun",
                    [
                        path.join(sdk, "dist/compiler/deployment-build.js"),
                        author,
                        "src/actors.ts",
                        path.join(author, "build")
                    ],
                    { env }
                )
            ).stdout
        )
    }
    const server = createServer((request, response) => {
        assert.equal(request.headers.authorization, "Bearer contract-key")
        assert.equal(request.method, "GET")
        requests.push(request.url!)
        response.setHeader("content-type", "application/json")
        response.end(JSON.stringify(publication))
    })
    t.after(() => server.close())
    server.listen(0, "127.0.0.1")
    await once(server, "listening")
    const origin = `http://127.0.0.1:${(server.address() as { port: number }).port}`
    const { contract } = publication
    assert.equal(contract.version, 1)
    assert.equal(contract.actors[0].actorName, "ChatRoom")
    assert.equal(contract.actors[0].rpc.methods[0].name, "sendMessage")
    await rm(author, { recursive: true })
    const result = await run(process.execPath, [cli, "generate", "--remote"], {
        cwd: directory,
        env: { ...env, DURABLE_ACTORS_CONTROL_PLANE_URL: origin }
    })
    assert.deepEqual(requests, ["/v1/projects/default/deployment/contract"])
    for (const [file, content] of expected)
        assert.equal(await readFile(path.join(directory, "generated", file), "utf8"), content)
    assert.deepEqual(await readdir(path.join(directory, "generated")), files)
    assert.match(result.stdout, /Generated 1 actor contract/)

    await run(process.execPath, [cli, "generate", "--remote", "--out-dir", "active"], {
        cwd: directory,
        env: { ...env, DURABLE_ACTORS_CONTROL_PLANE_URL: origin }
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
        run(process.execPath, [cli, "generate", "--remote", ...args], {
            cwd: directory,
            env: { ...env, DURABLE_ACTORS_CONTROL_PLANE_URL: origin }
        })
    await assert.rejects(generate("actors.ts"), /entrypoint/)
    await assert.rejects(generate("--config", "tsconfig.json"), /config/)
    await assert.rejects(
        run(process.execPath, [cli, "generate", "--remote"], {
            cwd: directory,
            env: { ...env, DURABLE_ACTORS_CONTROL_PLANE_URL: origin, DURABLE_ACTORS_SECRET: "" }
        }),
        /apiKey/
    )
    assert.equal(requests, 0)
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

test("generate --remote needs no project or secret for a local server", async t => {
    const directory = await mkdtemp(path.join(tmpdir(), "durable-actors-generate-no-auth-"))
    t.after(() => rm(directory, { recursive: true, force: true }))
    const contract = JSON.parse(await readFile(path.join(sdk, "tests/fixtures/public-contract.json"), "utf8"))
    const server = createServer((request, response) => {
        assert.equal(request.url, "/v1/projects/local/deployment/contract")
        assert.equal(request.headers.authorization, undefined)
        response.end(JSON.stringify({ contractHash: `sha256:${"a".repeat(64)}`, contract }))
    })
    t.after(() => server.close())
    server.listen(0, "127.0.0.1")
    await once(server, "listening")
    const environment = Object.fromEntries(
        Object.entries(process.env).filter(([key]) => !key.startsWith("DURABLE_ACTORS_"))
    )
    const result = await run(process.execPath, [cli, "generate", "--remote"], {
        cwd: directory,
        env: {
            ...environment,
            DURABLE_ACTORS_CONTROL_PLANE_URL: `http://127.0.0.1:${(server.address() as { port: number }).port}`
        }
    })
    assert.match(result.stdout, /Generated 1 actor contract/)
})
