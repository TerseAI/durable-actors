import assert from "node:assert/strict"
import { execFile } from "node:child_process"
import { once } from "node:events"
import { mkdir, mkdtemp, readdir, rm, symlink, writeFile } from "node:fs/promises"
import { createServer } from "node:http"
import { tmpdir } from "node:os"
import path from "node:path"
import { test } from "node:test"
import { fileURLToPath } from "node:url"
import { promisify } from "node:util"

const run = promisify(execFile)
const sdk = fileURLToPath(new URL("../../../", import.meta.url))
const cli = path.join(sdk, "dist/cli.js")

test("deploy validates source before publication, handles conflicts, and maps local source to the image entrypoint", async t => {
    const project = await mkdtemp(path.join(tmpdir(), "little-actors-deploy-"))
    t.after(() => rm(project, { recursive: true, force: true }))
    await mkdir(path.join(project, "node_modules"))
    await symlink(sdk, path.join(project, "node_modules/little-actors"), "dir")
    await writeFile(path.join(project, "package.json"), '{"type":"module"}')
    await writeFile(
        path.join(project, "actor-config.json"),
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
    const source = path.join(project, "actors.ts")
    await writeFile(
        source,
        'import { Actor } from "little-actors"; export class Room extends Actor { async hello(value: Date): Promise<Date> { return value } }'
    )
    let requests = 0
    let status = 409
    const server = createServer(async (request, response) => {
        requests++
        assert.equal(request.method, "PUT")
        assert.equal(request.url, "/v1/deployment")
        assert.equal(request.headers.authorization, "Bearer test-key")
        const chunks: Buffer[] = []
        for await (const chunk of request) chunks.push(Buffer.from(chunk))
        const body = JSON.parse(Buffer.concat(chunks).toString())
        assert.equal(body.actorEntrypoint, "lib/actors.ts")
        assert.deepEqual(
            body.contract.actors[0].rpc.methods.map((method: { name: string }) => method.name),
            ["hello"]
        )
        response.writeHead(status, {
            "content-type": "application/json",
            ...(status === 307 ? { location: "/should-not-follow" } : {})
        })
        response.end(
            JSON.stringify(
                status === 200
                    ? { changed: false }
                    : { error: { message: "A different contract is already published for this revision" } }
            )
        )
    })
    t.after(() => server.close())
    server.listen(0, "127.0.0.1")
    await once(server, "listening")
    const env = {
        ...process.env,
        DURABLE_OBJECT_API_KEY: "test-key",
        DURABLE_OBJECT_NAMESPACE_ID: "",
        DURABLE_OBJECT_CONTROL_PLANE_URL: `http://127.0.0.1:${(server.address() as { port: number }).port}`
    }
    const args = [
        cli,
        "deploy",
        source,
        "--image",
        "im-room",
        "--revision",
        "r1",
        "--working-directory",
        "/app",
        "--actor-entrypoint",
        "lib/actors.ts",
        "--config",
        "actor-config.json"
    ]
    await assert.rejects(run(process.execPath, args, { cwd: project, env }), /JSON-compatible/)
    assert.equal(requests, 0)
    await writeFile(
        source,
        'import { Actor } from "little-actors"; export class Room extends Actor { async hello(): Promise<string> { return "hi" } }\nthrow new Error("must not execute")'
    )
    await assert.rejects(
        run(process.execPath, [...args, "--revision", "bad/revision"], { cwd: project, env }),
        /codeRevision/
    )
    await assert.rejects(
        run(process.execPath, args, { cwd: project, env: { ...env, DURABLE_OBJECT_API_KEY: "" } }),
        /API key/
    )
    assert.equal(requests, 0)
    await assert.rejects(run(process.execPath, args, { cwd: project, env }), /HTTP 409.*different contract/)
    assert.equal(requests, 1)
    status = 307
    await assert.rejects(run(process.execPath, args, { cwd: project, env }), /Cannot complete PUT/)
    assert.equal(requests, 2)
    status = 200
    const result = await run(process.execPath, args, { cwd: project, env })
    assert.match(result.stdout, /Already registered revision r1/)
    assert.equal(requests, 3)
    assert.deepEqual((await readdir(project)).sort(), [
        "actor-config.json",
        "actors.ts",
        "node_modules",
        "package.json"
    ])
})
