import assert from "node:assert/strict"
import { execFile } from "node:child_process"
import { once } from "node:events"
import { mkdtemp, readdir, rm } from "node:fs/promises"
import { createServer } from "node:http"
import { tmpdir } from "node:os"
import path from "node:path"
import { test } from "node:test"
import { fileURLToPath } from "node:url"
import { promisify } from "node:util"

const run = promisify(execFile)
const cli = fileURLToPath(new URL("../../../dist/cli.js", import.meta.url))

test("deploy registers the source image in one request without local source or Modal credentials", async t => {
    const project = await mkdtemp(path.join(tmpdir(), "little-actors-deploy-"))
    t.after(() => rm(project, { recursive: true, force: true }))
    let requests = 0
    let status = 400
    const server = createServer(async (request, response) => {
        requests++
        assert.equal(request.method, "PUT")
        assert.equal(request.url, "/v1/projects/default/deployment")
        assert.equal(request.headers.authorization, "Bearer test-key")
        const chunks: Buffer[] = []
        for await (const chunk of request) chunks.push(Buffer.from(chunk))
        const body = JSON.parse(Buffer.concat(chunks).toString())
        assert.deepEqual(body, {
            imageRef: "im-customer",
            actorEntrypoint: "src/actors.ts",
            workingDirectory: "/project",
            secretRefs: ["project-secrets"]
        })
        response.writeHead(status, {
            "content-type": "application/json",
            ...(status === 307 ? { location: "/should-not-follow" } : {})
        })
        response.end(JSON.stringify(status === 200 ? { changed: false } : { error: { message: "Actor build failed" } }))
    })
    t.after(() => server.close())
    server.listen(0, "127.0.0.1")
    await once(server, "listening")
    const env = {
        ...process.env,
        DURABLE_OBJECT_PROJECT_ID: "default",
        DURABLE_OBJECT_API_KEY: "test-key",
        DURABLE_OBJECT_SANDBOX_COMMAND: "/does-not-exist",
        DURABLE_OBJECT_CONTROL_PLANE_URL: `http://127.0.0.1:${(server.address() as { port: number }).port}`
    }
    const args = [
        cli,
        "deploy",
        "src/actors.ts",
        "--image",
        "im-customer",
        "--working-directory",
        "/project",
        "--secret",
        "project-secrets"
    ]
    await assert.rejects(run(process.execPath, [...args, "--image", "bad-image"], { cwd: project, env }), /imageRef/)
    await assert.rejects(
        run(process.execPath, args, { cwd: project, env: { ...env, DURABLE_OBJECT_API_KEY: "" } }),
        /API key/
    )
    assert.equal(requests, 0)
    await assert.rejects(run(process.execPath, args, { cwd: project, env }), /HTTP 400.*Actor build failed/)
    assert.equal(requests, 1)
    status = 307
    await assert.rejects(run(process.execPath, args, { cwd: project, env }), /Cannot complete PUT/)
    assert.equal(requests, 2)
    status = 200
    const result = await run(process.execPath, args, { cwd: project, env })
    assert.match(result.stdout, /Deployment is up to date/)
    assert.equal(requests, 3)
    assert.deepEqual(await readdir(project), [])
})
