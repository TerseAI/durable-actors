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
    const project = await mkdtemp(path.join(tmpdir(), "durable-actors-deploy-"))
    t.after(() => rm(project, { recursive: true, force: true }))
    const revisions: string[] = []
    let requests = 0
    let status = 409
    const server = createServer(async (request, response) => {
        requests++
        assert.equal(request.method, "PUT")
        assert.equal(request.url, "/v1/projects/default/deployment")
        assert.equal(request.headers.authorization, "Bearer test-key")
        const chunks: Buffer[] = []
        for await (const chunk of request) chunks.push(Buffer.from(chunk))
        const body = JSON.parse(Buffer.concat(chunks).toString())
        revisions.push(body.codeRevision)
        assert.deepEqual(body, {
            codeRevision: body.codeRevision,
            imageRef: "im-customer",
            actorEntrypoint: "src/actors.ts",
            workingDirectory: "/project",
            secretRefs: ["project-secrets"]
        })
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
        DURABLE_ACTORS_PROJECT_ID: "default",
        DURABLE_ACTORS_SECRET: "test-key",
        DURABLE_ACTORS_SANDBOX_COMMAND: "/does-not-exist",
        DURABLE_ACTORS_CONTROL_PLANE_URL: `http://127.0.0.1:${(server.address() as { port: number }).port}`
    }
    const args = [
        cli,
        "deploy",
        "src/actors.ts",
        "--image",
        "im-customer",
        "--working-directory",
        "/project",
        "--revision",
        "r1",
        "--secret",
        "project-secrets"
    ]
    await assert.rejects(
        run(process.execPath, [...args, "--revision", "bad/revision"], { cwd: project, env }),
        /codeRevision/
    )
    await assert.rejects(
        run(process.execPath, args, { cwd: project, env: { ...env, DURABLE_ACTORS_SECRET: "" } }),
        /shared secret/
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
    const automaticArgs = args.filter((value, index) => value !== "--revision" && args[index - 1] !== "--revision")
    for (let attempt = 0; attempt < 2; attempt++) {
        const automatic = await run(process.execPath, automaticArgs, { cwd: project, env })
        const revision = revisions.at(-1)!
        assert.match(revision, /^[A-Za-z0-9._-]{1,128}$/u)
        assert.ok(automatic.stdout.includes(revision))
    }
    assert.notEqual(revisions.at(-1), revisions.at(-2))
    assert.deepEqual(await readdir(project), [])
})
