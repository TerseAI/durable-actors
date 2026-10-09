import { startLocalActors } from "durable-actors/dev"
import assert from "node:assert/strict"
import { execFile } from "node:child_process"
import { mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import { test } from "node:test"
import { fileURLToPath } from "node:url"
import { promisify } from "node:util"

const run = promisify(execFile)
const project = fileURLToPath(new URL("..", import.meta.url))
const cli = path.resolve(project, "../cli")

test("the separate CLI reaches prompt validation through the generated client", { timeout: 60_000 }, async t => {
    const dataDir = await mkdtemp(path.join(tmpdir(), "pi-agent-scaffold-"))
    t.after(() => rm(dataDir, { recursive: true, force: true }))
    const apiKey = process.env.OPENAI_API_KEY
    process.env.OPENAI_API_KEY = ""
    let runtime
    try {
        runtime = await startLocalActors({ project, entrypoint: "src/actors.ts", dataDir, quiet: true })
    } finally {
        if (apiKey === undefined) delete process.env.OPENAI_API_KEY
        else process.env.OPENAI_API_KEY = apiKey
    }
    t.after(() => runtime.stop())
    const env = {
        ...process.env,
        DURABLE_ACTORS_PROJECT_ID: runtime.connection.projectId,
        DURABLE_ACTORS_CONTROL_PLANE_URL: runtime.connection.controlPlaneUrl
    }
    await run("pnpm", ["run", "generate"], { cwd: cli, env })
    await assert.rejects(run("pnpm", ["start", "prompt", "Hi Pi"], { cwd: cli, env }), error => {
        assert.match(error.stderr, /Set OPENAI_API_KEY in the pi-agent project's .env/)
        return true
    })
})
