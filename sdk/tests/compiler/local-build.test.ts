import assert from "node:assert/strict"
import { execFile } from "node:child_process"
import { mkdtemp, rm, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import { type TestContext, test } from "node:test"
import { fileURLToPath } from "node:url"
import { promisify } from "node:util"

import { installSdk } from "../fixtures/installed-sdk.js"

const run = promisify(execFile)

test("local builds reject actor import failures before publishing a contract", async t => {
    const { args } = await localProject(t, 'throw new Error("actor import failed")')
    await assert.rejects(run("bun", args, { timeout: 15_000 }), (error: unknown) => {
        const failure = error as Error & { stdout: string; stderr: string }
        assert.equal(failure.stdout, "")
        assert.match(failure.stderr, /actor import failed/)
        return true
    })
})

test("local builds reject artifacts importing a different SDK copy", async t => {
    const { args } = await localProject(t, "")
    args[0] = fileURLToPath(new URL("../../../dist/compiler/deployment-build.js", import.meta.url))
    await assert.rejects(run("bun", args, { timeout: 15_000 }), /actor entrypoint has no named actor exports/)
})

test("validated actors retain identity and socket context and keep build output as JSON", async t => {
    const { sdk, project, args } = await localProject(t, 'console.log("loading actor")')
    const { stdout } = await run("bun", args, { timeout: 15_000 })
    assert.equal(JSON.parse(stdout).actors[0].actorName, "Counter")
    const script = fileURLToPath(new URL("../fixtures/invoke-built-actor.js", import.meta.url))
    const invoked = await run("bun", [script, sdk, path.join(project, "build/actors.mjs")], { timeout: 15_000 })
    assert.match(invoked.stdout, /invocation passed/)
})

async function localProject(t: TestContext, initialization: string) {
    const project = await mkdtemp(path.join(tmpdir(), "local-build-"))
    t.after(() => rm(project, { recursive: true, force: true }))
    const sdk = await installSdk(project)
    await writeFile(
        path.join(project, "actors.ts"),
        `
import { Actor } from "durable-actors"
${initialization}
export class Counter extends Actor<null, string> {
    async read(): Promise<string> {
        this.broadcast("hello")
        return this.id + ":" + (await this.getConnections()).length
    }
}
`
    )
    await writeFile(
        path.join(project, "tsconfig.json"),
        JSON.stringify({
            compilerOptions: {
                target: "ES2022",
                module: "NodeNext",
                moduleResolution: "NodeNext",
                strict: true,
                skipLibCheck: true,
                types: []
            }
        })
    )
    const args = [
        path.join(sdk, "dist/compiler/deployment-build.js"),
        project,
        "actors.ts",
        path.join(project, "build"),
        "local"
    ]
    return { sdk, project, args }
}
