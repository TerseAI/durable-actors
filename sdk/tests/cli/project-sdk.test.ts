import assert from "node:assert/strict"
import { execFile } from "node:child_process"
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import { test } from "node:test"
import { fileURLToPath } from "node:url"
import { promisify } from "node:util"

import { projectSdkFixture } from "../fixtures/project-sdk.js"

const run = promisify(execFile)
const cli = fileURLToPath(new URL("../../../dist/cli.js", import.meta.url))

test("global dev runs the project's exported CLI entrypoint with its original arguments", async t => {
    const { directory, project, sdk } = await projectSdkFixture()
    t.after(() => rm(directory, { recursive: true, force: true }))
    const metadataFile = path.join(sdk, "package.json")
    const metadata = JSON.parse(await readFile(metadataFile, "utf8"))
    metadata.exports["./cli"] = "./project-cli.mjs"
    await writeFile(metadataFile, JSON.stringify(metadata))
    await writeFile(
        path.join(sdk, "project-cli.mjs"),
        "export async function runCli(args) { console.log(JSON.stringify(args.slice(2))); process.exitCode = 7 }"
    )
    await assert.rejects(
        run(process.execPath, [cli, "dev", "--project-only-option"], {
            cwd: project,
            env: { ...process.env, DURABLE_ACTORS_PROJECT: project }
        }),
        (error: Error & { code?: number; stdout?: string }) => {
            assert.equal(error.code, 7)
            assert.deepEqual(JSON.parse(error.stdout!), ["dev", "--project-only-option"])
            return true
        }
    )
})

test("global dev uses the project's SDK version to load and invoke actors", async t => {
    const { directory, project, binary } = await projectSdkFixture()
    t.after(() => rm(directory, { recursive: true, force: true }))
    const env = {
        ...process.env,
        DURABLE_ACTORS_BINARY: binary,
        DURABLE_ACTORS_PROJECT: project,
        DURABLE_ACTORS_ENTRYPOINT: "actors.ts",
        DURABLE_ACTORS_PROJECT_ID: "default"
    }
    await run(process.execPath, [cli, "dev", "--no-watch"], { cwd: directory, env })
    assert.deepEqual(JSON.parse(await readFile(path.join(project, "invocation.json"), "utf8")), {
        id: "counter",
        count: 1
    })
    await assert.rejects(
        run(process.execPath, [cli, "dev", "--no-watch"], {
            cwd: project,
            env: { ...env, TEST_RUNTIME_EXIT_CODE: "7" }
        }),
        { code: 7 }
    )
})

test("dev requires a project SDK installation before starting the runtime", async t => {
    const directory = await mkdtemp(path.join(tmpdir(), "actor-missing-sdk-"))
    t.after(() => rm(directory, { recursive: true, force: true }))
    await assert.rejects(
        run(process.execPath, [cli, "dev"], {
            cwd: directory,
            env: {
                ...process.env,
                DURABLE_ACTORS_PROJECT: directory,
                DURABLE_ACTORS_BINARY: "missing-runtime",
                NODE_PATH: path.resolve(path.dirname(cli), "../../node_modules")
            }
        }),
        /Cannot resolve durable-actors.*Run pnpm install/s
    )
})

test("dev reports an incomplete project CLI installation without using the global copy", async t => {
    const { directory, project, sdk } = await projectSdkFixture()
    t.after(() => rm(directory, { recursive: true, force: true }))
    const metadata = JSON.parse(await readFile(path.join(sdk, "package.json"), "utf8"))
    await rm(path.join(sdk, metadata.exports["./cli"].import))
    await assert.rejects(
        run(process.execPath, [cli, "dev"], {
            cwd: project,
            env: { ...process.env, DURABLE_ACTORS_PROJECT: project }
        }),
        /Cannot resolve durable-actors.*Run pnpm install/s
    )
})
