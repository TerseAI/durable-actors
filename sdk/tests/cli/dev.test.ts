import assert from "node:assert/strict"
import { execFile } from "node:child_process"
import { mkdir, mkdtemp, realpath, rm, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import { test } from "node:test"
import { fileURLToPath } from "node:url"
import { promisify } from "node:util"

const run = promisify(execFile)
const cli = fileURLToPath(new URL("../../../dist/cli.js", import.meta.url))
const environment = Object.fromEntries(
    Object.entries(process.env).filter(([key]) => !key.startsWith("DURABLE_ACTORS_"))
)

test("dev explains invalid project paths before starting the runtime", async t => {
    const directory = await realpath(await mkdtemp(path.join(tmpdir(), "actor-dev-")))
    t.after(() => rm(directory, { recursive: true, force: true }))
    await mkdir(path.join(directory, "actors.ts"))
    await mkdir(path.join(directory, "custom"))
    await writeFile(path.join(directory, "file.ts"), "")
    const cases = [
        {
            name: "missing default entrypoint",
            settings: {},
            expected: "src/actors.ts",
            hint: "DURABLE_ACTORS_ENTRYPOINT"
        },
        {
            name: "missing project",
            settings: { DURABLE_ACTORS_PROJECT: "missing" },
            expected: "missing",
            hint: "DURABLE_ACTORS_PROJECT"
        },
        {
            name: "project is a file",
            settings: { DURABLE_ACTORS_PROJECT: "file.ts" },
            expected: "file.ts",
            hint: "DURABLE_ACTORS_PROJECT"
        },
        {
            name: "entrypoint is a directory",
            settings: { DURABLE_ACTORS_ENTRYPOINT: "actors.ts" },
            expected: "actors.ts",
            hint: "DURABLE_ACTORS_ENTRYPOINT"
        },
        {
            name: "entrypoint parent is a file",
            settings: { DURABLE_ACTORS_ENTRYPOINT: "file.ts/actors.ts" },
            expected: "file.ts/actors.ts",
            hint: "DURABLE_ACTORS_ENTRYPOINT"
        },
        {
            name: "configured project and entrypoint",
            settings: { DURABLE_ACTORS_PROJECT: "custom", DURABLE_ACTORS_ENTRYPOINT: "app/actors.ts" },
            expected: "custom/app/actors.ts",
            hint: "DURABLE_ACTORS_ENTRYPOINT"
        }
    ]
    for (const scenario of cases)
        for (const flags of [[], ["--no-watch"]])
            await t.test(`${scenario.name}${flags.length ? " without watching" : ""}`, async () => {
                await assert.rejects(
                    run(process.execPath, [cli, "dev", ...flags], {
                        cwd: directory,
                        env: {
                            ...environment,
                            DURABLE_ACTORS_BINARY: path.join(directory, "missing-runtime"),
                            ...scenario.settings
                        },
                        timeout: 5000
                    }),
                    (error: Error & { code?: number; stdout?: string; stderr?: string }) => {
                        assert.equal(error.code, 1)
                        assert.equal(error.stdout, "")
                        assert.ok(error.stderr?.includes(path.join(directory, scenario.expected)), error.stderr)
                        assert.ok(error.stderr?.includes(scenario.hint), error.stderr)
                        assert.match(error.stderr!, /durable-actors init my-project/u)
                        return true
                    }
                )
            })
})

test("dev resolves project and entrypoint settings from .env", async t => {
    const directory = await mkdtemp(path.join(tmpdir(), "actor-dev-env-"))
    t.after(() => rm(directory, { recursive: true, force: true }))
    await mkdir(path.join(directory, "project", "actors"), { recursive: true })
    await writeFile(path.join(directory, "project", "actors", "index.ts"), "export {}")
    await writeFile(
        path.join(directory, ".env"),
        "DURABLE_ACTORS_PROJECT=project\nDURABLE_ACTORS_ENTRYPOINT=actors/index.ts\n"
    )
    const executable = path.join(directory, "runtime.mjs")
    await writeFile(
        executable,
        `#!${process.execPath}
import { createWriteStream } from "node:fs"
createWriteStream(null, { fd: 3 }).end(JSON.stringify({
    projectId: "local", controlPlaneUrl: "http://127.0.0.1:7100"
}))
console.log(JSON.stringify(process.argv.slice(2)))
`,
        { mode: 0o755 }
    )
    const { stdout } = await run(process.execPath, [cli, "dev"], {
        cwd: directory,
        env: { ...environment, DURABLE_ACTORS_BINARY: executable },
        timeout: 5000
    })
    const args: string[] = JSON.parse(stdout)
    assert.equal(args[args.indexOf("--project") + 1], "project")
    assert.equal(args[args.indexOf("--entrypoint") + 1], "actors/index.ts")
})
