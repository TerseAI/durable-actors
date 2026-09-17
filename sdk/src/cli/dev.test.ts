import assert from "node:assert/strict"
import { execFile } from "node:child_process"
import { mkdir, mkdtemp, readFile, rm, symlink, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import { test } from "node:test"
import { fileURLToPath } from "node:url"
import { promisify } from "node:util"

const run = promisify(execFile)
const sdk = fileURLToPath(new URL("../../../", import.meta.url))
const cli = path.join(sdk, "dist/cli.js")

test("dev compiles the project contract before launching and cleans it up when the runtime exits", async t => {
    const directory = await mkdtemp(path.join(tmpdir(), "little-actors-dev-"))
    t.after(() => rm(directory, { recursive: true, force: true }))
    const project = path.join(directory, "actor project")
    await mkdir(path.join(project, "node_modules"), { recursive: true })
    await symlink(sdk, path.join(project, "node_modules/little-actors"), "dir")
    await writeFile(path.join(project, "package.json"), '{"type":"module"}')
    const source = path.join(project, "actors.ts")
    await writeFile(
        source,
        'import { Actor } from "little-actors"; export class Room extends Actor { async hello(): Promise<string> { return "hi" } }\nthrow new Error("must not execute actor source")'
    )
    const executable = path.join(directory, "runtime.mjs")
    await writeFile(
        executable,
        `#!/usr/bin/env node
import { readFileSync } from "node:fs"
const args = process.argv.slice(2)
const index = args.indexOf("--contract")
if (index < 0) throw new Error("No public contract supplied to runtime")
const file = args[index + 1]
console.log(JSON.stringify({ args, file, contract: JSON.parse(readFileSync(file, "utf8")) }))
process.exitCode = Number(process.env.TEST_RUNTIME_EXIT_CODE ?? 0)
`,
        { mode: 0o755 }
    )
    const env = { ...process.env, DURABLE_OBJECT_BINARY: executable }
    const args = [cli, "dev", "--project", project, "--entrypoint", "actors.ts", "--port", "0"]
    const { stdout } = await run(process.execPath, args, { cwd: directory, env })
    const result = JSON.parse(stdout)
    assert.equal(result.contract.actors[0].actorType, "Room")
    assert.deepEqual(
        result.contract.actors[0].rpc.methods.map((method: { name: string }) => method.name),
        ["hello"]
    )
    assert.ok(result.args.includes(project))
    await assert.rejects(readFile(result.file), { code: "ENOENT" })

    const failure = await run(process.execPath, args, {
        cwd: directory,
        env: { ...env, TEST_RUNTIME_EXIT_CODE: "7" }
    }).then(
        () => assert.fail("runtime failure should propagate"),
        (error: Error & { code: number; stdout: string }) => error
    )
    assert.equal(failure.code, 7)
    await assert.rejects(readFile(JSON.parse(failure.stdout).file), { code: "ENOENT" })
    await writeFile(
        source,
        'import { Actor } from "little-actors"; export class Room extends Actor { async hello(value: Date): Promise<Date> { return value } }'
    )
    await assert.rejects(run(process.execPath, args, { cwd: directory, env }), /JSON-compatible/)
})
