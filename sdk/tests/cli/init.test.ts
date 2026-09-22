import assert from "node:assert/strict"
import { execFile } from "node:child_process"
import { mkdir, mkdtemp, readFile, readdir, rm, symlink } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import { test } from "node:test"
import { fileURLToPath } from "node:url"
import { promisify, stripVTControlCharacters } from "node:util"

const run = promisify(execFile)
const sdk = fileURLToPath(new URL("../../../", import.meta.url))
const cli = path.join(sdk, "dist/cli.js")

test("init presents copyable next steps with optional terminal color", async t => {
    const directory = await mkdtemp(path.join(tmpdir(), "durable-actors-init-output-"))
    t.after(() => rm(directory, { recursive: true, force: true }))
    const name = "Sam's actors"
    const project = path.join(directory, name)
    const plain = await run(process.execPath, [cli, "init", name], {
        cwd: directory,
        env: { ...process.env, NO_COLOR: "1", FORCE_COLOR: undefined }
    })
    assert.match(plain.stdout, /durable actors\s+\/ new project/)
    assert.ok(plain.stdout.includes(`✓ ${name} is ready.`))
    assert.match(plain.stdout, /Start here/)
    assert.ok(plain.stdout.includes("cd -- 'Sam'\\''s actors'"))
    assert.match(plain.stdout, /pnpm install\n\n\s+Start the actor server\n\s+durable-actors dev/)
    assert.match(plain.stdout, /src\/durable-objects\.ts/)
    assert.match(plain.stdout, /Connect your app/)
    assert.equal(plain.stdout, stripVTControlCharacters(plain.stdout))
    await rm(project, { recursive: true })
    const colored = await run(process.execPath, [cli, "init", name], {
        cwd: directory,
        env: { ...process.env, FORCE_COLOR: "1", NO_COLOR: undefined, NODE_DISABLE_COLORS: undefined }
    })
    assert.notEqual(colored.stdout, stripVTControlCharacters(colored.stdout))
    assert.equal(stripVTControlCharacters(colored.stdout), plain.stdout)
})

test("init defaults to a standalone actor project that can generate and build", async t => {
    const directory = await mkdtemp(path.join(tmpdir(), "durable-actors-init-actor-"))
    t.after(() => rm(directory, { recursive: true, force: true }))
    const project = path.join(directory, "my actors")
    const { stdout } = await run(process.execPath, [cli, "init", project])
    const metadata = JSON.parse(await readFile(path.join(project, "package.json"), "utf8"))
    const installed = JSON.parse(await readFile(path.join(sdk, "package.json"), "utf8"))
    assert.deepEqual(metadata.dependencies, { "durable-actors": installed.version })
    assert.equal(metadata.scripts.dev, "durable-actors dev")
    assert.equal(metadata.scripts.build, "durable-actors build")
    assert.deepEqual(await readdir(path.join(project, "src")), ["durable-objects.ts"])
    assert.match(await readFile(path.join(project, ".gitignore"), "utf8"), /\.durable-actors\//)
    assert.match(stdout, /pnpm install/)
    assert.match(stdout, /\n\s+durable-actors dev\n/)
    assert.match(stdout, /separate.*project/i)
    assert.doesNotMatch(stdout, /localhost:3000|127\.0\.0\.1:3000|another terminal/)

    await mkdir(path.join(project, "node_modules"))
    await symlink(sdk, path.join(project, "node_modules/durable-actors"), "dir")
    await symlink(path.join(sdk, "node_modules/@types"), path.join(project, "node_modules/@types"), "dir")
    await run(process.execPath, [path.join(sdk, "node_modules/typescript/bin/tsc"), "--noEmit"], { cwd: project })
    await run(process.execPath, [cli, "generate"], { cwd: project })
    const generated = await readFile(path.join(project, "generated/index.ts"), "utf8")
    assert.match(generated, /Counter/)
    assert.match(generated, /increment/)
    await run(process.execPath, [cli, "build"], { cwd: project })
    assert.match(await readFile(path.join(project, "dist/actors.mjs"), "utf8"), /Counter/)
})
