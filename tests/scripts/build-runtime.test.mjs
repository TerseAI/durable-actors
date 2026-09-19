import assert from "node:assert/strict"
import { execFile } from "node:child_process"
import { createHash } from "node:crypto"
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import test from "node:test"
import { promisify } from "node:util"

import { RuntimeBuilder } from "../../scripts/build-runtime.mjs"

const execute = promisify(execFile)

test("native bundles contain both executable files and a matching download checksum", async t => {
    const root = await mkdtemp(path.join(tmpdir(), "ldo-bundle-"))
    t.after(() => rm(root, { recursive: true, force: true }))
    const run = async (command, args, options) => {
        if (command === "cargo") {
            await mkdir(path.join(root, "target/release"), { recursive: true })
            await writeFile(path.join(root, "target/release/little-actors"), "runtime", { mode: 0o755 })
        } else if (command === "go") {
            await writeFile(args[args.indexOf("-o") + 1], "provider", { mode: 0o755 })
        } else {
            return execute(command, args, options)
        }
    }
    const archive = await new RuntimeBuilder({ root, platform: "linux", arch: "arm64" }, run).build()
    assert.equal(path.basename(archive), "little-actors-linux-arm64.tar.gz")
    const checksum = createHash("sha256")
        .update(await readFile(archive))
        .digest("hex")
    assert.equal(await readFile(`${archive}.sha256`, "utf8"), `${checksum}  ${path.basename(archive)}\n`)
    const extracted = path.join(root, "extracted")
    await mkdir(extracted)
    await execute("tar", ["-xzf", archive, "-C", extracted])
    for (const [name, contents] of [
        ["little-actors", "runtime"],
        ["little-actors-modal-go", "provider"]
    ]) {
        assert.equal(await readFile(path.join(extracted, name), "utf8"), contents)
        await execute("test", ["-x", path.join(extracted, name)])
    }
})

test("a compiler failure does not publish a native bundle", async t => {
    const root = await mkdtemp(path.join(tmpdir(), "ldo-bundle-failure-"))
    t.after(() => rm(root, { recursive: true, force: true }))
    const builder = new RuntimeBuilder({ root }, async () => {
        throw new Error("compiler failed")
    })
    await assert.rejects(builder.build(), /compiler failed/)
    await assert.rejects(readFile(path.join(root, `dist-runtime/little-actors-${process.platform}-${process.arch}.tar.gz`)), { code: "ENOENT" })
})
