import assert from "node:assert/strict"
import { execFile } from "node:child_process"
import { mkdir, mkdtemp, readFile, rm, symlink, writeFile } from "node:fs/promises"
import { createRequire } from "node:module"
import os from "node:os"
import path from "node:path"
import { test } from "node:test"
import { fileURLToPath } from "node:url"
import { promisify } from "node:util"

const sdk = fileURLToPath(new URL("../../", import.meta.url))
const require = createRequire(import.meta.url)
const run = promisify(execFile)

for (const [specifier, symbol] of [
    ["durable-actors", "Actor"],
    ["durable-actors/generated", "createActorStub"],
    ["durable-actors/backend", "createActorStub"],
    ["durable-actors/dev", "startLocalActors"],
    ["durable-actors/proxy", "SocketProxy"],
    ["durable-actors/host", "runActorHost"],
    ["durable-actors/compiler", "ActorCompiler"],
    ["durable-actors/codegen", "generateTypeScript"]
]) {
    test(`${specifier} resolves and loads the same API through import and require`, async () => {
        assert.equal(require.resolve(specifier), fileURLToPath(import.meta.resolve(specifier)))
        const commonjs = require(specifier)
        const esm = await import(specifier)
        assert.equal(typeof commonjs[symbol], "function")
        assert.equal(commonjs[symbol], esm[symbol])
    })
}

test("generated clients run through tsx in a package without a module type", async t => {
    const root = await commonjsProject(t)
    const { generateClient } = await import("../../dist/compiler/generators/client-generator.js")
    const contract = JSON.parse(await readFile(new URL("../fixtures/public-contract.json", import.meta.url), "utf8"))
    await generateClient(contract, path.join(root, "generated"))
    await writeFile(
        path.join(root, "client.ts"),
        `import assert from "node:assert/strict"
        import { actors, ActorProxy } from "./generated/index.js"
        assert.equal(typeof require, "function")
        assert.equal(typeof ActorProxy, "function")
        async function main() {
            const text: string = "hello"
            const actor = actors.ChatRoom.get("room-1", {
                async invoke(actorName, actorId, method, args) {
                    assert.deepEqual([actorName, actorId, method, args], ["ChatRoom", "room-1", "sendMessage", [{ text }]])
                    return { id: "message-1", text }
                }
            })
            assert.deepEqual(await actor.sendMessage({ text }), { id: "message-1", text })
        }
        main().catch(error => { console.error(error); process.exitCode = 1 })`
    )
    await run(process.execPath, ["--import", import.meta.resolve("tsx"), "client.ts"], { cwd: root })
})

for (const mode of ["module", "commonjs"]) {
    test(`the browser condition preserves server-only guards in ${mode} mode`, async () => {
        const imports =
            mode === "module"
                ? 'import assert from "node:assert/strict"; import { createActorStub, SocketProxy } from "durable-actors/generated";'
                : 'const assert = require("node:assert/strict"); const { createActorStub, SocketProxy } = require("durable-actors/generated");'
        await run(
            process.execPath,
            [
                "--conditions=browser",
                `--input-type=${mode}`,
                "--eval",
                `${imports}
                assert.throws(() => createActorStub(), /actors must be used on the server/);
                assert.throws(() => new SocketProxy(), /ActorProxy must be used on the server/);`
            ],
            { cwd: sdk }
        )
    })
}

async function commonjsProject(t) {
    const root = await mkdtemp(path.join(os.tmpdir(), "actor-commonjs-"))
    t.after(() => rm(root, { recursive: true, force: true }))
    await mkdir(path.join(root, "node_modules"))
    await symlink(sdk, path.join(root, "node_modules/durable-actors"), "dir")
    await writeFile(path.join(root, "package.json"), "{}")
    return root
}
