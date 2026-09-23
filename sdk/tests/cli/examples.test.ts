import assert from "node:assert/strict"
import { execFile } from "node:child_process"
import { copyFile, mkdtemp, readFile, rm, symlink, writeFile } from "node:fs/promises"
import path from "node:path"
import { test } from "node:test"
import { fileURLToPath } from "node:url"
import { promisify } from "node:util"
import ts from "typescript"

import { ActorCompiler } from "../../src/compiler/actor-compiler.js"
import { generateClient } from "../../src/compiler/generators/client-generator.js"

const run = promisify(execFile)
const sdk = fileURLToPath(new URL("../../../", import.meta.url))

test("the README ChatHistory contract compiles with the AI SDK", async t => {
    const project = await mkdtemp(path.resolve(sdk, "../.durable-actors-example-"))
    t.after(() => rm(project, { recursive: true, force: true }))
    await symlink(path.resolve(sdk, "../examples/ai-chat/node_modules"), path.join(project, "node_modules"))
    await writeFile(path.join(project, "package.json"), '{"type":"module"}')
    const readme = await readFile(path.resolve(sdk, "../README.md"), "utf8")
    const source = readme.split("## Define an Actor")[1].match(/```ts\n([\s\S]*?)```/)![1]
    const entrypoint = path.join(project, "actors.ts")
    await writeFile(entrypoint, source)
    const contract = new ActorCompiler().compileContract(entrypoint)
    const [actor] = contract.actors
    assert.equal(actor.actorName, "ChatHistory")
    assert.deepEqual(
        actor.rpc.methods.map(method => method.name),
        ["append", "load"]
    )
    await generateClient(contract, path.join(project, "generated"))
    const consumer = path.join(project, "backend.ts")
    await writeFile(consumer, readme.split("## Stream from the backend (Express)")[1].match(/```ts\n([\s\S]*?)```/)![1])
    const program = ts.createProgram([consumer], {
        strict: true,
        noEmit: true,
        skipLibCheck: false,
        target: ts.ScriptTarget.ES2022,
        module: ts.ModuleKind.NodeNext
    })
    assert.deepEqual(
        ts
            .getPreEmitDiagnostics(program)
            .map(diagnostic => ts.flattenDiagnosticMessageText(diagnostic.messageText, "\n")),
        []
    )
})

for (const template of ["chat", "ai-chat", "documents"]) {
    test(
        `the ${template} template builds its app and actor contract from a fresh init`,
        { timeout: 60_000 },
        async t => {
            // Match the examples' directory depth so pnpm's relative executable paths remain valid.
            const directory = await mkdtemp(path.resolve(sdk, "../.durable-actors-example-"))
            t.after(() => rm(directory, { recursive: true, force: true }))
            const project = path.join(directory, template)
            await run(process.execPath, [path.join(sdk, "dist/cli.js"), "init", template, "--template", template], {
                cwd: directory
            })
            await copyFile(path.join(project, ".env.example"), path.join(project, ".env"))
            await symlink(
                path.resolve(sdk, "../examples", template, "node_modules"),
                path.join(project, "node_modules")
            )
            await run("npm", ["run", "build"], { cwd: project })
            await run(process.execPath, [path.join(sdk, "dist/cli.js"), "generate", "src/actors.ts"], { cwd: project })
        }
    )
}
