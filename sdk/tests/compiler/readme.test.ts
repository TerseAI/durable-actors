import assert from "node:assert/strict"
import { mkdtemp, readFile, rm, symlink, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import { test } from "node:test"
import { fileURLToPath } from "node:url"
import ts from "typescript"

import { ActorCompiler } from "../../src/compiler/actor-compiler.js"
import { generateClient } from "../../src/compiler/generators/client-generator.js"

test("README chat actor and generated-client backend compile together", async t => {
    const sdk = fileURLToPath(new URL("../../../", import.meta.url))
    const root = path.resolve(sdk, "..")
    const project = await mkdtemp(path.join(tmpdir(), "readme-chat-"))
    t.after(() => rm(project, { recursive: true, force: true }))
    await symlink(path.join(root, "examples/ai-chat/node_modules"), path.join(project, "node_modules"), "dir")
    await writeFile(path.join(project, "package.json"), '{"type":"module"}')
    const options = {
        target: ts.ScriptTarget.ES2022,
        module: ts.ModuleKind.NodeNext,
        strict: true,
        skipLibCheck: true,
        noEmit: true
    }
    for (const file of [path.join(root, "README.md"), path.join(sdk, "README.md")]) {
        const readme = await readFile(file, "utf8")
        const blocks = [...readme.matchAll(/```ts\n([\s\S]*?)```/g)].map(match => match[1])
        const actor = blocks.find(block => block.includes("export class ChatHistory"))
        const backend = blocks.find(block => block.includes('app.post("/api/chat"'))
        assert.ok(actor)
        assert.ok(backend)
        const entrypoint = path.join(project, "actors.ts")
        await writeFile(entrypoint, actor)
        await writeFile(path.join(project, "backend.ts"), backend)
        const contract = new ActorCompiler().compileContract(entrypoint)
        assert.equal(contract.actors[0].actorName, "ChatHistory")
        await generateClient(contract, path.join(project, "generated"))
        const program = ts.createProgram([entrypoint, path.join(project, "backend.ts")], options)
        const diagnostics = ts.getPreEmitDiagnostics(program)
        assert.equal(
            diagnostics.length,
            0,
            ts.formatDiagnosticsWithColorAndContext(diagnostics, {
                getCurrentDirectory: () => project,
                getCanonicalFileName: name => name,
                getNewLine: () => "\n"
            })
        )
    }
})
