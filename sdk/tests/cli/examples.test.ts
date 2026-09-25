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

test("UIMessage round-trips through the public contract and generated client without casts", async t => {
    const project = await mkdtemp(path.resolve(sdk, "../.durable-actors-uimessage-"))
    t.after(() => rm(project, { recursive: true, force: true }))
    await symlink(path.resolve(sdk, "../examples/ai-chat/node_modules"), path.join(project, "node_modules"))
    await writeFile(path.join(project, "package.json"), '{"type":"module"}')
    const entrypoint = path.join(project, "actors.ts")
    await writeFile(
        entrypoint,
        `
        import { Actor, Persisted } from "durable-actors"
        import type { UIMessage } from "ai"
        export class ChatHistory extends Actor<{}, never, never> {
            @Persisted private messages: UIMessage[] = []
            async append(message: UIMessage): Promise<void> { this.messages.push(message) }
            async load(): Promise<UIMessage[]> { return this.messages }
        }
    `
    )
    const contract = JSON.parse(JSON.stringify(new ActorCompiler().compileContract(entrypoint)))
    const ai = JSON.parse(await readFile(path.join(project, "node_modules/ai/package.json"), "utf8"))
    assert.deepEqual(contract.typescript.dependencies, { ai: ai.version })
    assert.match(contract.typescript.declarations, /import.*UIMessage.*from ['"]ai['"]/)
    await rm(entrypoint)
    await generateClient(contract, path.join(project, "generated"))
    const consumer = path.join(project, "consumer.ts")
    await writeFile(
        consumer,
        `
        import { convertToModelMessages, type UIMessage } from "ai"
        import { actors } from "./generated/index.js"
        declare const message: UIMessage
        const chat = actors.ChatHistory.get("lobby")
        await chat.append(message)
        const messages: UIMessage[] = await chat.load()
        await convertToModelMessages(messages)
        await convertToModelMessages(await chat.load())
    `
    )
    const program = ts.createProgram([consumer], {
        strict: true,
        noEmit: true,
        skipLibCheck: false,
        types: ["node"],
        typeRoots: [path.join(project, "node_modules/@types")],
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
