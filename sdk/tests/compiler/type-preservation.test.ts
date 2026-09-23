import { Ajv } from "ajv"
import assert from "node:assert/strict"
import { mkdir, mkdtemp, rm, symlink, writeFile } from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { test } from "node:test"
import { fileURLToPath } from "node:url"
import ts from "typescript"

import { ActorCompiler } from "../../src/compiler/actor-compiler.js"
import { generateClient } from "../../src/compiler/generators/client-generator.js"
import { parsePublicContract } from "../../src/compiler/validate-public-contract.js"

test("mapped dictionaries preserve their value schemas and generated index signatures", async t => {
    const project = await createProject(t)
    await project.generate(`
        type Metadata = Record<string, { value: string }>
        export class Room extends Actor<{}, never, never> {
            async echo(value: Metadata): Promise<Metadata> { return value }
        }
    `)
    const { rpc } = project.contract().actors[0]
    const validate = new Ajv().compile({ ...rpc.schema, ...rpc.methods[0].parameters[0].type })
    assert.equal(validate({ provider: { value: "ok" } }), true)
    assert.equal(validate({ provider: 1 }), false)
    assert.equal(validate({ provider: { value: 1 } }), false)
    await project.check(`
        type Metadata = Record<string, { value: string }>
        declare const metadata: Metadata
        const result: Metadata = await room.echo(metadata)
        result.provider.value.toUpperCase()
        // @ts-expect-error dictionary values retain their structure
        await room.echo({ provider: 1 })
    `)
})

test("nested dictionaries preserve union branches and recursive values", async t => {
    const project = await createProject(t)
    await project.generate(`
        type Node = { value: string; children: Record<string, Node> }
        type Part = { kind: "tree"; nodes: Record<string, Node> } | { kind: "text"; text: string }
        export class Room extends Actor<{}, Part, Part> {
            async echo(value: Part[]): Promise<Part[]> { return value }
        }
    `)
    await project.check(`
        type Node = { value: string; children: Record<string, Node> }
        type Part = { kind: "tree"; nodes: Record<string, Node> } | { kind: "text"; text: string }
        declare const parts: Part[]
        const result: Part[] = await room.echo(parts)
        const incoming: actors.Room.Incoming = parts[0]
        declare const outgoing: actors.Room.Outgoing
        const sent: Part = outgoing
        // @ts-expect-error recursive dictionary entries retain their value type
        await room.echo([{ kind: "tree", nodes: { root: { value: "ok", children: { leaf: 42 } } } }])
        // @ts-expect-error union branches retain their required properties
        await room.echo([{ kind: "text", nodes: {} }])
    `)
})

test("intersections preserve dictionary values and required properties", async t => {
    const project = await createProject(t)
    await project.generate(`
        type Message = { id: string } & { metadata: Record<string, { requestId: string }> }
        export class Room extends Actor<{}, Message, Message> {
            async echo(value: Message): Promise<Message> { return value }
        }
    `)
    await project.check(`
        type Message = { id: string } & { metadata: Record<string, { requestId: string }> }
        declare const message: Message
        const result: Message = await room.echo(message)
        const incoming: actors.Room.Incoming = message
        declare const outgoing: actors.Room.Outgoing
        const sent: Message = outgoing
        // @ts-expect-error both sides of the intersection are required
        await room.echo({ metadata: {} })
        // @ts-expect-error the dictionary retains its value type
        await room.echo({ id: "one", metadata: { provider: { requestId: 42 } } })
    `)
})

async function createProject(t: { after(fn: () => Promise<void>): void }) {
    const root = await mkdtemp(path.join(os.tmpdir(), "type-preservation-"))
    t.after(() => rm(root, { recursive: true, force: true }))
    await mkdir(path.join(root, "node_modules"))
    const location = fileURLToPath(new URL("../../", import.meta.url))
    const sdk = location.endsWith(`${path.sep}.test-dist${path.sep}`) ? path.dirname(location.slice(0, -1)) : location
    await symlink(sdk, path.join(root, "node_modules/durable-actors"))
    await writeFile(path.join(root, "package.json"), JSON.stringify({ type: "module" }))
    let contract: ReturnType<ActorCompiler["compileContract"]>
    return {
        contract: () => contract,
        async generate(source: string) {
            const entrypoint = path.join(root, "actors.ts")
            await writeFile(entrypoint, `import { Actor, Persisted } from "durable-actors"\n${source}`)
            contract = parsePublicContract(JSON.parse(JSON.stringify(new ActorCompiler().compileContract(entrypoint))))
            await rm(entrypoint)
            await generateClient(contract, path.join(root, "generated"))
        },
        async check(source: string) {
            const consumer = path.join(root, "consumer.ts")
            await writeFile(
                consumer,
                `import { actors } from "./generated/index.js"\nconst room = actors.Room.get("one")\n${source}`
            )
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
        }
    }
}
