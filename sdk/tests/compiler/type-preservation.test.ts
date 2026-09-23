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
    const validate = new Ajv({ strict: false }).compile({ ...rpc.schema, ...rpc.methods[0].parameters[0].type })
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

test("unknown stays unconstrained in method, property, dictionary and socket types", async t => {
    const project = await createProject(t)
    await project.generate(`
        type Message = { metadata?: unknown; values: Record<string, unknown> }
        export class Room extends Actor<unknown, Message, unknown> {
            @Persisted messages: Message[] = []
            async echo(value: unknown): Promise<unknown> { return value }
            async message(value: Message): Promise<Message> { return value }
        }
    `)
    await project.check(`
        declare const value: unknown
        await room.echo(value)
        await room.message({ metadata: value, values: { anything: value } })
        const metadata: actors.Room.Metadata = value
        const outgoing: actors.Room.Outgoing = value
        const incoming: actors.Room.Incoming = { metadata: value, values: { anything: value } }
        const state: actors.Room.State = { messages: [incoming] }
        // @ts-expect-error unknown results require narrowing
        const object: Record<string, unknown> = await room.echo(value)
        // @ts-expect-error unknown properties require narrowing
        const text: string = (await room.message(incoming)).metadata
    `)
})

test("template literal types survive unions, tuples, dictionaries and JSON transport", async t => {
    const project = await createProject(t)
    await project.generate(`
        type Kind = \`\${string}.\${string}\`
        type Part = { kind: "text"; text: string } | { kind: Kind; data: unknown }
        type Combined = ({ kind: Kind } & { metadata: unknown }) | ({ kind: "text" } & { text: string })
        export class Room extends Actor<{}, Part, Part> {
            @Persisted parts: Part[] = []
            async echo(value: Kind): Promise<Kind> { return value }
            async part(value: Part): Promise<Part> { return value }
            async tuple(value: [Kind, \`item-\${number}\`]) { return value }
            async dictionary(value: Record<string, Kind>) { return value }
            async intersect(value: { kind: Kind } & { metadata: Record<string, unknown> }) { return value }
            async combined(value: Combined): Promise<Combined> { return value }
        }
    `)
    await project.check(`
        type Kind = \`\${string}.\${string}\`
        type Part = { kind: "text"; text: string } | { kind: Kind; data: unknown }
        type Combined = ({ kind: Kind } & { metadata: unknown }) | ({ kind: "text" } & { text: string })
        declare const part: Part
        const result: Kind = await room.echo("custom.part")
        const returned: Part = await room.part(part)
        const tuple: [Kind, \`item-\${number}\`] = await room.tuple(["custom.part", "item-2"])
        const dictionary: Record<string, Kind> = await room.dictionary({ kind: "custom.part" })
        const intersection: { kind: Kind; metadata: Record<string, unknown> } = await room.intersect({ kind: "custom.part", metadata: {} })
        const combined: Combined = await room.combined({ kind: "custom.part", metadata: null })
        const incoming: actors.Room.Incoming = part
        declare const outgoing: actors.Room.Outgoing
        const sent: Part = outgoing
        const state: actors.Room.State = { parts: [part] }
        const stored: Part[] = state.parts
        // @ts-expect-error arbitrary strings do not satisfy the template
        await room.echo("invalid")
        // @ts-expect-error union branches retain template constraints
        await room.part({ kind: "invalid", data: null })
    `)
})

test("recursive JSON dictionaries retain undefined without widening other uses", async t => {
    const project = await createProject(t)
    await project.generate(`
        type Json = null | boolean | number | string | Json[] | JsonObject
        interface JsonObject { [key: string]: Json | undefined }
        type Payload = { kind: "json"; value: JsonObject } | { kind: "text"; value: string }
        export class Room extends Actor<{}, Payload, Payload> {
            async echo(value: JsonObject): Promise<JsonObject> { return value }
            async json(value: Json): Promise<Json> { return value }
            async payload(value: Payload): Promise<Payload> { return value }
        }
    `)
    await project.check(`
        type Json = null | boolean | number | string | Json[] | JsonObject
        interface JsonObject { [key: string]: Json | undefined }
        declare const value: JsonObject
        const result: JsonObject = await room.echo(value)
        await room.echo({ missing: undefined, nested: { missing: undefined }, items: [{ missing: undefined }] })
        const json: Json = await room.json(value)
        await room.payload({ kind: "json", value })
        const incoming: actors.Room.Incoming = { kind: "json", value }
        // @ts-expect-error undefined is not a top-level JSON value
        await room.json(undefined)
        // @ts-expect-error undefined is not an array element
        await room.json([undefined])
        // @ts-expect-error dictionary lookups can be undefined
        const present: Json = (await room.echo(value)).missing
    `)
})

test("generated chat methods compose with AI SDK UIMessage and convertToModelMessages without casts", async t => {
    const project = await createProject(t)
    await project.generate(`
        import type { UIMessage } from "ai"
        export class Room extends Actor {
            @Persisted private messages: UIMessage[] = []
            async append(message: UIMessage) { this.messages.push(message) }
            async load(): Promise<UIMessage[]> { return this.messages }
        }
    `)
    await project.check(`
        import { convertToModelMessages, type UIMessage } from "ai"
        declare const message: UIMessage
        const chat = room
        await chat.append(message)
        const messages: UIMessage[] = await chat.load()
        await convertToModelMessages(messages)
        await convertToModelMessages(await chat.load())
    `)
})

async function createProject(t: { after(fn: () => Promise<void>): void }) {
    const root = await mkdtemp(path.join(os.tmpdir(), "type-preservation-"))
    t.after(() => rm(root, { recursive: true, force: true }))
    await mkdir(path.join(root, "node_modules"))
    const location = fileURLToPath(new URL("../../", import.meta.url))
    const sdk = location.endsWith(`${path.sep}.test-dist${path.sep}`) ? path.dirname(location.slice(0, -1)) : location
    await symlink(sdk, path.join(root, "node_modules/durable-actors"))
    await symlink(path.join(sdk, "node_modules/ai"), path.join(root, "node_modules/ai"))
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
