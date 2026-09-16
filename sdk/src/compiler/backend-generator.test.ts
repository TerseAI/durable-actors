import { build } from "esbuild"
import assert from "node:assert/strict"
import { mkdir, mkdtemp, readFile, rm, symlink, writeFile } from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { test } from "node:test"
import { fileURLToPath, pathToFileURL } from "node:url"
import ts from "typescript"

import { ActorCompiler } from "./actor-compiler.js"
import { generateTypeScript } from "./typescript-generator.js"

test("preserves named RPC types and their dependencies in clients generated from stored contracts", async t => {
    const author = await project(t)
    const entrypoint = path.join(author, "actors.ts")
    await writeFile(
        entrypoint,
        `
        import { Actor } from "little-actors"
        interface Author { name: string }
        interface Message { text: string; author: Author; reply?: Message }
        type SendMessageInput = { text: string }
        export class ChatRoom extends Actor<{}, never, never> {
            async sendMessage(input: SendMessageInput): Promise<Message> {
                return { text: input.text, author: { name: "Ada" } }
            }
            async latest(): Promise<Message> { return this.sendMessage({ text: "hi" }) }
        }
    `
    )
    const contract = JSON.parse(JSON.stringify(new ActorCompiler().compileContract(entrypoint)))
    await rm(author, { recursive: true, force: true })
    const files = await generateTypeScript(contract)
    const code = files.get("ChatRoom.backend.ts")!
    assert.match(code, /export interface SendMessageInput\b/)
    assert.match(code, /export interface Message\b/)
    assert.match(code, /export interface Author\b/)
    assert.match(code, /"sendMessage"\(input: SendMessageInput\): Promise<Message>/)
    assert.match(code, /"latest"\(\): Promise<Message>/)
    assert.match(code, /reply\?: Message/)
    const consumer = await project(t)
    for (const [file, content] of files) await writeFile(path.join(consumer, file), content)
    checkTypes(path.join(consumer, "backend.ts"))
})

test("names anonymous RPC types from methods and parameters in existing published contracts", async t => {
    const contract = JSON.parse(
        await readFile(new URL("../../../fixtures/public-contract.json", import.meta.url), "utf8")
    )
    const files = await generateTypeScript(contract)
    const code = files.get("ChatRoom.backend.ts")!
    assert.match(code, /export interface SendMessageInput\b/)
    assert.match(code, /export interface SendMessageResult\b/)
    assert.match(code, /"sendMessage"\(input: SendMessageInput\): Promise<SendMessageResult>/)
    const consumer = await project(t)
    for (const [file, content] of files) await writeFile(path.join(consumer, file), content)
    checkTypes(path.join(consumer, "backend.ts"))
})

test("disambiguates source type names without merging distinct RPC types or generated helpers", async t => {
    const root = await project(t)
    await writeFile(path.join(root, "first.ts"), "export interface Item { value: string }")
    await writeFile(path.join(root, "second.ts"), "export interface Item { value: number }")
    const entrypoint = path.join(root, "actors.ts")
    await writeFile(
        entrypoint,
        `
        import { Actor } from "little-actors"
        import type { Item as First } from "./first.js"
        import type { Item as Second } from "./second.js"
        type Stub = { stub: boolean }
        type RpcTypes = { rpc: boolean }
        export class Room extends Actor<{}, never, never> {
            async first(input: First): Promise<First> { return input }
            async second(input: Second): Promise<Second> { return input }
            async helpers(input: Stub): Promise<RpcTypes> { return { rpc: input.stub } }
        }
    `
    )
    const files = await generateTypeScript(JSON.parse(JSON.stringify(new ActorCompiler().compileContract(entrypoint))))
    const code = files.get("Room.backend.ts")!
    assert.match(code, /export interface Item\b/)
    assert.match(code, /"first"\(input: Item\): Promise<Item>/)
    for (const [file, content] of files) await writeFile(path.join(root, file), content)
    await writeFile(
        path.join(root, "consumer.ts"),
        `
        import { Room } from "./backend.js"
        const room = Room.get("one")
        const first: Promise<{ value: string }> = room.first({ value: "one" })
        const second: Promise<{ value: number }> = room.second({ value: 1 })
        const helpers: Promise<{ rpc: boolean }> = room.helpers({ stub: true })
        // @ts-expect-error distinct types with the same source name
        room.first({ value: 1 })
        // @ts-expect-error distinct types with the same source name
        room.second({ value: "one" })
    `
    )
    checkTypes(path.join(root, "consumer.ts"))
})

test("generates callable typed backend stubs in a consumer without actor source or private dependencies", async t => {
    const author = await project(t)
    await mkdir(path.join(author, "node_modules/private-data"))
    await writeFile(
        path.join(author, "node_modules/private-data/package.json"),
        JSON.stringify({ types: "index.d.ts" })
    )
    await writeFile(
        path.join(author, "node_modules/private-data/index.d.ts"),
        "export interface Message { text: string; reply?: Message }"
    )
    const entrypoint = path.join(author, "actors.ts")
    await writeFile(
        entrypoint,
        `
        import { Actor } from "little-actors"
        import type { Message } from "private-data"
        export class ChatRoom extends Actor<{}, never, never> {
            async sendMessage(input: Message): Promise<Message> { return input }
            async clear(): Promise<void> {}
            async count(scale = 1): Promise<number> { return scale }
            async tags(...tags: string[]): Promise<string[]> { return tags }
            async nullable(): Promise<null> { return null }
            private async secret() {}
        }
        throw new Error("must never execute")
    `
    )
    const contract = JSON.parse(JSON.stringify(new ActorCompiler().compileContract(entrypoint)))
    await rm(author, { recursive: true, force: true })
    const files = await generateTypeScript(contract)
    assert.ok(files.has("backend.ts"))
    const consumer = await project(t)
    for (const [file, content] of files) await writeFile(path.join(consumer, file), content)
    await writeFile(
        path.join(consumer, "consumer.ts"),
        `
        import { ChatRoom } from "./backend.js"
        const room = ChatRoom.get("room-123")
        const message: Promise<{ text: string; reply?: { text: string } }> = room.sendMessage({ text: "hi" })
        const cleared: Promise<void> = room.clear()
        const count: Promise<number> = room.count()
        const tags: Promise<string[]> = room.tags("one", "two")
        const nullable: Promise<null> = room.nullable()
        // @ts-expect-error wrong argument type
        room.sendMessage({ text: 123 })
        // @ts-expect-error required argument
        room.sendMessage()
        // @ts-expect-error private method
        room.secret()
        // @ts-expect-error recursive type
        room.sendMessage({ text: "hi", reply: { text: 123 } })
        // @ts-expect-error wrong result type
        const wrong: Promise<string> = room.count()
        // @ts-expect-error wrong rest type
        room.tags(123)
    `
    )
    checkTypes(path.join(consumer, "consumer.ts"))
    const outfile = path.join(consumer, "backend.mjs")
    const bundled = await build({
        entryPoints: [path.join(consumer, "backend.ts")],
        outfile,
        bundle: true,
        format: "esm",
        platform: "node",
        external: ["little-actors/backend"],
        metafile: true
    })
    assert.equal(
        Object.keys(bundled.metafile!.inputs).some(file => file.includes("private-data")),
        false
    )
    const { ChatRoom } = await import(pathToFileURL(outfile).href)
    const calls: unknown[][] = []
    const room = ChatRoom.get("room-123", {
        async invoke(...args: unknown[]) {
            calls.push(args)
            return args[2] === "sendMessage" ? { text: "hi" } : null
        }
    })
    assert.deepEqual(await room.sendMessage({ text: "hi" }), { text: "hi" })
    assert.equal(await room.clear(), undefined)
    assert.equal(await room.nullable(), null)
    assert.deepEqual(calls[0], ["ChatRoom", "room-123", "sendMessage", [{ text: "hi" }]])
    const browser = await build({
        entryPoints: [path.join(consumer, "frontend.ts")],
        bundle: true,
        platform: "browser",
        format: "esm",
        write: false,
        metafile: true
    })
    assert.equal(
        Object.keys(browser.metafile!.inputs).some(file =>
            /\/(?:backend\.js|@grpc\/|typescript\/|private-data\/)/.test(file)
        ),
        false
    )
})

test("generated modules handle actor/helper collisions, duplicate argument labels and never results", async t => {
    const root = await project(t)
    const entrypoint = path.join(root, "actors.ts")
    await writeFile(
        entrypoint,
        `
        import { Actor } from "little-actors"
        export class Stub extends Actor<{}, never, never> {
            async fail(): Promise<never> { throw new Error("failed") }
            async unpack({ value }: { value: number }, arg0: string) { return value }
            async __proto__() { return 1 }
        }
        export class RpcTypes extends Actor<{}, never, never> { async clear() {} }
        export class backend extends Actor<{}, never, never> {}
    `
    )
    const files = await generateTypeScript(new ActorCompiler().compileContract(entrypoint))
    for (const [file, content] of files) await writeFile(path.join(root, file), content)
    await writeFile(
        path.join(root, "consumer.ts"),
        `
        import { Stub, RpcTypes, backend } from "./backend.js"
        const never: Promise<never> = Stub.get("one").fail()
        const value: Promise<number> = Stub.get("one").unpack({ value: 1 }, "text")
        const proto: Promise<number> = Stub.get("one").__proto__()
        const cleared: Promise<void> = RpcTypes.get("one").clear()
        backend.get("one")
    `
    )
    checkTypes(path.join(root, "consumer.ts"))
})

test("public contracts reject unsupported versions and generate an empty backend module", async () => {
    const files = await generateTypeScript({ version: 1, actors: [] })
    assert.equal(files.get("backend.ts"), "export {}\n")
    await assert.rejects(generateTypeScript({ version: 2, actors: [] } as never), /version/)
})

async function project(t: { after(fn: () => Promise<void>): void }) {
    const root = await mkdtemp(path.join(os.tmpdir(), "backend-codegen-"))
    t.after(() => rm(root, { recursive: true, force: true }))
    await mkdir(path.join(root, "node_modules"))
    const location = fileURLToPath(new URL("../../", import.meta.url))
    const sdk = location.endsWith(`${path.sep}.test-dist${path.sep}`) ? path.dirname(location.slice(0, -1)) : location
    await symlink(sdk, path.join(root, "node_modules/little-actors"))
    await writeFile(path.join(root, "package.json"), JSON.stringify({ type: "module" }))
    return root
}

function checkTypes(entrypoint: string) {
    const program = ts.createProgram([entrypoint], {
        strict: true,
        noEmit: true,
        skipLibCheck: true,
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
