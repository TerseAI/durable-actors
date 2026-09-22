import { Ajv } from "ajv"
import assert from "node:assert/strict"
import { mkdir, mkdtemp, rm, symlink, writeFile } from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { test } from "node:test"
import { fileURLToPath } from "node:url"

import { ActorCompiler } from "../../src/compiler/actor-compiler.js"

test("public contracts survive JSON transport without actor source or dependency imports", async t => {
    const project = await createProject(t)
    await mkdir(path.join(project.root, "node_modules/private-data"))
    await writeFile(
        path.join(project.root, "node_modules/private-data/package.json"),
        JSON.stringify({ name: "private-data", type: "module", types: "index.d.ts", main: "index.js" })
    )
    await writeFile(
        path.join(project.root, "node_modules/private-data/index.d.ts"),
        `export interface Message { id: string; text: string; reply?: Message; status: "sent" | "pending" }`
    )
    await writeFile(path.join(project.root, "node_modules/private-data/index.js"), 'throw new Error("never load")')
    await project.write(`
        import type { Message } from "private-data"
        export class Room extends Actor<{ userId: string }, { text: string }, Message> {
            @Persisted @Emittable messages: Message[] = []
            @Persisted private secret = "hidden"
            async send(message: Message): Promise<Message> { return message }
            async clear(): Promise<void> {}
            private async internal(): Promise<Date> { return new Date() }
            protected async helper() { return 1n }
            async #hidden() {}
            static async utility() {}
            async onConnect() {}
            async onMessage() {}
            async onDisconnect() {}
        }
        throw new Error("generation must not execute actor code")
    `)
    const contract = new ActorCompiler().compileContract(project.entrypoint)
    const serialized = JSON.stringify(contract)
    assert.deepEqual(JSON.parse(serialized), contract)
    assert.equal(contract.version, 1)
    const [actor] = contract.actors
    assert.equal(actor.actorName, "Room")
    assert.deepEqual(
        actor.rpc.methods.map(method => method.name),
        ["clear", "send"]
    )
    assert.deepEqual(actor.rpc.methods[0].result, { kind: "void" })
    assert.deepEqual(actor.socket.emittable, ["messages"])
    for (const absent of [project.root, "private-data", "secret", "hidden", "internal", "onConnect"])
        assert.equal(serialized.includes(absent), false, absent)
    await rm(project.root, { recursive: true, force: true })
    const fetched = JSON.parse(serialized)
    const method = fetched.actors[0].rpc.methods.find((method: { name: string }) => method.name === "send")
    const validate = new Ajv().compile({ ...fetched.actors[0].rpc.schema, ...method.parameters[0].type })
    assert.equal(
        validate({ id: "1", text: "hello", status: "sent", reply: { id: "0", text: "hi", status: "pending" } }),
        true
    )
    assert.equal(validate({ id: "1", text: 42, status: "sent" }), false)
    assert.equal(validate({ id: "1", text: "hello", status: "wrong" }), false)
    const validateResult = new Ajv().compile({ ...fetched.actors[0].rpc.schema, ...method.result.type })
    assert.equal(validateResult({ id: "1", text: "hello", status: "sent" }), true)
    assert.equal(validateResult({ id: 1, text: "hello", status: "sent" }), false)
})

test("captures optional, default, rest and nullable parameters and inferred promise results", async t => {
    const project = await createProject(t)
    await project.write(`
        export class Counter extends Actor<{}, never, never> {
            async add(amount: number, label?: string | null, scale = 1, ...tags: string[]) { return amount * scale }
            async unpack({ value }: { value: number }) { return value }
        }
    `)
    const [actor] = new ActorCompiler().compileContract(project.entrypoint).actors
    const [add, unpack] = actor.rpc.methods
    assert.deepEqual(
        add.parameters.map(({ name, optional, rest }) => ({ name, optional, rest })),
        [
            { name: "amount", optional: false, rest: false },
            { name: "label", optional: true, rest: false },
            { name: "scale", optional: true, rest: false },
            { name: "tags", optional: false, rest: true }
        ]
    )
    assert.equal(unpack.parameters[0].name, "arg0")
    assert.equal(add.result.kind, "value")
    if (add.result.kind !== "value") assert.fail("expected result schema")
    const ajv = new Ajv()
    const result = ajv.compile({ ...actor.rpc.schema, ...add.result.type })
    assert.equal(result(42), true)
    assert.equal(result("42"), false)
    const label = ajv.compile({ ...actor.rpc.schema, ...add.parameters[1].type })
    assert.equal(label(null), true)
    assert.equal(label("label"), true)
    const tags = ajv.compile({ ...actor.rpc.schema, ...add.parameters[3].type })
    assert.equal(tags(["one", "two"]), true)
    assert.equal(tags([1]), false)
})

test("extracts named re-exports and orders actors and methods deterministically", async t => {
    const project = await createProject(t)
    await project.write(`
        export class Zebra extends Actor<{}, never, never> { async z() {} async a() {} }
        export class Alpha extends Actor<{}, never, never> {}
    `)
    const entrypoint = path.join(project.root, "index.ts")
    await writeFile(entrypoint, 'export { Zebra, Alpha } from "./actors.js"')
    const compiler = new ActorCompiler()
    const first = compiler.compileContract(entrypoint)
    await writeFile(entrypoint, 'export { Alpha, Zebra } from "./actors.js"')
    assert.deepEqual(compiler.compileContract(entrypoint), first)
    assert.deepEqual(
        first.actors.map(actor => actor.actorName),
        ["Alpha", "Zebra"]
    )
    assert.deepEqual(
        first.actors[1].rpc.methods.map(method => method.name),
        ["a", "z"]
    )
})

test("rejects RPC types that cannot preserve their meaning across JSON", async t => {
    const project = await createProject(t)
    for (const type of ["Date", "bigint", "any", "unknown", "() => void", "string | undefined", "{ nested: Date }"]) {
        await project.write(`export class Room extends Actor<{}, never, never> {
            async send(value: ${type}): Promise<void> {}
        }`)
        assert.throws(() => new ActorCompiler().compileContract(project.entrypoint), /Room\.send.*JSON/, type)
    }
    await project.write(`export class Room extends Actor<{}, never, never> {
        async send(): Promise<Date> { return new Date() }
    }`)
    assert.throws(() => new ActorCompiler().compileContract(project.entrypoint), /Room\.send.*JSON/)
})

test("contracts are independent of checkout paths and preserve distinct types with the same name", async t => {
    const contracts = []
    for (let index = 0; index < 2; index++) {
        const project = await createProject(t)
        await writeFile(path.join(project.root, "first.ts"), "export interface Item { value: string }")
        await writeFile(path.join(project.root, "second.ts"), "export interface Item { value: number }")
        await project.write(`
            import type { Item as First } from "./first.js"
            import type { Item as Second } from "./second.js"
            export class Room extends Actor<First, Second, never> {
                async first(value: First): Promise<First> { return value }
                async second(value: Second): Promise<Second> { return value }
            }
        `)
        contracts.push(new ActorCompiler().compileContract(project.entrypoint))
    }
    assert.deepEqual(contracts[0], contracts[1])
    const { rpc } = contracts[0].actors[0]
    const first = new Ajv().compile({ ...rpc.schema, ...rpc.methods[0].parameters[0].type })
    const second = new Ajv().compile({ ...rpc.schema, ...rpc.methods[1].parameters[0].type })
    assert.equal(first({ value: "one" }), true)
    assert.equal(first({ value: 1 }), false)
    assert.equal(second({ value: "one" }), false)
    assert.equal(second({ value: 1 }), true)
})

test("rejects unsupported callable shapes instead of publishing incomplete signatures", async t => {
    const project = await createProject(t)
    for (const [method, message] of [
        ["async send<T>(value: T): Promise<T> { return value }", /Room\.send.*generic/],
        [
            "async send(value: string): Promise<string>; async send(value: number): Promise<number>; async send(value: string | number) { return value }",
            /Room\.send.*overload/
        ],
        ["send() { return Promise.resolve(1) }", /Room\.send.*async/],
        ["async then() {}", /Room\.then.*reserved/],
        ["async connect() {}", /Room\.connect.*reserved/],
        ["get value() { return 1 }", /Room\.value.*accessor/],
        ["async send(this: Room) {}", /Room\.send.*this/],
        ["async send(...values: [string, number?]) {}", /Room\.send.*rest/],
        ["async send(value = 1, required: string) {}", /Room\.send.*default/]
    ] as const) {
        await project.write(`export class Room extends Actor<{}, never, never> { ${method} }`)
        assert.throws(() => new ActorCompiler().compileContract(project.entrypoint), message, method)
    }
})

async function createProject(t: { after(fn: () => Promise<void>): void }) {
    const root = await mkdtemp(path.join(os.tmpdir(), "public-contract-"))
    t.after(() => rm(root, { recursive: true, force: true }))
    await mkdir(path.join(root, "node_modules"))
    const location = fileURLToPath(new URL("../../", import.meta.url))
    const sdk = location.endsWith(`${path.sep}.test-dist${path.sep}`) ? path.dirname(location.slice(0, -1)) : location
    await symlink(sdk, path.join(root, "node_modules/durable-actors"))
    await writeFile(path.join(root, "package.json"), JSON.stringify({ type: "module" }))
    const entrypoint = path.join(root, "actors.ts")
    return {
        root,
        entrypoint,
        write: (source: string) =>
            writeFile(entrypoint, `import { Actor, Persisted, Emittable } from "durable-actors"\n${source}`)
    }
}
