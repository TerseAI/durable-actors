import assert from "node:assert/strict"
import { execFile, spawn } from "node:child_process"
import { once } from "node:events"
import { cp, mkdir, mkdtemp, readFile, rm, symlink, writeFile } from "node:fs/promises"
import { createServer } from "node:net"
import os from "node:os"
import path from "node:path"
import { createInterface } from "node:readline"
import { test } from "node:test"
import { fileURLToPath } from "node:url"
import { promisify } from "node:util"

import { buildActor } from "../../dist/compiler/actor-build.js"

const sdk = fileURLToPath(new URL("../../", import.meta.url))
const run = promisify(execFile)

test("deployment builds produce code and a contract without executing customer code", async t => {
    const root = await project(t)
    await writeFile(
        path.join(root, "src/actors.ts"),
        'import { Actor } from "durable-actors"; export class Counter extends Actor { async get(): Promise<number> { return 42 } }; throw new Error("customer code executed during build")'
    )
    const output = path.join(root, "published")
    const { stdout } = await run("bun", [path.join(sdk, "dist/compiler/deployment-build.js"), root, "src/actors.ts", output])
    const contract = JSON.parse(stdout)
    assert.equal(contract.actors[0].actorName, "Counter")
    assert.equal(contract.actors[0].rpc.methods[0].name, "get")
    assert.match(await readFile(path.join(output, "actors.mjs"), "utf8"), /Counter/)
    await writeFile(path.join(root, "src/actors.ts"), 'import { Actor } from "durable-actors"; export class Counter extends Actor { async get(): Promise<Date> { return new Date() } }')
    await assert.rejects(run("bun", [path.join(sdk, "dist/compiler/deployment-build.js"), root, "src/actors.ts", output]), /JSON-compatible/)
})

test("built actors run without source, compiler, or TypeScript loader", { timeout: 30_000 }, async t => {
    const root = await project(t)
    await writeFile(path.join(root, "src/increment.ts"), "export const increment = (amount: number) => amount + 1\n")
    await writeFile(
        path.join(root, "src/actors.ts"),
        `import { isMainThread } from "node:worker_threads"
        import { Actor, Persisted, Ephemeral, Emittable } from "durable-actors"
        import { increment } from "./increment.js"
        if (isMainThread) throw new Error("actor code must run only inside a Worker")
        export class BuiltCounter extends Actor {
            @Persisted @Emittable count = 0
            @Ephemeral calls = 0
            async add(amount: number) {
                this.count += increment(amount)
                return { count: this.count, calls: ++this.calls }
            }
            async invalidState() { this.count = "invalid" as never }
        }`
    )
    await buildActor(path.join(root, "src/actors.ts"), path.join(root, "dist/actors.mjs"))
    assert.match(await readFile(path.join(root, "dist/actors.mjs"), "utf8"), /BuiltCounter/)

    const deployed = path.join(root, "deployed")
    await mkdir(deployed)
    await cp(path.join(root, "dist"), path.join(deployed, "dist"), { recursive: true })
    await rm(path.join(root, "src"), { recursive: true })
    await rm(path.join(root, "tsconfig.json"))
    await rm(path.join(root, "dist"), { recursive: true })
    const socketPath = path.join(root, "host.sock")
    const bootstrap = path.join(root, "host.mjs")
    await writeFile(bootstrap, `import { runActorHost } from ${JSON.stringify(new URL("../../dist/host.js", import.meta.url).href)}; await runActorHost()`)
    const server = createServer()
    t.after(() => server.close())
    const connected = once(server, "connection")
    server.listen(socketPath)
    await once(server, "listening")
    const environment = { ...process.env, DURABLE_ACTORS_EXECUTOR_SOCKET: socketPath }
    delete environment.DURABLE_ACTORS_ENTRYPOINT
    const host = spawn("bun", [bootstrap], {
        cwd: deployed,
        env: environment,
        stdio: ["ignore", "pipe", "pipe"]
    })
    let stderr = ""
    host.stderr.on("data", data => (stderr += data))
    const exited = once(host, "exit")
    t.after(async () => {
        host.kill()
        await exited
    })
    const [socket] = await Promise.race([
        connected,
        exited.then(() => {
            throw new Error(`actor host exited before connecting: ${stderr}`)
        })
    ])
    t.after(() => socket.destroy())
    const lines = createInterface({ input: socket })[Symbol.asyncIterator]()
    const receive = async () => JSON.parse((await lines.next()).value)
    assert.deepEqual(await receive(), { type: "attach", protocol: 17, actor_names: ["BuiltCounter"] })
    const send = message => socket.write(JSON.stringify(message) + "\n")
    send({ type: "attached", protocol: 17 })
    const actor = { project_id: "default", actor_name: "BuiltCounter", actor_id: "counter" }
    const invoke = (messageId, state) =>
        send({
            type: "command",
            message_id: messageId,
            command: { type: "invoke", request_id: `request-${messageId}`, actor, method: "add", args: [2], state }
        })
    invoke(1, null)
    assert.deepEqual(await receive(), {
        type: "reply",
        message_id: 1,
        reply: {
            type: "invoked",
            result: { count: 3, calls: 1 },
            state: { count: 3 },
            effects: [{ type: "state_update", changes: { count: 3 }, removed: [] }]
        }
    })
    send({ type: "command", message_id: 2, command: { type: "evict", actor } })
    assert.equal((await receive()).reply.type, "evicted")
    invoke(3, { count: 3 })
    assert.deepEqual(await receive(), {
        type: "reply",
        message_id: 3,
        reply: {
            type: "invoked",
            result: { count: 6, calls: 1 },
            state: { count: 6 },
            effects: [{ type: "state_update", changes: { count: 6 }, removed: [] }]
        }
    })
    send({
        type: "command",
        message_id: 4,
        command: {
            type: "invoke",
            request_id: "invalid-state",
            actor,
            method: "invalidState",
            args: [],
            state: { count: 6 }
        }
    })
    const unchecked = (await receive()).reply
    assert.equal(unchecked.type, "invoked")
    assert.deepEqual(unchecked.state, { count: "invalid" })
})

test("actor builds report invalid persistence annotations before deployment", async t => {
    const root = await project(t)
    await writeFile(path.join(root, "src/actors.ts"), `import { Actor } from "durable-actors"; export class Counter extends Actor { count = 0 }`)
    await assert.rejects(buildActor(path.join(root, "src/actors.ts"), path.join(root, "dist/actors.mjs")), /must declare exactly one of @Persisted or @Ephemeral/)
})

async function project(t) {
    const root = await mkdtemp(path.join(os.tmpdir(), "actor-build-"))
    t.after(() => rm(root, { recursive: true, force: true }))
    await mkdir(path.join(root, "src"))
    await mkdir(path.join(root, "node_modules"))
    await symlink(sdk, path.join(root, "node_modules/durable-actors"), "dir")
    await writeFile(path.join(root, "package.json"), JSON.stringify({ type: "module" }))
    await writeFile(
        path.join(root, "tsconfig.json"),
        JSON.stringify({
            compilerOptions: {
                target: "ES2022",
                module: "NodeNext",
                strict: true,
                skipLibCheck: true,
                typeRoots: [path.join(sdk, "node_modules/@types")]
            }
        })
    )
    return root
}
