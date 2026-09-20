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

const sdk = fileURLToPath(new URL("../../", import.meta.url))
const run = promisify(execFile)

test("built actors run without source, compiler, or TypeScript loader", { timeout: 30_000 }, async t => {
    const root = await project(t)
    await writeFile(path.join(root, "src/increment.ts"), "export const increment = (amount: number) => amount + 1\n")
    await writeFile(
        path.join(root, "src/durable-objects.ts"),
        `import { isMainThread } from "node:worker_threads"
        import { Actor, Persisted, Ephemeral, Emittable } from "little-actors"
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
    await run(process.execPath, [path.join(sdk, "dist/cli.js"), "build"], { cwd: root })
    assert.match(await readFile(path.join(root, "dist/actors.mjs"), "utf8"), /BuiltCounter/)

    const deployed = path.join(root, "deployed")
    await mkdir(deployed)
    await cp(path.join(root, "dist"), path.join(deployed, "dist"), { recursive: true })
    await rm(path.join(root, "src"), { recursive: true })
    await rm(path.join(root, "tsconfig.json"))
    await rm(path.join(root, "dist"), { recursive: true })
    const guard = path.join(root, "guard.mjs")
    await writeFile(
        guard,
        `import { register } from "node:module"
        register(${JSON.stringify(
            "data:text/javascript," +
                encodeURIComponent(`export function resolve(specifier, context, nextResolve) {
                    if (['typescript', 'tsx', 'esbuild'].includes(specifier.split('/')[0]) || specifier.includes('/compiler/'))
                        throw new Error('build tooling loaded during actor startup: ' + specifier)
                    return nextResolve(specifier, context)
                }`)
        )}, import.meta.url)`
    )
    const socketPath = path.join(root, "host.sock")
    const bootstrap = path.join(root, "host.mjs")
    await writeFile(bootstrap, `import { runDurableObjectHost } from ${JSON.stringify(new URL("../../dist/host.js", import.meta.url).href)}; await runDurableObjectHost()`)
    const server = createServer()
    t.after(() => server.close())
    const connected = once(server, "connection")
    server.listen(socketPath)
    await once(server, "listening")
    const environment = { ...process.env, DURABLE_OBJECT_EXECUTOR_SOCKET: socketPath }
    delete environment.DURABLE_OBJECT_ENTRYPOINT
    const host = spawn(process.execPath, ["--import", guard, bootstrap], {
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
    assert.deepEqual(await receive(), { type: "attach", protocol: 16, actor_names: ["BuiltCounter"] })
    const send = message => socket.write(JSON.stringify(message) + "\n")
    send({ type: "attached", protocol: 16 })
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
    const failed = (await receive()).reply
    assert.equal(failed.type, "failed")
    assert.match(failed.message, /state violates its socket contract/)
    assert.equal("state" in failed, false)
})

test("actor builds report invalid persistence annotations before deployment", async t => {
    const root = await project(t)
    await writeFile(path.join(root, "src/durable-objects.ts"), `import { Actor } from "little-actors"; export class Counter extends Actor { count = 0 }`)
    await assert.rejects(run(process.execPath, [path.join(sdk, "dist/cli.js"), "build"], { cwd: root }), error => {
        assert.match(error.stderr, /must declare exactly one of @Persisted or @Ephemeral/)
        return true
    })
})

async function project(t) {
    const root = await mkdtemp(path.join(os.tmpdir(), "actor-build-"))
    t.after(() => rm(root, { recursive: true, force: true }))
    await mkdir(path.join(root, "src"))
    await mkdir(path.join(root, "node_modules"))
    await symlink(sdk, path.join(root, "node_modules/little-actors"), "dir")
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
