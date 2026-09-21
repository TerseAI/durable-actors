import assert from "node:assert/strict"
import { EventEmitter, once } from "node:events"
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { test } from "node:test"
import { fileURLToPath, pathToFileURL } from "node:url"

import type { SocketEffect } from "../../src/actor/socketProtocol.js"
import { prepareActorEntrypoint } from "../../src/host/actor-host.js"
import { ActorWorker, ActorWorkerSupervisor } from "../../src/host/worker-supervisor.js"

const actorIdentity = {
    project_id: "default",
    actor_name: "SessionCounter",
    actor_id: "counter-1"
}

test("correlates overlapping worker replies and socket lookups without idle eviction", { timeout: 10000 }, async () => {
    const root = await createTypeScriptConsumer("InterleavedWorker")
    const file = path.join(root, "src/durable-objects.ts")
    await writeFile(
        file,
        `import { Actor, Persisted, Reentrant } from ${JSON.stringify(fileURLToPath(new URL("../../src/index.js", import.meta.url)))}
        export class InterleavedWorker extends Actor {
            @Persisted count = 0
            @Reentrant async hold() {
                const sockets = await this.getConnections()
                this.broadcast("started:" + sockets[0]?.id)
                await new Promise(resolve => setTimeout(resolve, 150))
                this.broadcast("ended:" + sockets[0]?.id)
                return ++this.count
            }
            async increment() { this.broadcast("increment"); return ++this.count }
        }`
    )
    const entrypoint = pathToFileURL(file).href
    const runtime = new ActorWorkerSupervisor({
        actorEntrypointUrl: entrypoint,
        actorSchemas: await prepareActorEntrypoint(entrypoint),
        actorIdleTimeoutMs: 25
    })
    const events: string[] = []
    let started!: () => void
    const start = new Promise<void>(resolve => {
        started = resolve
    })
    try {
        const holding = runtime.handle(
            { ...invokeCommand("one", "InterleavedWorker"), method: "hold" },
            () => {},
            async effects => {
                for (const effect of effects) if (effect.type === "broadcast") events.push(effect.message.data)
                started()
            },
            async () => [{ id: "one", metadata: {}, tags: [] }]
        )
        await start
        const second = await runtime.handle(
            { ...invokeCommand("one", "InterleavedWorker"), request_id: "second" },
            () => {},
            async effects => {
                assert.deepEqual(
                    effects.map(effect => effect.type === "broadcast" && effect.message.data),
                    ['"increment"']
                )
            }
        )
        assert.equal("result" in second && second.result, 1)
        const first = await holding
        assert.equal("result" in first && first.result, 2)
        assert.deepEqual(events, ['"started:one"', '"ended:one"'])
        await new Promise(resolve => setTimeout(resolve, 60))
        assert.deepEqual(runtime.activeActors(), [])
        const resumed = await runtime.handle(
            { ...invokeCommand("one", "InterleavedWorker"), state: { count: 2 } },
            () => {},
            async () => {}
        )
        assert.ok(resumed.type === "invoked")
        assert.equal(resumed.result, 3)
        assert.equal(resumed.sequence, 3)
    } finally {
        runtime.close()
        await rm(root, { recursive: true, force: true })
    }
})

test("a generic Bun worker is warm before customer code is assigned", async () => {
    const worker = new ActorWorker()
    try {
        await worker.warm()
        const root = await createTypeScriptConsumer("WarmCounter")
        try {
            const moduleUrl = pathToFileURL(path.join(root, "src/durable-objects.ts")).href
            worker.load({ moduleUrl, schemas: await prepareActorEntrypoint(moduleUrl) }, () => {})
            assert.deepEqual(await worker.ready(), ["WarmCounter"])
            assert.throws(() => worker.load({ moduleUrl, schemas: [] }, () => {}), /already assigned/)
            const command = invokeCommand("one", "WarmCounter")
            assert.deepEqual(
                await worker.execute({ type: "hydrate", actor: command.actor, state: { count: 41 } }, () => {}),
                {
                    type: "hydrated"
                }
            )
            assert.deepEqual(await worker.execute({ ...command, resident_only: true, state: undefined }, () => {}), {
                type: "invoked",
                result: 42,
                state: { count: 42 }
            })
        } finally {
            await rm(root, { recursive: true, force: true })
        }
    } finally {
        worker.terminate("test finished")
    }
})

test("an executor never accepts a second actor identity, even after eviction", async () => {
    const root = await createTypeScriptConsumer("BoundCounter")
    const entrypoint = pathToFileURL(path.join(root, "src/durable-objects.ts")).href
    const runtime = new ActorWorkerSupervisor({
        actorEntrypointUrl: entrypoint,
        actorSchemas: await prepareActorEntrypoint(entrypoint)
    })
    try {
        const first = invokeCommand("first", "BoundCounter")
        assert.equal((await runtime.handle(first, () => {})).type, "invoked")
        await runtime.handle({ type: "evict", actor: first.actor }, () => {})
        const reply = await runtime.handle(invokeCommand("second", "BoundCounter"), () => {})
        assert.equal(reply.type, "failed")
        if (reply.type === "failed") assert.equal(reply.code, "actor_identity_mismatch")
    } finally {
        runtime.close()
        await rm(root, { recursive: true, force: true })
    }
})

test("keeps an actor resident until Rust explicitly evicts it", async () => {
    const consumerRoot = await createTypeScriptConsumer()
    const entrypoint = pathToFileURL(path.join(consumerRoot, "src/durable-objects.ts")).href
    try {
        await exerciseActiveActors(entrypoint)
        await exerciseIdleRecycling(entrypoint)
        await exerciseSocketHibernation(entrypoint)
    } finally {
        await rm(consumerRoot, { recursive: true, force: true })
    }
})

test("reports a new actor only after its Worker is ready", { timeout: 5_000 }, async () => {
    const root = await createTypeScriptConsumer()
    const entrypoint = pathToFileURL(path.join(root, "src/durable-objects.ts")).href
    const supervisor = new ActorWorkerSupervisor({
        actorEntrypointUrl: entrypoint,
        actorSchemas: await prepareActorEntrypoint(entrypoint)
    })
    try {
        const starting = supervisor.handle(invokeCommand("counter-1", "SessionCounter"), () => {})
        assert.deepEqual(supervisor.activeActors(), [])
        assert.equal((await starting).type, "invoked")
        assert.deepEqual(supervisor.activeActors(), [actorIdentity])
    } finally {
        supervisor.close()
        await rm(root, { recursive: true, force: true })
    }
})

for (const [reason, action] of [
    ["exit", "process.exit(0)"],
    ["uncaught error", 'throw new Error("worker crashed")']
]) {
    test(`publishes an idle Worker's ${reason} without another invocation`, { timeout: 10_000 }, async context => {
        const root = await createTypeScriptConsumer(
            "SessionCounter",
            `
import { watch } from "node:fs"
watch(new URL(".", import.meta.url), (_, name) => {
    if (name === "stop") { ${action} }
})`
        )
        const entrypoint = pathToFileURL(path.join(root, "src/durable-objects.ts")).href
        const supervisor = new ActorWorkerSupervisor({
            actorEntrypointUrl: entrypoint,
            actorSchemas: await prepareActorEntrypoint(entrypoint)
        })
        try {
            await supervisor.handle(invokeCommand("counter-1", "SessionCounter"), () => {})
            const changes = new EventEmitter()
            supervisor.onActiveActorsChange(() => changes.emit("change"))
            const stopped = once(changes, "change", { signal: context.signal })
            // Idle Workers are unreferenced, so the deadline also keeps the test alive.
            const deadline = setTimeout(() => changes.emit("error", new Error("Worker exit was not reported")), 5_000)
            context.after(() => clearTimeout(deadline))
            await writeFile(path.join(root, "src/stop"), "")
            await stopped
            assert.deepEqual(supervisor.activeActors(), [])
        } finally {
            supervisor.close()
            await rm(root, { recursive: true, force: true })
        }
    })
}

test("starts one speculative Worker and gives it to the first actor", async () => {
    const consumerRoot = await createTypeScriptConsumer("PreloadedCounter")
    const entrypoint = pathToFileURL(path.join(consumerRoot, "src/durable-objects.ts")).href
    const created: number[] = []
    try {
        const runtime = new ActorWorkerSupervisor({
            actorEntrypointUrl: entrypoint,
            actorSchemas: await prepareActorEntrypoint(entrypoint),
            createWorker: () => {
                created.push(created.length + 1)
                return {
                    state: "ready",

                    async ready() {
                        return ["PreloadedCounter"]
                    },
                    async execute() {
                        return { type: "invoked", result: null, state: {} }
                    },
                    terminate() {}
                }
            }
        })

        assert.equal(created.length, 1)
        await runtime.handle(invokeCommand("counter-1", "PreloadedCounter"), () => {})
        assert.equal(created.length, 1)
        const second = await runtime.handle(invokeCommand("counter-2", "PreloadedCounter"), () => {})
        assert.equal(second.type, "failed")
        assert.equal(created.length, 1)
        runtime.close()
    } finally {
        await rm(consumerRoot, { recursive: true, force: true })
    }
})

test("thrown methods and socket handlers roll back state without restarting the worker", async () => {
    const root = await createTypeScriptConsumer()
    const entrypoint = pathToFileURL(path.join(root, "src/durable-objects.ts")).href
    const supervisor = new ActorWorkerSupervisor({
        actorEntrypointUrl: entrypoint,
        actorSchemas: await prepareActorEntrypoint(entrypoint)
    })
    const seen: number[] = []
    supervisor.onActiveActorsChange(() => seen.push(supervisor.activeActors().length))
    const command = invokeCommand("counter-1", "SessionCounter")
    try {
        await supervisor.handle(command, () => {})
        const workerId = await supervisor.handle({ ...command, method: "workerId" }, () => {})
        for (const failure of [
            { ...command, method: "explode", resident_only: true, state: undefined },
            {
                type: "websocket_event" as const,
                request_id: "socket-failure",
                actor: actorIdentity,
                resident_only: true,
                event: {
                    type: "message" as const,
                    connection_id: "socket-1",
                    message: { type: "text" as const, data: JSON.stringify({ text: "fail" }) }
                },
                connections: [{ id: "socket-1", metadata: { userId: "user-1" }, tags: [] }]
            }
        ]) {
            assert.equal((await supervisor.handle(failure, () => {})).type, "failed")
            assert.deepEqual(supervisor.activeActors(), [actorIdentity])
            assert.deepEqual(seen, [1])
            assert.deepEqual(
                await supervisor.handle(
                    { ...command, method: "getCount", resident_only: true, state: undefined },
                    () => {}
                ),
                {
                    type: "invoked",
                    result: 1,
                    state: { count: 1 }
                }
            )
            assert.deepEqual(await supervisor.handle({ ...command, method: "workerId" }, () => {}), workerId)
        }
    } finally {
        supervisor.close()
        await rm(root, { recursive: true, force: true })
    }
})

test("expires an unused speculative Worker without replenishing it", async () => {
    let created = 0
    let terminated = 0
    let finish: (() => void) | undefined
    const expired = new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => reject(new Error("preload did not expire")), 1_000)
        finish = () => {
            clearTimeout(timeout)
            resolve()
        }
    })
    const supervisor = new ActorWorkerSupervisor({
        actorEntrypointUrl: "file:///unused.ts",
        actorSchemas: [],
        actorIdleTimeoutMs: 50,
        createWorker: () => {
            created += 1
            return {
                state: "starting",

                ready: () => new Promise(() => undefined),
                async execute() {
                    return { type: "invoked", result: null, state: {} }
                },
                terminate() {
                    terminated += 1
                    finish?.()
                }
            }
        }
    })
    await expired
    assert.equal(terminated, 1)
    await new Promise(resolve => setTimeout(resolve, 100))
    assert.equal(created, 1)
    supervisor.close()
})

test("eviction during Worker startup settles the invocation and allows recovery", { timeout: 5_000 }, async () => {
    const root = await createTypeScriptConsumer("CancelledCounter")
    const entrypoint = pathToFileURL(path.join(root, "src/durable-objects.ts")).href
    const command = invokeCommand("counter-1", "CancelledCounter")
    try {
        const runtime = new ActorWorkerSupervisor({
            actorEntrypointUrl: entrypoint,
            actorSchemas: await prepareActorEntrypoint(entrypoint)
        })
        await runtime.ready()
        const pending = runtime.handle(command, () => {})
        await runtime.handle({ type: "evict", actor: command.actor }, () => {})
        const reply = await pending
        assert.equal(reply.type, "failed")
        if (reply.type === "failed") assert.equal(reply.code, "actor_worker_terminated")
        assert.deepEqual(await runtime.handle({ ...command, state: { count: 9 } }, () => {}), {
            type: "invoked",
            result: 10,
            state: { count: 10 }
        })
        await runtime.handle({ type: "evict", actor: command.actor }, () => {})
    } finally {
        await rm(root, { recursive: true, force: true })
    }
})

test("discards a failed preload before accepting the first actor", async () => {
    const root = await createTypeScriptConsumer("RetryPreloadCounter")
    const entrypoint = pathToFileURL(path.join(root, "src/durable-objects.ts")).href
    let created = 0
    let terminated = 0
    try {
        const runtime = new ActorWorkerSupervisor({
            actorEntrypointUrl: entrypoint,
            actorSchemas: await prepareActorEntrypoint(entrypoint),
            createWorker: () => {
                const failed = created++ === 0
                return {
                    state: failed ? "stopped" : "ready",

                    ready: () =>
                        failed ? Promise.reject(new Error("preload failed")) : Promise.resolve(["RetryPreloadCounter"]),
                    async execute() {
                        if (failed) throw new Error("preload failed")
                        return { type: "invoked", result: 1, state: { count: 1 } }
                    },
                    terminate() {
                        terminated += 1
                    }
                }
            }
        })
        await new Promise(resolve => setImmediate(resolve))
        assert.equal(terminated, 1)
        assert.deepEqual(await runtime.handle(invokeCommand("counter-1", "RetryPreloadCounter"), () => {}), {
            type: "invoked",
            result: 1,
            state: { count: 1 }
        })
        assert.equal(created, 2)
        runtime.close()
    } finally {
        await rm(root, { recursive: true, force: true })
    }
})

test("closing the supervisor terminates an unused Worker and rejects new work", async () => {
    let terminated = 0
    const runtime = new ActorWorkerSupervisor({
        actorEntrypointUrl: "file:///unused.ts",
        actorSchemas: [],
        createWorker: () => ({
            state: "ready",

            async ready() {
                return ["UnusedCounter"]
            },
            async execute() {
                throw new Error("closed supervisor must not execute")
            },
            terminate() {
                terminated += 1
            }
        })
    })
    runtime.close()
    runtime.close()
    assert.equal(terminated, 1)
    const reply = await runtime.handle(invokeCommand("counter-1", "UnusedCounter"), () => {})
    assert.equal(reply.type, "failed")
    if (reply.type === "failed") assert.equal(reply.code, "actor_worker_terminated")
})

test("an actor module that fails inside a Worker returns a failure without hanging", { timeout: 5_000 }, async () => {
    const root = await createTypeScriptConsumer(
        "FailedImportCounter",
        'import { isMainThread } from "node:worker_threads"\nif (!isMainThread) throw new Error("worker import failed")'
    )
    const entrypoint = pathToFileURL(path.join(root, "src/durable-objects.ts")).href
    try {
        const runtime = new ActorWorkerSupervisor({
            actorEntrypointUrl: entrypoint,
            actorSchemas: await prepareActorEntrypoint(entrypoint)
        })
        try {
            const reply = await runtime.handle(invokeCommand("counter-1", "FailedImportCounter"), () => {})
            assert.equal(reply.type, "failed")
            if (reply.type === "failed") assert.match(reply.message, /worker import failed/)
        } finally {
            runtime.close()
        }
    } finally {
        await rm(root, { recursive: true, force: true })
    }
})

async function exerciseActiveActors(entrypoint: string): Promise<void> {
    const runtime = new ActorWorkerSupervisor({
        actorEntrypointUrl: entrypoint,
        actorSchemas: await prepareActorEntrypoint(entrypoint)
    })
    assert.deepEqual(await runtime.ready(), ["SessionCounter"])

    assert.deepEqual(
        await runtime.handle(
            {
                type: "invoke",
                request_id: "request-1",
                actor: actorIdentity,
                method: "increment",
                args: [2],
                state: null
            },
            () => {}
        ),
        { type: "invoked", result: 2, state: { count: 2 } }
    )
    assert.deepEqual(
        await runtime.handle(
            {
                type: "invoke",
                request_id: "request-2",
                actor: actorIdentity,
                method: "increment",
                args: [3],
                state: { count: 0 }
            },
            () => {}
        ),
        { type: "invoked", result: 5, state: { count: 5 } }
    )
    assert.deepEqual(await runtime.handle({ type: "evict", actor: actorIdentity }, () => {}), { type: "evicted" })
    assert.deepEqual(
        await runtime.handle(
            {
                type: "invoke",
                request_id: "request-3",
                actor: actorIdentity,
                method: "getCount",
                args: [],
                state: { count: 2 }
            },
            () => {}
        ),
        { type: "invoked", result: 2, state: { count: 2 } }
    )
}

async function exerciseSocketHibernation(entrypoint: string): Promise<void> {
    const runtime = new ActorWorkerSupervisor({
        actorEntrypointUrl: entrypoint,
        actorSchemas: await prepareActorEntrypoint(entrypoint),
        actorIdleTimeoutMs: 10
    })
    const connection = { id: "socket-1", metadata: { userId: "user-1" }, tags: [] }
    assert.deepEqual(
        await runtime.handle(
            {
                type: "websocket_event",
                request_id: "socket-request-1",
                actor: actorIdentity,
                event: { type: "connect", connection },
                connections: [connection],
                state: null
            },
            () => {}
        ),
        {
            type: "websocket_handled",
            state: { count: 1 },
            effects: []
        }
    )
    assert.deepEqual(runtime.activeActors(), [actorIdentity])
    await new Promise(resolve => setTimeout(resolve, 30))
    assert.deepEqual(runtime.activeActors(), [])
    const published: SocketEffect[] = []
    assert.deepEqual(
        await runtime.handle(
            {
                type: "websocket_event",
                request_id: "socket-request-2",
                actor: actorIdentity,
                event: {
                    type: "message",
                    connection_id: "socket-1",
                    message: { type: "text", data: JSON.stringify({ text: "hello" }) }
                },
                connections: [connection],
                state: { count: 1 }
            },
            () => {},
            async effects => {
                published.push(...effects)
            }
        ),
        {
            type: "websocket_handled",
            state: { count: 2 },
            effects: []
        }
    )
    assert.deepEqual(published, [
        {
            type: "send",
            connection_id: "socket-1",
            message: { type: "text", data: JSON.stringify({ text: "user-1:hello" }) }
        }
    ])
    runtime.close()
}

async function exerciseIdleRecycling(entrypoint: string): Promise<void> {
    const runtime = new ActorWorkerSupervisor({
        actorEntrypointUrl: entrypoint,
        actorSchemas: await prepareActorEntrypoint(entrypoint),
        actorIdleTimeoutMs: 10
    })
    assert.deepEqual(
        await runtime.handle(
            {
                type: "invoke",
                request_id: "idle-request-1",
                actor: actorIdentity,
                method: "increment",
                args: [2],
                state: null
            },
            () => {}
        ),
        { type: "invoked", result: 2, state: { count: 2 } }
    )
    await new Promise(resolve => setTimeout(resolve, 30))
    assert.deepEqual(
        await runtime.handle(
            {
                type: "invoke",
                request_id: "idle-request-2",
                actor: actorIdentity,
                method: "getCount",
                args: [],
                state: { count: 9 }
            },
            () => {}
        ),
        { type: "invoked", result: 9, state: { count: 9 } }
    )
}

async function createTypeScriptConsumer(actorName = "SessionCounter", preamble = ""): Promise<string> {
    const root = await mkdtemp(path.join(os.tmpdir(), "durable-object-worker-"))
    const source = path.join(root, "src")
    await mkdir(source)
    const compiledSdkRoot = fileURLToPath(new URL("../../src/", import.meta.url))
    await writeFile(path.join(root, "package.json"), JSON.stringify({ type: "module" }))
    await writeFile(
        path.join(source, "durable-objects.ts"),
        `import { Actor, Persisted, Ephemeral } from ${JSON.stringify(path.join(compiledSdkRoot, "index.js"))}
import { threadId } from "node:worker_threads"
${preamble}

export class ${actorName} extends Actor<{ userId: string }, { text: string }> {
    @Persisted count = 0
    @Ephemeral cache = new Map<string, number>()

    async increment(amount = 1): Promise<number> {
        this.count += amount
        return this.count
    }

    async getCount(): Promise<number> {
        return this.count
    }

    async workerId(): Promise<number> {
        return threadId
    }

    async explode(): Promise<void> {
        this.count = 999
        throw new Error("failed")
    }

    async onConnect(): Promise<void> {
        this.count += 1
    }

    async onMessage(socket: { metadata: { userId: string }, send(message: { text: string }): void }, message: { text: string }): Promise<void> {
        this.count += 1
        if (message.text === "fail") throw new Error("failed")
        socket.send({ text: \`${"${socket.metadata.userId}"}:${"${message.text}"}\` })
    }
}
`
    )
    return root
}

function invokeCommand(actorId: string, actorName: string) {
    return {
        type: "invoke" as const,
        request_id: `request-${actorId}`,
        actor: { ...actorIdentity, actor_name: actorName, actor_id: actorId },
        method: "increment",
        args: [],
        state: null
    }
}

test("residency reports actual workers and drops evicted and failed instances", async () => {
    let fail = false
    const supervisor = new ActorWorkerSupervisor({
        actorEntrypointUrl: "file:///unused.mjs",
        actorSchemas: undefined,
        createWorker: () => ({
            state: "ready",

            ready: async () => ["SessionCounter"],
            execute: async () =>
                fail
                    ? { type: "failed", code: "test", message: "failed" }
                    : { type: "invoked", result: null, state: {} },
            terminate() {}
        })
    })
    try {
        assert.deepEqual(supervisor.activeActors(), [])
        await supervisor.handle(invokeCommand("counter-1", "SessionCounter"), () => {})
        assert.deepEqual(supervisor.activeActors(), [actorIdentity])
        await supervisor.handle({ type: "evict", actor: actorIdentity }, () => {})
        assert.deepEqual(supervisor.activeActors(), [])
        fail = true
        await supervisor.handle(invokeCommand("counter-1", "SessionCounter"), () => {})
        assert.deepEqual(supervisor.activeActors(), [])
    } finally {
        supervisor.close()
    }
})

test("residency subscribers see worker creation and eviction immediately", async () => {
    const supervisor = new ActorWorkerSupervisor({
        actorEntrypointUrl: "file:///unused.mjs",
        actorSchemas: undefined,
        createWorker: () => ({
            state: "ready",

            ready: async () => ["SessionCounter"],
            execute: async () => ({ type: "invoked", result: null, state: {} }),
            terminate() {}
        })
    })
    const seen: number[] = []
    try {
        const unsubscribe = supervisor.onActiveActorsChange(() => seen.push(supervisor.activeActors().length))
        await supervisor.handle(invokeCommand("counter-1", "SessionCounter"), () => {})
        await supervisor.handle({ type: "evict", actor: actorIdentity }, () => {})
        assert.deepEqual(seen, [1, 0])
        unsubscribe()
        await supervisor.handle(invokeCommand("counter-1", "SessionCounter"), () => {})
        assert.deepEqual(seen, [1, 0])
    } finally {
        supervisor.close()
    }
})

test("activity resets the idle timeout without publishing dormant residency", async context => {
    context.mock.timers.enable({ apis: ["setTimeout"] })
    const supervisor = new ActorWorkerSupervisor({
        actorEntrypointUrl: "file:///unused.mjs",
        actorSchemas: undefined,
        actorIdleTimeoutMs: 10_000,
        createWorker: () => ({
            state: "ready",

            ready: async () => ["SessionCounter"],
            execute: async () => ({ type: "invoked", result: null, state: {} }),
            terminate() {}
        })
    })
    const seen: number[] = []
    supervisor.onActiveActorsChange(() => seen.push(supervisor.activeActors().length))
    try {
        await supervisor.ready()
        for (let request = 0; request < 5; request++) {
            await supervisor.handle(invokeCommand("counter-1", "SessionCounter"), () => {})
            context.mock.timers.tick(9_000)
            assert.deepEqual(supervisor.activeActors(), [actorIdentity])
        }
        assert.deepEqual(seen, [1])
        context.mock.timers.tick(1_000)
        assert.deepEqual(supervisor.activeActors(), [])
        assert.deepEqual(seen, [1, 0])
    } finally {
        supervisor.close()
    }
})

test("a running request stays resident beyond the idle timeout", async context => {
    context.mock.timers.enable({ apis: ["setTimeout"] })
    let finish: (() => void) | undefined
    const pending = new Promise<void>(resolve => {
        finish = resolve
    })
    let block = false
    const supervisor = new ActorWorkerSupervisor({
        actorEntrypointUrl: "file:///unused.mjs",
        actorSchemas: undefined,
        actorIdleTimeoutMs: 10_000,
        createWorker: () => ({
            state: "ready",

            ready: async () => ["SessionCounter"],
            async execute() {
                if (block) await pending
                return { type: "invoked", result: null, state: {} }
            },
            terminate() {}
        })
    })
    try {
        await supervisor.handle(invokeCommand("counter-1", "SessionCounter"), () => {})
        context.mock.timers.tick(9_000)
        block = true
        const running = supervisor.handle(invokeCommand("counter-1", "SessionCounter"), () => {})
        context.mock.timers.tick(30_000)
        assert.deepEqual(supervisor.activeActors(), [actorIdentity])
        finish!()
        assert.equal((await running).type, "invoked")
        context.mock.timers.tick(9_999)
        assert.deepEqual(supervisor.activeActors(), [actorIdentity])
        context.mock.timers.tick(1)
        assert.deepEqual(supervisor.activeActors(), [])
    } finally {
        finish?.()
        supervisor.close()
    }
})
