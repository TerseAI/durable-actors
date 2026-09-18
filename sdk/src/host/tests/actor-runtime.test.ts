import assert from "node:assert/strict"
import { test } from "node:test"

import { runWithActorClientForTests } from "../../../fixtures/actorClient.js"
import { Actor, registerActorClass } from "../../actor/actor.js"
import { Ephemeral, Persisted } from "../../actor/decorators.js"
import { Persistence } from "../../actor/schema.js"
import type { ActorConnection, ActorSocket } from "../../actor/socket.js"
import type { SocketEffect } from "../../actor/socketProtocol.js"
import { ActorRuntime } from "../actor-runtime.js"

export class Counter extends Actor {
    @Persisted private count = 0

    async increment(amount = 1): Promise<number> {
        this.count += amount
        return this.count
    }

    async getCount(): Promise<number> {
        return this.count
    }

    async getIdentity(): Promise<string> {
        return this.id
    }

    async explode(): Promise<never> {
        this.count = 999
        throw new CounterExplosionError()
    }
}

export class Forwarder extends Actor {
    async incrementCounter(): Promise<number> {
        return Counter.get("counter-1").increment()
    }
}

interface ChatSession {
    readonly userId: string
    readonly connectedAt: number
}

export class ChatRoom extends Actor<ChatSession, { text: string }> {
    @Persisted private events: string[] = []

    async onConnect(socket: ActorSocket<ChatSession>): Promise<void> {
        this.events.push(`connect:${socket.metadata.userId}:${(await this.getConnections()).length}`)
        socket.metadata = { ...socket.metadata, connectedAt: 2 }
        socket.setTags("member")
        socket.send({ text: "ready" })
    }

    async onMessage(socket: ActorSocket<ChatSession>, message: { text: string }): Promise<void> {
        this.events.push(`message:${socket.metadata.userId}:${message.text}`)
        this.broadcast(message)
    }

    async onDisconnect(socket: ActorSocket<ChatSession>, code: number, reason: string): Promise<void> {
        this.events.push(
            `disconnect:${socket.metadata.userId}:${code}:${reason}:${(await this.getConnections()).length}`
        )
    }

    async getEvents(): Promise<readonly string[]> {
        return this.events
    }

    async announce(message: string): Promise<void> {
        this.broadcast({ text: message })
    }
}

export class RejectingRoom extends Actor {
    async onConnect(socket: ActorSocket): Promise<void> {
        socket.reject(3000, "closed")
    }
}

const actorIdentity = {
    actor_type: "Counter",
    actor_id: "counter-1"
}

const forwarderIdentity = {
    ...actorIdentity,
    actor_type: "Forwarder",
    actor_id: "forwarder-1"
}

const counterDefinition = registerActorClass(Counter, {
    actorType: "Counter",
    fields: [{ name: "count", persistence: Persistence.Persisted }]
})
const forwarderDefinition = registerActorClass(Forwarder)
const chatDefinition = registerActorClass(ChatRoom, {
    actorType: "ChatRoom",
    fields: [{ name: "events", persistence: Persistence.Persisted, visibility: "private" }]
})
const rejectingDefinition = registerActorClass(RejectingRoom)

test("ephemeral caches survive resident calls and reset after failure or reconstruction", async () => {
    class CachingCounter extends Actor {
        @Persisted count = 0
        @Ephemeral cache = new Map<string, number>()

        async increment() {
            this.cache.set("calls", (this.cache.get("calls") ?? 0) + 1)
            return { count: ++this.count, calls: this.cache.get("calls") }
        }

        async fail() {
            this.count = 999
            this.cache.set("calls", 999)
            throw new Error("failed")
        }
    }
    const definition = registerActorClass(CachingCounter, {
        actorType: "CachingCounter",
        fields: [
            { name: "count", persistence: Persistence.Persisted },
            { name: "cache", persistence: Persistence.Ephemeral }
        ]
    })
    const runtime = new ActorRuntime(definition)
    const command = {
        type: "invoke" as const,
        request_id: "cache",
        actor: { ...actorIdentity, actor_type: "CachingCounter" },
        method: "increment",
        args: [],
        state: null
    }
    assert.deepEqual(await runtime.handle(command), {
        type: "invoked",
        result: { count: 1, calls: 1 },
        state: { count: 1 }
    })
    assert.deepEqual(await runtime.handle(command), {
        type: "invoked",
        result: { count: 2, calls: 2 },
        state: { count: 2 }
    })
    assert.equal((await runtime.handle({ ...command, method: "fail" })).type, "failed")
    const restored = { ...command, state: { count: 2, cache: "stale" } }
    const expected = { type: "invoked", result: { count: 3, calls: 1 }, state: { count: 3 } }
    assert.deepEqual(await runtime.handle({ ...command, resident_only: true, state: undefined }), expected)
    assert.deepEqual(await new ActorRuntime(definition).handle(restored), expected)
})

test("failed state recovery reports a fatal error instead of keeping a damaged instance", async () => {
    let rejectConstruction = false
    class RecoveryFailure extends Actor {
        constructor() {
            super()
            if (rejectConstruction) throw new Error("cannot restore")
        }

        async fail(): Promise<void> {
            rejectConstruction = true
            throw new Error("request failed")
        }
    }
    const runtime = new ActorRuntime(registerActorClass(RecoveryFailure))
    const command = {
        type: "invoke" as const,
        actor: { actor_type: "RecoveryFailure", actor_id: "one" },
        request_id: "failed-recovery",
        method: "fail",
        args: [],
        state: null
    }
    assert.deepEqual(await runtime.handle(command), {
        type: "failed",
        code: "invalid_actor_state",
        message: "cannot restore"
    })
    assert.deepEqual(await runtime.handle({ ...command, resident_only: true, state: undefined }), {
        type: "state_required"
    })
})

test("streams actor output before execution finishes without replaying it in the final reply", async () => {
    let release!: () => void
    const gate = new Promise<void>(resolve => {
        release = resolve
    })
    class StreamingActor extends Actor {
        async stream() {
            this.broadcast("first")
            await gate
            this.broadcast("last")
        }
    }
    const effects: SocketEffect[] = []
    let first!: () => void
    const published = new Promise<void>(resolve => {
        first = resolve
    })
    const runtime = new ActorRuntime(registerActorClass(StreamingActor), async batch => {
        effects.push(...batch)
        first()
    })
    let completed = false
    const invocation = runtime
        .handle({
            type: "invoke",
            request_id: "stream-1",
            actor: { ...actorIdentity, actor_type: "StreamingActor" },
            method: "stream",
            args: [],
            state: null
        })
        .then(reply => {
            completed = true
            return reply
        })
    try {
        await Promise.race([
            published,
            new Promise((_, reject) => setTimeout(() => reject(new Error("output waited for actor completion")), 500))
        ])
        assert.equal(completed, false)
        assert.equal(effects.length, 1)
    } finally {
        release()
    }
    assert.deepEqual(await invocation, { type: "invoked", result: null, state: {} })
    assert.deepEqual(
        effects.map(effect => effect.type === "broadcast" && effect.message),
        [
            { type: "text", data: JSON.stringify("first") },
            { type: "text", data: JSON.stringify("last") }
        ]
    )
})

test("rejecting a connection never publishes live socket effects", async () => {
    const runtime = new ActorRuntime(rejectingDefinition, async () => {
        assert.fail("connection effects escaped before acceptance")
    })
    const reply = await runtime.handle({
        type: "websocket_event",
        request_id: "connect-1",
        actor: { ...actorIdentity, actor_type: "RejectingRoom" },
        state: null,
        connections: [],
        event: { type: "connect", connection: { id: "socket-1", metadata: {}, tags: [] } }
    })
    assert.equal(reply.type, "websocket_handled")
})

test("batches pending stream output in order and surfaces publish failures", async () => {
    class BurstActor extends Actor {
        async stream() {
            for (let index = 0; index < 10; index++) this.broadcast(String(index))
        }
    }
    const batches: (readonly SocketEffect[])[] = []
    const definition = registerActorClass(BurstActor)
    const command = {
        type: "invoke" as const,
        request_id: "burst",
        actor: { ...actorIdentity, actor_type: "BurstActor" },
        method: "stream",
        args: [],
        state: null
    }
    const runtime = new ActorRuntime(definition, async effects => {
        batches.push(effects)
    })
    assert.equal((await runtime.handle(command)).type, "invoked")
    assert.ok(batches.length <= 2, "queued deltas should not each cost a network round trip")
    assert.deepEqual(
        batches.flat().map(effect => effect.type === "broadcast" && effect.message.data),
        Array.from({ length: 10 }, (_, index) => JSON.stringify(String(index)))
    )
    const failing = new ActorRuntime(definition, async () => {
        throw new Error("publish denied")
    })
    const reply = await failing.handle(command)
    assert.equal(reply.type, "failed")
    if (reply.type === "failed") assert.match(reply.message, /publish denied/)
})

test("a resident-only command requests hydration before constructing or executing an actor", async () => {
    const runtime = new ActorRuntime(counterDefinition)
    const command = {
        type: "invoke" as const,
        request_id: "resident-1",
        actor: actorIdentity,
        method: "increment",
        args: [1],
        state: null,
        resident_only: true
    }
    assert.deepEqual(await runtime.handle(command), { type: "state_required" })
    assert.deepEqual(await runtime.handle({ ...command, resident_only: false, state: { count: 9 } }), {
        type: "invoked",
        result: 10,
        state: { count: 10 }
    })
    assert.deepEqual(await runtime.handle(command), { type: "invoked", result: 11, state: { count: 11 } })
})

test("a resident actor rejects another type or ID without changing its state", async () => {
    const runtime = new ActorRuntime(counterDefinition)
    const command = {
        type: "invoke" as const,
        request_id: "identity-1",
        actor: actorIdentity,
        method: "increment",
        args: [1],
        state: null
    }
    assert.deepEqual(await runtime.handle(command), { type: "invoked", result: 1, state: { count: 1 } })
    for (const actor of [
        { ...actorIdentity, actor_type: "OtherCounter" },
        { ...actorIdentity, actor_id: "other" }
    ]) {
        const reply = await runtime.handle({ ...command, actor })
        assert.equal(reply.type, "failed")
        if (reply.type === "failed")
            assert.equal(
                reply.code,
                actor.actor_type === actorIdentity.actor_type ? "actor_identity_mismatch" : "actor_type_not_found"
            )
    }
    assert.deepEqual(await runtime.handle(command), { type: "invoked", result: 2, state: { count: 2 } })
})

test("keeps a successful actor instance resident and restores it after failure", async () => {
    const runtime = new ActorRuntime(counterDefinition)
    assert.deepEqual(
        await runtime.handle({
            type: "invoke",
            request_id: "request-1",
            actor: actorIdentity,
            method: "increment",
            args: [2],
            state: null
        }),
        { type: "invoked", result: 2, state: { count: 2 } }
    )
    assert.deepEqual(
        await runtime.handle({
            type: "invoke",
            request_id: "request-2",
            actor: actorIdentity,
            method: "increment",
            args: [3],
            state: { count: 2 }
        }),
        { type: "invoked", result: 5, state: { count: 5 } }
    )
    assert.deepEqual(
        await runtime.handle({
            type: "invoke",
            request_id: "request-3",
            actor: actorIdentity,
            method: "getCount",
            args: [],
            state: { count: 2 }
        }),
        { type: "invoked", result: 5, state: { count: 5 } }
    )

    assert.deepEqual(
        await runtime.handle({
            type: "invoke",
            request_id: "request-4",
            actor: actorIdentity,
            method: "getIdentity",
            args: [],
            state: { count: 2 }
        }),
        { type: "invoked", result: "counter-1", state: { count: 5 } }
    )

    assert.deepEqual(
        await runtime.handle({
            type: "invoke",
            request_id: "request-5",
            actor: actorIdentity,
            method: "explode",
            args: [],
            state: { count: 2 }
        }),
        { type: "failed", code: "actor_method_failed", message: "boom" }
    )
    assert.deepEqual(
        await runtime.handle({
            type: "invoke",
            request_id: "request-6",
            actor: actorIdentity,
            method: "getCount",
            args: [],
            resident_only: true
        }),
        { type: "invoked", result: 5, state: { count: 5 } }
    )
})

test("Actor.get returns a typed forwarding reference", async () => {
    const calls: unknown[] = []
    await runWithActorClientForTests(
        {
            requestId: () => "fixed-request",
            invoke: async request => {
                calls.push(request)
                return 7
            }
        },
        async () => {
            const counter = Counter.get("counter-1")

            assert.equal(Reflect.get(counter, "missingMethod"), undefined)
            assert.equal(await counter.increment(3), 7)
            assert.deepEqual(calls, [
                {
                    requestId: "fixed-request",
                    actorType: "Counter",
                    actorId: "counter-1",
                    method: "increment",
                    args: [3]
                }
            ])
        }
    )
})

test("Actor.get connects with typed metadata", async () => {
    const calls: unknown[] = []
    const connection: ActorConnection = {
        readyState: 1,
        send: () => undefined,
        close: () => undefined,
        addEventListener: () => undefined,
        removeEventListener: () => undefined
    }
    await runWithActorClientForTests(
        {
            requestId: () => "fixed-request",
            invoke: async () => undefined,
            connect: async request => {
                calls.push(request)
                return connection
            }
        },
        async () => {
            assert.equal(await ChatRoom.get("room-1").connect({ userId: "user-1", connectedAt: 1 }), connection)
        }
    )
    assert.deepEqual(calls, [
        {
            requestId: "fixed-request",
            actorType: "ChatRoom",
            actorId: "room-1",
            metadata: { userId: "user-1", connectedAt: 1 }
        }
    ])
})

test("sends durable actor properties when a connection has no onConnect hook", async () => {
    const runtime = new ActorRuntime(counterDefinition)
    const connection = { id: "connection-1", metadata: {}, tags: [] }

    assert.deepEqual(
        await runtime.handle({
            type: "websocket_event",
            request_id: "request-state",
            actor: actorIdentity,
            event: { type: "connect", connection },
            connections: [connection],
            state: null
        }),
        {
            type: "websocket_handled",
            state: { count: 0 },
            effects: [
                {
                    type: "state_snapshot",
                    connection_id: "connection-1",
                    state: { count: 0 }
                }
            ]
        }
    )
})

test("does not expose actor properties to a rejected connection", async () => {
    const runtime = new ActorRuntime(rejectingDefinition)
    const actor = { ...actorIdentity, actor_type: "RejectingRoom", actor_id: "room-1" }
    const connection = { id: "connection-1", metadata: {}, tags: [] }

    assert.deepEqual(
        await runtime.handle({
            type: "websocket_event",
            request_id: "request-reject",
            actor,
            event: { type: "connect", connection },
            connections: [connection],
            state: null
        }),
        {
            type: "websocket_handled",
            state: {},
            effects: [{ type: "reject", connection_id: "connection-1", code: 3000, reason: "closed" }]
        }
    )
})

test("runs the full socket lifecycle and exposes live actor connections", async () => {
    const runtime = new ActorRuntime(chatDefinition)
    const actor = { ...actorIdentity, actor_type: "ChatRoom", actor_id: "room-1" }
    const connection = { id: "connection-1", metadata: { userId: "user-1", connectedAt: 1 }, tags: [] }

    assert.deepEqual(
        await runtime.handle({
            type: "websocket_event",
            request_id: "request-1",
            actor,
            event: { type: "connect", connection },
            connections: [connection],
            state: null
        }),
        {
            type: "websocket_handled",
            state: { events: ["connect:user-1:1"] },
            effects: [
                { type: "set_metadata", connection_id: "connection-1", metadata: { userId: "user-1", connectedAt: 2 } },
                { type: "set_tags", connection_id: "connection-1", tags: ["member"] },
                {
                    type: "send",
                    connection_id: "connection-1",
                    message: { type: "text", data: JSON.stringify({ text: "ready" }) }
                },
                {
                    type: "state_snapshot",
                    connection_id: "connection-1",
                    state: {}
                }
            ]
        }
    )

    const connected = { ...connection, metadata: { userId: "user-1", connectedAt: 2 }, tags: ["member"] }
    assert.deepEqual(
        await runtime.handle({
            type: "websocket_event",
            request_id: "request-2",
            actor,
            event: {
                type: "message",
                connection_id: "connection-1",
                message: { type: "text", data: JSON.stringify({ text: "hello" }) }
            },
            connections: [connected],
            state: { events: ["connect:user-1:1"] }
        }),
        {
            type: "websocket_handled",
            state: { events: ["connect:user-1:1", "message:user-1:hello"] },
            effects: [
                {
                    type: "broadcast",
                    message: { type: "text", data: JSON.stringify({ text: "hello" }) },
                    except_connection_ids: [],
                    tags: []
                }
            ]
        }
    )

    assert.deepEqual(
        await runtime.handle({
            type: "invoke",
            request_id: "request-announce",
            actor,
            method: "announce",
            args: ["announcement"],
            state: { events: ["connect:user-1:1", "message:user-1:hello"] }
        }),
        {
            type: "invoked",
            result: null,
            state: { events: ["connect:user-1:1", "message:user-1:hello"] },
            effects: [
                {
                    type: "broadcast",
                    message: { type: "text", data: JSON.stringify({ text: "announcement" }) },
                    except_connection_ids: [],
                    tags: []
                }
            ]
        }
    )

    assert.deepEqual(
        await runtime.handle({
            type: "websocket_event",
            request_id: "request-3",
            actor,
            event: { type: "disconnect", connection: connected, code: 1000, reason: "done", was_clean: true },
            connections: [],
            state: { events: ["connect:user-1:1", "message:user-1:hello"] }
        }),
        {
            type: "websocket_handled",
            state: { events: ["connect:user-1:1", "message:user-1:hello", "disconnect:user-1:1000:done:0"] },
            effects: []
        }
    )
})

test("the injected invoker remains available inside actor unit tests", async () => {
    await runWithActorClientForTests(
        {
            requestId: () => "nested-request",
            invoke: async request => {
                throw new Error(`unexpected external nested invocation ${request.requestId}`)
            }
        },
        async () => {
            const runtime = new ActorRuntime(forwarderDefinition)
            assert.deepEqual(
                await runtime.handle({
                    type: "invoke",
                    request_id: "parent-request",
                    actor: forwarderIdentity,
                    method: "incrementCounter",
                    args: [],
                    state: null
                }),
                {
                    type: "failed",
                    code: "actor_method_failed",
                    message: "unexpected external nested invocation nested-request"
                }
            )
        }
    )
})

class CounterExplosionError extends Error {
    constructor() {
        super("boom")
        this.name = "CounterExplosionError"
    }
}
