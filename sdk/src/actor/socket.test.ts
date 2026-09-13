import assert from "node:assert/strict"
import { test } from "node:test"

import { ActorProtocolError, ActorSerializationError } from "../errors.js"

import { decodeSocketMessage, runWithActorSockets } from "./socket.js"
import { parseSocketEffects } from "./socketProtocol.js"

test("broadcast tag matching modes survive socket transport", async () => {
    const published: unknown[] = []
    await runWithActorSockets(
        {},
        [],
        async scope => {
            for (const tagMatch of ["all", "any"] as const) {
                scope.broadcast({ type: "files-ready" }, { tags: ["file:a", "file:b"], tagMatch })
            }
        },
        async effects => {
            published.push(...parseSocketEffects(effects))
        }
    )
    assert.deepEqual(
        published,
        ["all", "any"].map(tag_match => ({
            type: "broadcast",
            message: { type: "text", data: '{"type":"files-ready"}' },
            except_connection_ids: [],
            tags: ["file:a", "file:b"],
            tag_match
        }))
    )
})

test("invalid broadcast tag matching modes fail before publishing", async () => {
    const result = await runWithActorSockets({}, [], async scope => {
        assert.throws(() => scope.broadcast({}, { tagMatch: "either" as never }), ActorProtocolError)
    })
    assert.deepEqual(result.effects, [])
    assert.throws(
        () =>
            parseSocketEffects([
                {
                    type: "broadcast",
                    message: { type: "text", data: "{}" },
                    except_connection_ids: [],
                    tags: [],
                    tag_match: "either"
                }
            ]),
        ActorProtocolError
    )
})

test("actor sends and broadcasts snapshot JSON values without caller serialization", async () => {
    const message = { type: "delta", payload: { text: "hello", flags: [true, null, 3] } }
    const result = await runWithActorSockets({}, [{ id: "socket-1", metadata: {}, tags: [] }], async scope => {
        scope.connection("socket-1").send(message)
        scope.broadcast(message, { tags: ["members"] })
        message.payload.text = "changed"
    })
    const encoded = { type: "text", data: '{"type":"delta","payload":{"text":"hello","flags":[true,null,3]}}' }
    assert.deepEqual(result.effects, [
        { type: "send", connection_id: "socket-1", message: encoded },
        { type: "broadcast", message: encoded, except_connection_ids: [], tags: ["members"] }
    ])
})

test("actor messages decode JSON and reject raw text and binary frames", () => {
    for (const value of [{ text: "hello" }, [1, true, null], "hello", 3, false, null]) {
        assert.deepEqual(decodeSocketMessage({ type: "text", data: JSON.stringify(value) }), value)
    }
    assert.throws(() => decodeSocketMessage({ type: "text", data: "hello" }), ActorProtocolError)
    assert.throws(() => decodeSocketMessage({ type: "binary", data: "e30=" }), ActorProtocolError)
})

test("invalid actor messages fail before queuing socket output", async () => {
    const circular: Record<string, unknown> = {}
    circular.self = circular
    const result = await runWithActorSockets({}, [{ id: "socket-1", metadata: {}, tags: [] }], async scope => {
        const socket = scope.connection("socket-1")
        for (const value of [undefined, 1n, circular]) {
            assert.throws(() => socket.send(value as never), ActorSerializationError)
        }
        assert.throws(() => socket.send(new Uint8Array([1]) as never), ActorProtocolError)
    })
    assert.deepEqual(result.effects, [])
})
