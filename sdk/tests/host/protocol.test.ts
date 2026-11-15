import assert from "node:assert/strict"
import { test } from "node:test"

import type { HostSocketEffect, SocketEffect } from "../../src/actor/socketProtocol.js"
import { ActorProtocolError } from "../../src/errors.js"
import { detachEffects, detachReply, parseActorSessionServerMessage } from "../../src/host/protocol.js"
import type { ActorExecutorReply } from "../../src/host/protocol.js"
import type { JsonValue } from "../../src/json.js"
import { encodeJson } from "../../src/payload/json-text.js"
import { attachEffects, attachReply } from "../fixtures/detached.js"

const messages = [{ text: "caf\u00e9 \\ \u2028" }, "", "last"] as const
const encoded = (value: JsonValue) => encodeJson(value)
const effects = socketEffects(messages.map(encoded))
const wireEffects = socketEffects(messages.map(message => JSON.stringify(message)))

test("rejects malformed actor session messages", () => {
    assert.throws(
        () =>
            parseActorSessionServerMessage({
                type: "command",
                message_id: 1,
                command: { type: "invoke", request_id: false }
            }),
        ActorProtocolError
    )
})

test("database calls are not session messages", () => {
    assert.throws(() => parseActorSessionServerMessage({ type: "database_result", call_id: 1 }), ActorProtocolError)
})

test("a reply leaves its result, its effects and the text of each message as parts of their own", () => {
    const result = encodeJson({ list: [1, "two"] })
    const { parts, ...head } = detachReply({ type: "invoked", sequence: 3, result, effects })

    assert.deepEqual(head, { type: "invoked", sequence: 3 })
    assert.deepEqual(parts, [
        result,
        JSON.stringify([
            { type: "send", connection_id: "socket-1", message: { type: "text" } },
            { type: "send", connection_id: "socket-1", message: { type: "binary", data: "AAH/" } },
            { type: "set_tags", connection_id: "socket-1", tags: ["room"] },
            { type: "broadcast", message: { type: "text" }, except_connection_ids: [], tags: [] },
            { type: "broadcast", message: { type: "text" }, except_connection_ids: ["socket-1"], tags: [] }
        ]),
        ...messages.map(encoded)
    ])
})

test("every reply is whole again once its parts are attached", () => {
    const held: ActorExecutorReply[] = replies(encoded, effects)
    const attached = replies(JSON.stringify, wireEffects)

    held.forEach((reply, index) => assert.deepEqual(attachReply(detachReply(reply)), attached[index]))
    assert.deepEqual(attachEffects(detachEffects(effects)), wireEffects)
})

test("only replies that carry a result or effects have parts", () => {
    assert.equal(detachReply({ type: "invoked", result: encodeJson(null) }).parts?.length, 2)
    assert.equal(detachReply({ type: "websocket_handled", effects: [] }).parts?.length, 1)
    for (const reply of [
        { type: "failed", code: "c", message: "m" },
        { type: "evicted" },
        { type: "hydrated" }
    ] as const)
        assert.equal(detachReply(reply).parts, undefined)
})

function socketEffects<Text>([first, empty, last]: Text[]): SocketEffect<Text>[] {
    return [
        { type: "send", connection_id: "socket-1", message: { type: "text", data: first! } },
        { type: "send", connection_id: "socket-1", message: { type: "binary", data: "AAH/" } },
        { type: "set_tags", connection_id: "socket-1", tags: ["room"] },
        { type: "broadcast", message: { type: "text", data: empty! }, except_connection_ids: [], tags: [] },
        { type: "broadcast", message: { type: "text", data: last! }, except_connection_ids: ["socket-1"], tags: [] }
    ]
}

// The same replies with their texts in either form: encoded, as this process holds them, or as strings.
function replies<Text, Effect extends HostSocketEffect | SocketEffect>(
    text: (value: JsonValue) => Text,
    effects: Effect[]
) {
    return [
        { type: "invoked" as const, result: text({ list: [1, null, "two"] }) },
        { type: "invoked" as const, sequence: 3, result: text(null), effects },
        { type: "websocket_handled" as const, effects },
        { type: "websocket_handled" as const, sequence: 0, effects: [] },
        { type: "failed" as const, code: "actor_method_failed", message: 'said "no"' },
        { type: "evicted" as const },
        { type: "hydrated" as const }
    ]
}
