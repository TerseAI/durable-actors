import assert from "node:assert/strict"
import { test } from "node:test"

import { ActorProtocolError } from "../../src/errors.js"
import { parseActorSessionServerMessage } from "../../src/host/protocol.js"

test("rejects malformed actor session messages", () => {
    for (const document of [
        '{"type":"command","message_id":1,"command":{"type":"invoke","request_id":false}}',
        '{"type":"command","message_id":1,"command":{"type":"invoke","request_id":"r","method":"m","args":{}}}',
        '{"type":"actor_result","call_id":1}'
    ])
        assert.throws(() => parseActorSessionServerMessage(document), ActorProtocolError)
})

test("drops __proto__ keys from relayed values however JSON spells them", () => {
    for (const key of [
        "__proto__",
        "\\u005f_proto__",
        "__\\u0070roto__",
        "__p\\u0072oto__",
        "__pr\\u006Fto__",
        "__pro\\u0074o__"
    ]) {
        const value = `{"${key}":{"polluted":true},"kept":[{"${key}":1}]}`
        const kept = { kept: [{}] }
        assert.deepEqual(
            parseActorSessionServerMessage(
                `{"type":"database_result","call_id":1,"result":{"type":"fields","fields":{"${key}":1,"state":${value}}}}`
            ),
            { type: "database_result", call_id: 1, result: { type: "fields", fields: { state: kept } } }
        )
        assert.deepEqual(
            parseActorSessionServerMessage(
                `{"type":"command","message_id":1,"command":{"type":"invoke","request_id":"r","method":"m","args":[${value}]}}`
            ),
            { type: "command", message_id: 1, command: { type: "invoke", request_id: "r", method: "m", args: [kept] } }
        )
        assert.deepEqual(parseActorSessionServerMessage(`{"type":"actor_result","call_id":1,"outcome":${value}}`), {
            type: "actor_result",
            call_id: 1,
            outcome: kept
        })
    }
})

test("parses database results", () => {
    for (const fields of [
        {
            result: {
                type: "rows",
                columns: ["value"],
                rows: [
                    [{ type: "integer", value: "9007199254740993" }, { type: "blob", value: "AAH/" }, { type: "null" }]
                ]
            }
        },
        {
            result: {
                type: "fields",
                fields: { count: 1, nested: [{ deep: [1.5, "__proto__", null, true, { empty: [] }] }] }
            }
        },
        { result: { type: "alarm", timestamp_ms: 1_000 } },
        { result: { type: "done" } },
        { error: "storage rejected write" }
    ]) {
        const message = { type: "database_result", call_id: 1, ...fields }
        assert.deepEqual(parseActorSessionServerMessage(JSON.stringify(message)), message)
    }
})

test("rejects ambiguous or malformed database results", () => {
    for (const fields of [
        {},
        { result: { type: "done" }, error: "both" },
        { result: { type: "fields", fields: [] } },
        { result: { type: "rows", columns: ["value"], rows: [[{ type: "integer", value: "1.5" }]] } }
    ]) {
        assert.throws(
            () => parseActorSessionServerMessage(JSON.stringify({ type: "database_result", call_id: 1, ...fields })),
            ActorProtocolError
        )
    }
})
