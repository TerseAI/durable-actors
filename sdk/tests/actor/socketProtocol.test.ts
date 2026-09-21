import assert from "node:assert/strict"
import { test } from "node:test"

import { parseSocketEffects } from "../../src/actor/socketProtocol.js"
import { receivedMessage } from "../../src/actor/socketValidation.js"
import { ActorProtocolError } from "../../src/errors.js"

test("retains committed state versions and accepts automatic state control messages", () => {
    const effects = [{ type: "state_snapshot", connection_id: "socket", state: { count: 1 }, version: 3 }]
    assert.deepEqual(parseSocketEffects(effects), effects)
    assert.deepEqual(receivedMessage({ type: "state", state: { count: 1 }, version: 3 }), {
        type: "state",
        state: { count: 1 },
        version: 3
    })
    const update = { type: "state_update", changes: { count: 2 }, removed: [], version: 4 }
    assert.deepEqual(receivedMessage(update), update)
})

test("rejects malformed socket effects", () => {
    assert.throws(
        () => parseSocketEffects([{ type: "close", connection_id: "socket-1", code: 1001, reason: "" }]),
        ActorProtocolError
    )
    assert.throws(
        () => parseSocketEffects({ type: "set_tags", connection_id: "socket-1", tags: [] }),
        ActorProtocolError
    )
})
