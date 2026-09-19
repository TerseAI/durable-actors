import assert from "node:assert/strict"
import { test } from "node:test"

import { ActorProtocolError } from "../../src/errors.js"
import { parseActorSessionServerMessage } from "../../src/host/protocol.js"

test("rejects malformed actor session messages", () => {
    assert.throws(
        () =>
            parseActorSessionServerMessage(
                '{"type":"command","message_id":1,"command":{"type":"invoke","request_id":false}}'
            ),
        ActorProtocolError
    )
})
