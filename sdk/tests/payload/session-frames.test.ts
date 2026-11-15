import assert from "node:assert/strict"
import { test } from "node:test"

import { ActorProtocolError } from "../../src/errors.js"
import { SessionFrames } from "../../src/payload/session-frames.js"
import type { SessionFrame } from "../../src/payload/session-frames.js"

const session = Buffer.concat([
    Buffer.from('{"type":"command","message_id":1}\n'),
    Buffer.from('{"type":"socket_effects","message_id":2,"parts":[9]}\n'),
    Buffer.from("line\none\n"),
    Buffer.from('{"type":"reply","parts":[0,3,0,5]}\n'),
    Buffer.from("caf\u00e9\u2028"),
    Buffer.from('{"type":"socket_effects","message_id":3,"parts":[]}\n')
])
const expected = [
    { head: { type: "command", message_id: 1 }, parts: [] },
    { head: { type: "socket_effects", message_id: 2, parts: [9] }, parts: ["line\none\n"] },
    { head: { type: "reply", parts: [0, 3, 0, 5] }, parts: ["", "caf", "", "\u00e9\u2028"] },
    { head: { type: "socket_effects", message_id: 3, parts: [] }, parts: [] }
]

test("reads the same messages however the session is cut into chunks", () => {
    for (const size of [1, 2, 7, 50, session.length]) {
        const frames = new SessionFrames()
        const read: SessionFrame[] = []
        for (let start = 0; start < session.length; start += size)
            read.push(...frames.accept(session.subarray(start, start + size)))
        assert.deepEqual(read.map(readable), expected, `chunks of ${size} bytes`)
    }
})

test("a part owns its buffer rather than sharing the chunk it arrived in", () => {
    const [, frame] = new SessionFrames().accept(session)
    const part = frame!.parts[0]!
    assert.equal(part.byteLength, part.buffer.byteLength)
    assert.notEqual(part.buffer, session.buffer)
})

test("rejects a line that is not JSON or announces parts that are not sizes", () => {
    for (const line of ["not json\n", '{"parts":[-1]}\n', '{"parts":[1.5]}\n', '{"parts":"3"}\n', "[1]\n"])
        assert.throws(() => new SessionFrames().accept(Buffer.from(line)), ActorProtocolError, line)
})

function readable(frame: SessionFrame): unknown {
    return { head: frame.head, parts: frame.parts.map(part => Buffer.from(part).toString()) }
}
