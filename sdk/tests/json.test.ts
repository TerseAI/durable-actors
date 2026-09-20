import assert from "node:assert/strict"
import { test } from "node:test"

import { ActorSerializationError } from "../src/errors.js"
import { cloneJson } from "../src/json.js"

test("JSON clones preserve serialized values without sharing nested state", () => {
    const value = {
        nested: { count: 1 },
        omitted: undefined,
        values: [undefined, NaN],
        date: new Date("2026-01-01T00:00:00Z")
    }
    const cloned = cloneJson(value, "test value")
    value.nested.count = 2
    assert.deepEqual(cloned, { nested: { count: 1 }, values: [null, null], date: "2026-01-01T00:00:00.000Z" })
})

test("JSON clones reject unsupported roots and cycles with a labeled error", () => {
    const circular = { self: {} }
    circular.self = circular
    for (const value of [undefined, () => {}, Symbol("value"), 1n, circular]) {
        assert.throws(
            () => cloneJson(value, "actor result"),
            error => error instanceof ActorSerializationError && error.message.includes("actor result")
        )
    }
})
