import assert from "node:assert/strict"
import { test } from "node:test"

import { GarbageCollection, byteLength } from "../../src/payload/garbage-collection.js"

const MIB = 1024 * 1024

test("collects each time 8 MiB of payload has passed, however it arrives", () => {
    let collections = 0
    const collection = new GarbageCollection(() => collections++)

    collection.payloadPassed(8 * MIB - 1)
    assert.equal(collections, 0)
    collection.payloadPassed(1)
    assert.equal(collections, 1)

    for (let passed = 0; passed < 20 * MIB; passed += 64 * 1024) collection.payloadPassed(64 * 1024)
    assert.equal(collections, 3)
})

test("collects with the engine it runs on", () => {
    new GarbageCollection().payloadPassed(8 * MIB)
})

test("a payload is as long as its parts together", () => {
    assert.equal(byteLength([]), 0)
    assert.equal(byteLength([new Uint8Array(3), new Uint8Array(0), Buffer.from("café")]), 8)
})
