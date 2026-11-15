import assert from "node:assert/strict"
import { closeSync, mkdtempSync, openSync, rmSync } from "node:fs"
import os from "node:os"
import path from "node:path"
import { test } from "node:test"

import { DescriptorStream } from "../../src/payload/blocking-stream.js"

test("a descriptor is written whole and read until it ends", context => {
    const directory = mkdtempSync(path.join(os.tmpdir(), "blocking-stream-"))
    const file = path.join(directory, "stream")
    const written = Uint8Array.from({ length: 200_000 }, (_, index) => index % 253)
    const output = openSync(file, "w")
    const input = openSync(file, "r")
    context.after(() => {
        closeSync(output)
        closeSync(input)
        rmSync(directory, { recursive: true })
    })

    new DescriptorStream(output).write(written)
    const stream = new DescriptorStream(input)
    const read = new Uint8Array(written.byteLength + 1)
    let filled = 0
    for (let count = stream.read(read, filled); count > 0; count = stream.read(read, filled)) filled += count

    assert.equal(filled, written.byteLength)
    assert.deepEqual(read.subarray(0, filled), written)
})
