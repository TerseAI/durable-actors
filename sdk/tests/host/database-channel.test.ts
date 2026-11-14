import assert from "node:assert/strict"
import { spawn } from "node:child_process"
import { once } from "node:events"
import { createInterface } from "node:readline"
import type { Duplex } from "node:stream"
import { test } from "node:test"
import { fileURLToPath } from "node:url"

test("database calls block on the Rust host's reply, without a size limit", { timeout: 10_000 }, async () => {
    const client = fileURLToPath(new URL("../fixtures/database-channel-client.js", import.meta.url))
    const child = spawn(process.execPath, [client], { stdio: ["ignore", "pipe", "inherit", "pipe"] })
    const channel = child.stdio[3] as Duplex
    const requests: { call_id: number; message_id?: number }[] = []
    let stdout = ""
    child.stdout!.on("data", data => (stdout += data))
    createInterface({ input: channel }).on("line", line => {
        const request = JSON.parse(line)
        requests.push(request)
        const reply = (fields: object) =>
            channel.write(`${JSON.stringify({ type: "database_result", call_id: request.call_id, ...fields })}\n`)
        if (requests.length === 1) reply({ result: { type: "fields", fields: { big: "x".repeat(200_000) } } })
        else if (requests.length === 2) reply({ error: "storage rejected write" })
        else channel.end()
    })
    const [code] = await once(child, "exit")
    assert.equal(code, 0)
    assert.deepEqual(
        requests.map(request => request.message_id),
        [7, undefined, 8]
    )
    assert.deepEqual(JSON.parse(stdout), [
        { size: 200_000 },
        { error: "storage rejected write" },
        { error: "Rust host closed the database channel" }
    ])
})
