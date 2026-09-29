import { createActorTransport } from "durable-actors/backend"
import { startLocalActors } from "durable-actors/dev"
import assert from "node:assert/strict"
import { mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import { test } from "node:test"
import { fileURLToPath } from "node:url"

test("notebooks persist SQL and fields together, roll back failures, and isolate actor IDs", { timeout: 120_000 }, async t => {
    const dataDir = await mkdtemp(path.join(tmpdir(), "sqlite-notebook-"))
    const options = {
        project: fileURLToPath(new URL("../", import.meta.url)),
        entrypoint: "src/actors.ts",
        projectId: "sqlite-test",
        dataDir
    }
    let runtime
    t.after(async () => {
        await runtime?.stop()
        await rm(dataDir, { recursive: true, force: true })
    })
    runtime = await startLocalActors(options)
    let transport = connect(runtime)
    const call = (method, args = [], actorId = "notebook") => transport.invoke("Notebook", actorId, method, args)

    assert.deepEqual(await call("list"), { edits: 0, notes: [] })
    const text = "SQLite's bound parameters handle 'quotes' safely."
    const first = await call("add", [text])
    assert.deepEqual(first, { id: 1, text })
    assert.deepEqual(await call("list"), { edits: 1, notes: [first] })

    await assert.rejects(call("add", ["   "]), /Note text cannot be empty/)
    await assert.rejects(call("addThenFail", ["This must roll back"]), /Intentional rollback/)
    assert.deepEqual(await call("list"), { edits: 1, notes: [first] })
    assert.deepEqual(await call("list", [], "another-notebook"), { edits: 0, notes: [] })

    await runtime.stop()
    runtime = await startLocalActors(options)
    transport = connect(runtime)
    assert.deepEqual(await call("list"), { edits: 1, notes: [first] })
    const second = await call("add", ["Written after restart"])
    assert.deepEqual(second, { id: 2, text: "Written after restart" })
    assert.deepEqual(await call("list"), { edits: 2, notes: [first, second] })
})

function connect({ connection: { projectId, controlPlaneUrl } }) {
    return createActorTransport({ projectId, controlPlaneUrl })
}
