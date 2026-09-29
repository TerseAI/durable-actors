import { createActorTransport } from "durable-actors/backend"
import { startLocalActors } from "durable-actors/dev"
import assert from "node:assert/strict"
import { mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import { test } from "node:test"
import { fileURLToPath } from "node:url"

test("practice analytics use durable SQL events, deduplicate taps, undo, and recover", { timeout: 120_000 }, async t => {
    const dataDir = await mkdtemp(path.join(tmpdir(), "basketball-test-"))
    const options = { project: fileURLToPath(new URL("../", import.meta.url)), entrypoint: "src/actors.ts", projectId: "basketball-test", dataDir }
    let runtime
    t.after(async () => {
        await runtime?.stop()
        await rm(dataDir, { recursive: true, force: true })
    })
    runtime = await startLocalActors(options)
    let transport = connect(runtime)
    const call = (method, args = [], actorId = "practice") => transport.invoke("Practice", actorId, method, args)

    const empty = await call("summary")
    assert.equal(empty.points, 0)
    assert.equal(empty.fieldGoalPercentage, null)
    assert.equal(empty.eventCount, 0)
    assert.deepEqual(empty.recent, [])

    await call("record", ["one", "two", true])
    await call("record", ["two", "two", false])
    await call("record", ["three", "three", true])
    await call("record", ["four", "free", true])
    await call("record", ["five", "rebound", false])
    const stats = await call("record", ["five", "rebound", false])
    assert.equal(stats.points, 6)
    assert.equal(stats.eventCount, 5)
    assert.equal(stats.fieldGoalsMade, 2)
    assert.equal(stats.fieldGoalsAttempted, 3)
    assert.equal(stats.fieldGoalPercentage, 66.7)
    assert.equal(stats.effectiveFieldGoalPercentage, 83.3)
    assert.equal(stats.counters.rebound, 1)
    assert.deepEqual(stats.shots, [
        { kind: "two", made: 1, attempts: 2, percentage: 50 },
        { kind: "three", made: 1, attempts: 1, percentage: 100 },
        { kind: "free", made: 1, attempts: 1, percentage: 100 }
    ])
    assert.deepEqual(
        stats.lastTen.map(shot => shot.made),
        [1, 0, 1, 1]
    )

    const undone = await call("undo", ["five"])
    assert.equal(undone.counters.rebound, 0)
    assert.equal(undone.eventCount, 4)
    assert.deepEqual(await call("undo", ["five"]), undone)
    assert.deepEqual(await call("undo", ["one"]), undone)
    await assert.rejects(call("record", ["bad", "invalid", true]))
    assert.deepEqual(await call("summary"), undone)
    assert.equal((await call("summary", [], "separate")).eventCount, 0)

    await runtime.stop()
    runtime = await startLocalActors(options)
    transport = connect(runtime)
    assert.deepEqual(await call("summary"), undone)
    const recovered = await call("record", ["six", "three", false])
    assert.equal(recovered.points, 6)
    assert.equal(recovered.fieldGoalPercentage, 50)
    assert.equal(recovered.eventCount, 5)
    assert.equal(recovered.recent[0].id, "six")
})

function connect({ connection: { projectId, controlPlaneUrl } }) {
    return createActorTransport({ projectId, controlPlaneUrl })
}
