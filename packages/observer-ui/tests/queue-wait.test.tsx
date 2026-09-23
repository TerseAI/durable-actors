import assert from "node:assert/strict"
import { test } from "node:test"

import { formatWait, queueWaitByActor, queueWaitByInstance, queueWaitRows } from "../src/queue-wait.js"

test("queue wait rollups weight instances by admitted attempts", () => {
    const rows = [
        { actorName: "Room", actorId: "lobby", admitted: 2, averageMs: 20, maxMs: 30 },
        { actorName: "Counter", actorId: "one", admitted: 1, averageMs: 4, maxMs: 4 },
        { actorName: "Room", actorId: "random", admitted: 1, averageMs: 100, maxMs: 100 }
    ]
    assert.deepEqual(
        [...queueWaitByActor(rows)],
        [
            ["Room", { admitted: 3, averageMs: 140 / 3, maxMs: 100 }],
            ["Counter", { admitted: 1, averageMs: 4, maxMs: 4 }]
        ]
    )
    assert.deepEqual([...queueWaitByInstance(rows, "Room").keys()], ["lobby", "random"])
    assert.equal(queueWaitByInstance(rows, "Room").get("lobby")!.averageMs, 20)
})

test("queue wait rows are validated and formatted", () => {
    assert.throws(() => queueWaitRows([{ actorName: "Room", actorId: "lobby", admitted: 0, averageMs: 1, maxMs: 1 }]))
    assert.throws(() => queueWaitRows([{ actorName: "Room", actorId: "lobby", admitted: 1, averageMs: null, maxMs: 1 }]))
    assert.equal(formatWait(undefined), "—")
    assert.equal(formatWait(12.34), "12.3 ms")
    assert.equal(formatWait(1500), "1.5 s")
})
