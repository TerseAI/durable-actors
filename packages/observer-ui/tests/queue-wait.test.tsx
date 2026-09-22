import assert from "node:assert/strict"
import { DatabaseSync } from "node:sqlite"
import { test } from "node:test"

import type { ObserverQuery, ObserverQueryResult, RequestTrace } from "../src/client.js"
import { formatWait, queueWaitByActor, queueWaitByInstance, queueWaitQuery, queueWaitRows } from "../src/queue-wait.js"

function database() {
    const db = new DatabaseSync(":memory:")
    db.exec(`CREATE TABLE traces (position INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL UNIQUE, event TEXT NOT NULL,
            started_at_ms INTEGER NOT NULL DEFAULT 0, actor_name TEXT NOT NULL DEFAULT '', actor_id TEXT NOT NULL DEFAULT '', outcome TEXT NOT NULL DEFAULT '');
        CREATE VIEW request_events AS SELECT position AS sequence, event_id, started_at_ms, actor_name, actor_id, outcome, event,
            json_extract(event, '$.kind') AS kind,
            json_extract(event, '$.operation') AS operation,
            json_extract(event, '$.queueWaitMs') AS queue_wait_ms
        FROM traces;`)
    return db
}
let counter = 0
function append(db: DatabaseSync, trace: Partial<RequestTrace>) {
    counter++
    const event = {
        eventId: `event-${counter}`,
        requestId: `request-${counter}`,
        hostId: "host",
        sessionId: "session",
        actorName: "Room",
        actorId: "lobby",
        kind: "method",
        operation: "post",
        connectionId: null,
        startedAtMs: 5000,
        durationMs: 50,
        queueWaitMs: 10,
        outcome: "completed",
        ...trace
    }
    db.prepare("INSERT INTO traces (event_id, event, started_at_ms, actor_name, actor_id, outcome) VALUES (?, ?, ?, ?, ?, ?)").run(
        event.eventId,
        JSON.stringify(event),
        event.startedAtMs,
        event.actorName,
        event.actorId,
        event.outcome
    )
}
function execute(db: DatabaseSync, query: ObserverQuery): ObserverQueryResult {
    return { rows: db.prepare(query.sql).all(...(query.params as (string | number | null)[])) as ObserverQueryResult["rows"], truncated: false }
}

test("queue wait averages admitted attempts per instance inside the window and rolls up per actor", () => {
    const db = database()
    try {
        append(db, { actorId: "lobby", queueWaitMs: 10 })
        append(db, { actorId: "lobby", queueWaitMs: 30, outcome: "failed" })
        append(db, { actorId: "lobby", queueWaitMs: 500, outcome: "rerouted" })
        append(db, { actorId: "lobby", queueWaitMs: null, outcome: "rejected" })
        append(db, { actorId: "lobby", queueWaitMs: 900, startedAtMs: 100 })
        append(db, { actorId: "random", queueWaitMs: 100 })
        append(db, { actorName: "Counter", actorId: "one", queueWaitMs: 4 })
        const rows = queueWaitRows(execute(db, queueWaitQuery({ fromMs: 1000 })))
        assert.deepEqual(rows, [
            { actorName: "Room", actorId: "lobby", admitted: 2, averageMs: 20, maxMs: 30 },
            { actorName: "Counter", actorId: "one", admitted: 1, averageMs: 4, maxMs: 4 },
            { actorName: "Room", actorId: "random", admitted: 1, averageMs: 100, maxMs: 100 }
        ])
        assert.deepEqual(
            queueWaitRows(execute(db, queueWaitQuery({ fromMs: 1000 }, "Room"))).map(row => row.actorId),
            ["lobby", "random"]
        )
        assert.deepEqual(
            [...queueWaitByActor(rows)],
            [
                ["Room", { admitted: 3, averageMs: 140 / 3, maxMs: 100 }],
                ["Counter", { admitted: 1, averageMs: 4, maxMs: 4 }]
            ]
        )
        assert.deepEqual([...queueWaitByInstance(rows, "Room").keys()], ["lobby", "random"])
        assert.deepEqual(
            queueWaitRows(execute(db, queueWaitQuery({ fromMs: 0, toMs: 200 }))).map(row => row.averageMs),
            [900],
            "an absolute range bounds both ends"
        )
        assert.equal(queueWaitByInstance(rows, "Room").get("lobby")!.averageMs, 20)
    } finally {
        db.close()
    }
})

test("queue wait rows are validated and formatted", () => {
    assert.throws(() => queueWaitRows({ rows: [{ actor_name: "Room", actor_id: "lobby", admitted: 0, average_ms: 1, max_ms: 1 }], truncated: false }))
    assert.throws(() => queueWaitRows({ rows: [{ actor_name: "Room", actor_id: "lobby", admitted: 1, average_ms: null, max_ms: 1 }], truncated: false }))
    assert.equal(formatWait(undefined), "—")
    assert.equal(formatWait(12.34), "12.3 ms")
    assert.equal(formatWait(1500), "1.5 s")
})
