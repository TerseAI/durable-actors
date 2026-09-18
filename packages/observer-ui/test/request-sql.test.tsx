import assert from "node:assert/strict"
import { DatabaseSync } from "node:sqlite"
import { test } from "node:test"

import type { ObserverQuery, ObserverQueryResult } from "../src/client.js"
import { historyPage, historyQuery } from "../src/request-sql.js"

function database() {
    const db = new DatabaseSync(":memory:")
    db.exec(`CREATE TABLE request_events (sequence INTEGER PRIMARY KEY, started_at_ms INTEGER, actor_id TEXT, outcome TEXT, event TEXT);
        CREATE TABLE request_history (generation TEXT, watermark INTEGER, pruned INTEGER, total INTEGER);
        INSERT INTO request_history VALUES ('one', 0, 0, 0);`)
    return db
}
function append(db: DatabaseSync, sequence: number, actorId = "lobby") {
    const event = {
        eventId: `event-${sequence}`,
        requestId: `request-${sequence}`,
        hostId: "host",
        sessionId: "session",
        actorType: "Room",
        actorId,
        kind: "method",
        operation: "post",
        connectionId: null,
        startedAtMs: 1000,
        durationMs: 25,
        queueWaitMs: 10,
        outcome: "completed"
    }
    db.prepare("INSERT INTO request_events VALUES (?, ?, ?, ?, ?)").run(sequence, 1000, actorId, "completed", JSON.stringify(event))
    db.prepare("UPDATE request_history SET watermark = ?, total = total + 1").run(sequence)
}
function execute(db: DatabaseSync, query: ObserverQuery): ObserverQueryResult {
    return { rows: db.prepare(query.sql).all(...(query.params as (string | number | null)[])) as ObserverQueryResult["rows"], truncated: false }
}

test("SQL history pagination freezes the read boundary and handles equal timestamps", () => {
    const db = database()
    try {
        for (let sequence = 1; sequence <= 203; sequence++) append(db, sequence)
        const first = historyPage(execute(db, historyQuery({})))
        assert.equal(first.records.length, 100)
        append(db, 204)
        const second = historyPage(execute(db, historyQuery({}, first.nextCursor!)), first.nextCursor!)
        const last = historyPage(execute(db, historyQuery({}, second.nextCursor!)), second.nextCursor!)
        assert.equal(last.nextCursor, null)
        assert.deepEqual(
            [...first.records, ...second.records, ...last.records].map(record => record.sequence),
            Array.from({ length: 203 }, (_, i) => 203 - i)
        )
    } finally {
        db.close()
    }
})

test("SQL filters bind values and empty history still carries retention metadata", () => {
    const db = database()
    try {
        append(db, 1)
        append(db, 2, "' OR 1=1 --")
        const filtered = historyPage(execute(db, historyQuery({ fromMs: 1000, toMs: 1000, actorId: "' OR 1=1 --", outcome: "completed" })))
        assert.equal(filtered.records.length, 1)
        assert.equal(filtered.records[0]!.sequence, 2)
        const empty = historyPage(execute(db, historyQuery({ fromMs: 2000 })))
        assert.equal(empty.records.length, 0)
        assert.equal(empty.epoch, "one")
        assert.equal(empty.nextCursor, null)
    } finally {
        db.close()
    }
})
