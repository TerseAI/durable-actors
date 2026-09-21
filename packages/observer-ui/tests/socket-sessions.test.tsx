import assert from "node:assert/strict"
import { DatabaseSync } from "node:sqlite"
import { test } from "node:test"

import type { ActorInventory, ObserverQuery, ObserverQueryResult, RequestTrace } from "../src/client.js"
import { formatDuration, metadataSummary, sessionDuration, sessionRows, sessionSummary, sessionsQuery, socketSessions } from "../src/socket-sessions.js"

function database() {
    const db = new DatabaseSync(":memory:")
    db.exec(`CREATE TABLE traces (position INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL UNIQUE, event TEXT NOT NULL,
            started_at_ms INTEGER NOT NULL DEFAULT 0, actor_name TEXT NOT NULL DEFAULT '', actor_id TEXT NOT NULL DEFAULT '', outcome TEXT NOT NULL DEFAULT '');
        CREATE VIEW request_events AS SELECT position AS sequence, event_id, started_at_ms, actor_name, actor_id, outcome, event,
            json_extract(event, '$.requestId') AS request_id,
            json_extract(event, '$.hostId') AS host_id,
            json_extract(event, '$.sessionId') AS session_id,
            json_extract(event, '$.kind') AS kind,
            json_extract(event, '$.operation') AS operation,
            json_extract(event, '$.connectionId') AS connection_id,
            json_extract(event, '$.durationMs') AS duration_ms,
            json_extract(event, '$.queueWaitMs') AS queue_wait_ms
        FROM traces;`)
    return db
}
let counter = 0
function append(db: DatabaseSync, trace: Partial<RequestTrace> & { metadata?: unknown }) {
    counter++
    const event = {
        eventId: `event-${counter}`,
        requestId: `request-${counter}`,
        hostId: "host-1",
        sessionId: "session",
        actorName: "Room",
        actorId: "lobby",
        kind: "websocket",
        operation: "onMessage",
        connectionId: "c1",
        startedAtMs: 1000,
        durationMs: 5,
        queueWaitMs: 1,
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

test("sessions pair connect and disconnect events per connection and count messages", () => {
    const db = database()
    try {
        append(db, { operation: "onConnect", connectionId: "c1", startedAtMs: 1000, metadata: { name: "Ada", role: "moderator" } })
        append(db, { operation: "onMessage", connectionId: "c1", startedAtMs: 1500 })
        append(db, { operation: "onMessage", connectionId: "c1", startedAtMs: 1800, outcome: "failed" })
        append(db, { operation: "onDisconnect", connectionId: "c1", startedAtMs: 4000 })
        append(db, { operation: "onConnect", connectionId: "c2", actorId: "random", startedAtMs: 6000, hostId: "host-2" })
        append(db, { operation: "onMessage", connectionId: "c3", startedAtMs: 500 })
        append(db, { kind: "method", operation: "post", connectionId: null, startedAtMs: 9000 })
        const rows = sessionRows(execute(db, sessionsQuery()))
        assert.deepEqual(rows, [
            { connectionId: "c2", actorName: "Room", actorId: "random", hostId: "host-2", openedAtMs: 6000, closedAtMs: null, lastSeenMs: 6000, messages: 0, failures: 0, metadata: undefined },
            {
                connectionId: "c1",
                actorName: "Room",
                actorId: "lobby",
                hostId: "host-1",
                openedAtMs: 1000,
                closedAtMs: 4000,
                lastSeenMs: 4000,
                messages: 2,
                failures: 1,
                metadata: { name: "Ada", role: "moderator" }
            },
            { connectionId: "c3", actorName: "Room", actorId: "lobby", hostId: "host-1", openedAtMs: null, closedAtMs: null, lastSeenMs: 500, messages: 1, failures: 0, metadata: undefined }
        ])
        assert.deepEqual(
            sessionRows(execute(db, sessionsQuery({ fromMs: 4000 }))).map(row => row.connectionId),
            ["c2", "c1"],
            "the window keeps sessions with any activity inside it and still pairs their earlier events"
        )
        assert.deepEqual(execute(db, sessionsQuery({ fromMs: 4001 })).rows.length, 1)
        assert.deepEqual(
            sessionRows(execute(db, sessionsQuery({ fromMs: 1000, toMs: 5000 }))).map(row => row.connectionId),
            ["c1"],
            "an absolute range keeps sessions that overlap it"
        )
    } finally {
        db.close()
    }
})

test("live inventory decides whether unfinished sessions are open or lost and adds unrecorded connections", () => {
    const inventory: ActorInventory = {
        actors: [
            {
                actorName: "Room",
                live: 1,
                dormant: 0,
                unknown: 0,
                instances: [
                    {
                        actorId: "random",
                        status: "live",
                        connections: [
                            { id: "c2", metadata: { name: "Ada" } },
                            { id: "c9", metadata: null }
                        ]
                    }
                ]
            }
        ]
    }
    const rows = [
        { connectionId: "c2", actorName: "Room", actorId: "random", hostId: "host-2", openedAtMs: 6000, closedAtMs: null, lastSeenMs: 6000, messages: 0, failures: 0, metadata: { name: "Stale" } },
        { connectionId: "c1", actorName: "Room", actorId: "lobby", hostId: "host-1", openedAtMs: 1000, closedAtMs: 4000, lastSeenMs: 4000, messages: 2, failures: 1, metadata: { name: "Grace" } },
        { connectionId: "c0", actorName: "Room", actorId: "lobby", hostId: "host-1", openedAtMs: 100, closedAtMs: null, lastSeenMs: 300, messages: 1, failures: 0 }
    ]
    const sessions = socketSessions(rows, inventory)
    assert.deepEqual(
        sessions.map(session => [session.connectionId, session.status, session.metadata]),
        [
            ["c9", "open", null],
            ["c2", "open", { name: "Ada" }],
            ["c1", "closed", { name: "Grace" }],
            ["c0", "lost", undefined]
        ],
        "live inventory metadata wins for open connections; saved connect metadata serves closed ones"
    )
    assert.deepEqual(sessionDuration(sessions[0]!, 10_000), null, "a connection without a retained connect event has no duration")
    assert.deepEqual(sessionDuration(sessions[1]!, 10_000), { ms: 4000, lowerBound: false })
    assert.deepEqual(sessionDuration(sessions[2]!, 10_000), { ms: 3000, lowerBound: false })
    assert.deepEqual(sessionDuration(sessions[3]!, 10_000), { ms: 200, lowerBound: true })
    assert.deepEqual(
        socketSessions(rows, undefined).map(session => session.status),
        ["lost", "closed", "lost"]
    )
    const summary = sessionSummary(sessions, 10_000)
    assert.deepEqual(summary, { total: 4, open: 2, lost: 1, median: 3000, p95: 4000, messages: 3, messagesPerSession: 0.75 })
    assert.deepEqual(sessionSummary([], 10_000), { total: 0, open: 0, lost: 0, median: null, p95: null, messages: 0, messagesPerSession: null })
})

test("invalid session rows are rejected and durations format by magnitude", () => {
    assert.throws(() =>
        sessionRows({
            rows: [{ connection_id: "c1", actor_name: "Room", actor_id: "lobby", host_id: null, opened_at_ms: null, closed_at_ms: null, last_seen_ms: null, messages: 0, failures: 0 }],
            truncated: false
        })
    )
    assert.throws(() =>
        sessionRows({
            rows: [{ connection_id: 5, actor_name: "Room", actor_id: "lobby", host_id: null, opened_at_ms: 1, closed_at_ms: null, last_seen_ms: 1, messages: 0, failures: 0 }],
            truncated: false
        })
    )
    assert.equal(formatDuration(820), "820 ms")
    assert.equal(formatDuration(4210), "4.2 s")
    assert.equal(formatDuration(192_000), "3m 12s")
    assert.equal(formatDuration(2 * 3_600_000 + 5 * 60_000), "2h 5m")
    assert.equal(formatDuration(27 * 3_600_000), "1d 3h")
    assert.equal(metadataSummary({ name: "Ada", role: "moderator", team: "core", seat: 4 }), "name: Ada · role: moderator · team: core · +1")
    assert.equal(metadataSummary({ user: { id: 7 } }), 'user: {"id":7}')
    assert.equal(metadataSummary("guest-token"), "guest-token")
    assert.equal(metadataSummary(null), null)
    assert.equal(metadataSummary({}), null)
    assert.equal(metadataSummary(undefined), null)
})
