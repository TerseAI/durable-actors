import assert from "node:assert/strict"
import { test } from "node:test"

import { requestSummary } from "../src/overview-data.js"
import type { RequestTrace } from "../src/client.js"

const trace = (durationMs: number, outcome: RequestTrace["outcome"]): RequestTrace => ({ sequence: 1, requestId: "request", hostId: "host", sessionId: "session", actorType: "Room", actorId: "one", kind: "method", operation: "post", connectionId: null, startedAtMs: 1000, durationMs, queueWaitMs: 0, outcome })

test("overview metrics use retained execution attempts, without counting reroutes as successful executions", () => {
    assert.deepEqual(requestSummary([]), { count: 0, success: null, p95: null })
    assert.deepEqual(requestSummary([trace(10, "completed"), trace(30, "failed"), trace(100, "rerouted")]), { count: 3, success: 50, p95: 30 })
})
