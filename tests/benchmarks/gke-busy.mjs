import assert from "node:assert/strict"
import { setTimeout as sleep } from "node:timers/promises"

const name = process.env.DURABLE_ACTORS_PROJECT_ID
const peers = Array.from({ length: 4 }, (_, index) => `http://${name}-load-${index}.${name}-load:8080`)
const connections = 32768
const startedAt = new Date().toISOString()
let stopped = false
let trafficError
let traffic = Promise.resolve()
const hosts = new Set()
let probes = 0

try {
    const started = Date.now()
    const admission = await all("/connect", { count: connections / 4, concurrency: 32 })
    const connectMs = Date.now() - started
    assert.equal((await call(0, "/inspect")).connections, connections)
    traffic = keepBusy().catch(error => {
        trafficError = error
    })
    const latencies = []
    const measured = Date.now()
    for (let sequence = 0; sequence < 25; sequence++) {
        const sent = Date.now()
        await call(0, "/broadcast", { sequence })
        while (true) {
            if (trafficError) throw trafficError
            const stats = await all("/stats")
            stats.forEach(worker => {
                assert.deepEqual(worker.failures, [])
                assert.equal(worker.live, connections / 4)
                assert.ok((worker.rounds[sequence]?.count ?? 0) <= connections / 4, "duplicate broadcast")
            })
            if (stats.every(worker => worker.rounds[sequence]?.count === connections / 4)) break
            assert.ok(Date.now() - sent < 30000, "full-room broadcast timed out")
            await sleep(10)
        }
        latencies.push(Date.now() - sent)
    }
    const measuredMs = Date.now() - measured
    stopped = true
    await traffic
    if (trafficError) throw trafficError
    assert.equal(hosts.size, 1, "sustained traffic must keep the actor sandbox active")
    latencies.sort((a, b) => a - b)
    console.log(
        JSON.stringify({
            phase: "busy_result",
            startedAt,
            time: new Date().toISOString(),
            connections,
            connectMs,
            admission,
            probes,
            actorHost: [...hosts][0],
            failures: [],
            broadcast: {
                rounds: 25,
                deliveries: connections * 25,
                measuredMs,
                deliveriesPerSecond: (connections * 25 * 1000) / measuredMs,
                p50Ms: latencies[12],
                p95Ms: latencies[23],
                maximumMs: latencies[24]
            }
        })
    )
} finally {
    stopped = true
    await traffic
    await all("/close")
}

async function keepBusy() {
    while (!stopped) {
        const probe = await call(3, "/probe")
        hosts.add(probe.currentHost)
        probes++
        await sleep(100)
    }
}

function all(path, body = {}) {
    return Promise.all(peers.map((_, index) => call(index, path, body)))
}

async function call(index, path, body = {}) {
    const response = await fetch(peers[index] + path, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body),
        signal: AbortSignal.timeout(900000)
    })
    const result = await response.json()
    assert.ok(response.ok, `${index} ${path}: ${JSON.stringify(result)}`)
    return result
}
