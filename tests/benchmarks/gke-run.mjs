import assert from "node:assert/strict"
import { setTimeout as sleep } from "node:timers/promises"

const name = process.env.DURABLE_ACTORS_PROJECT_ID
const peers = Array.from({ length: 4 }, (_, index) => `http://${name}-load-${index}.${name}-load:8080`)
const report = {
    startedAt: new Date().toISOString(),
    environment: {
        cluster: "terse-actors",
        region: "us-west4",
        transport: "GKE load pods → TLS nginx → Rust gateway → gVisor actor host",
        actorCpuMillis: Number(process.env.BENCH_ACTOR_CPU_MILLIS ?? 2000),
        actorMemoryMiB: 2048,
        gateways: 2,
        generators: 4
    },
    phases: []
}
let sequence = 0
let connected = 0
try {
    for (const target of [128, 1024, 8192, 32768]) {
        const start = Date.now()
        const admission = await all("/connect", { count: (target - connected) / 4, concurrency: 32 })
        connected = target
        const connectMs = Date.now() - start
        const inventory = await call(0, "/inspect")
        assert.equal(inventory.connections, target)
        log({ phase: "connected", target, milliseconds: Date.now() - start, admission })
        const listed = await call(0, "/list")
        assert.deepEqual(listed, { connections: target, allTagged: true, allAttached: true })
        log({ phase: "idle", target, host: inventory.host })
        await sleep(20000)
        const heartbeat = await all("/heartbeat")
        log({ phase: "automatic_response", target, host: inventory.host, heartbeat })
        const observer = await call(0, "/observer")
        assert.equal(observer.connections, target)
        await sleep(2000)
        const probe = await call(0, "/probe")
        assert.notEqual(probe.currentHost, inventory.host, "the sandbox stops without closing its sockets")
        log({ phase: "hibernation", target, probe, observer })
        const slowReader = target === 128 ? await call(0, "/slow-read") : undefined
        if (slowReader) {
            log({ phase: "slow_reader", ...slowReader })
            await call(0, "/probe")
        }
        const echo = await all("/echo", { seconds: 10, concurrency: 8 })
        const broadcast = await broadcasts(target, 25)
        const stats = await all("/stats")
        stats.forEach(s => {
            assert.deepEqual(s.failures, [])
            assert.equal(s.live, target / 4)
        })
        const phase = {
            connections: target,
            connectMs,
            admission,
            sandboxReplacedWithLiveSockets: true,
            probe,
            heartbeat,
            observer,
            slowReader,
            echo,
            broadcast,
            failures: stats.flatMap(s => s.failures)
        }
        report.phases.push(phase)
        log({ phase: "measured", ...phase })
        await sleep(10000)
    }
    const extra = await call(0, "/reject-extra")
    assert.equal(extra.code, 1013)
    report.extraConnection = extra
    const reconnect = await all("/reconnect", { count: 819, concurrency: 128 })
    await sleep(1000)
    assert.equal((await call(0, "/inspect")).connections, 32768)
    const broadcast = await broadcasts(32768, 25)
    const stats = await all("/stats")
    stats.forEach(s => {
        assert.deepEqual(s.failures, [])
        assert.equal(s.live, 8192)
    })
    report.reconnect = { connections: 3276, concurrency: 512, workers: reconnect, broadcast }
    log({ phase: "result", ...report })
} catch (error) {
    console.error(JSON.stringify({ phase: "failed", error: String(error), report, workers: await all("/stats").catch(() => []) }))
    process.exitCode = 1
} finally {
    if (!process.env.BENCH_KEEP_OPEN) await all("/close").catch(error => console.error(String(error)))
}

async function broadcasts(target, rounds) {
    const durations = []
    const start = Date.now()
    for (let index = 0; index < rounds; index++) {
        const id = sequence++
        const sent = Date.now()
        await call(0, "/broadcast", { sequence: id })
        while (true) {
            const stats = await all("/stats")
            for (const shard of stats) {
                assert.deepEqual(shard.failures, [])
                assert.ok((shard.rounds[id]?.count ?? 0) <= target / 4, "duplicate broadcast")
            }
            if (stats.every(s => s.rounds[id]?.count === target / 4)) break
            assert.ok(Date.now() - sent < 30000, "full-room broadcast timed out")
            await sleep(10)
        }
        durations.push(Date.now() - sent)
    }
    durations.sort((a, b) => a - b)
    return {
        rounds,
        deliveries: target * rounds,
        deliveriesPerSecond: (target * rounds * 1000) / (Date.now() - start),
        p50Ms: durations[Math.floor(rounds * 0.5)],
        p95Ms: durations[Math.floor(rounds * 0.95)],
        maximumMs: durations.at(-1),
        measurement: "coordinator completion, includes 10ms polling and HTTP control overhead"
    }
}

function all(path, body = {}) {
    return Promise.all(peers.map((_, index) => call(index, path, body)))
}
async function call(index, path, body = {}) {
    const response = await fetch(peers[index] + path, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body), signal: AbortSignal.timeout(900000) })
    const result = await response.json()
    assert.ok(response.ok, `${index} ${path}: ${JSON.stringify(result)}`)
    return result
}

function log(value) {
    console.log(JSON.stringify({ time: new Date().toISOString(), ...value }))
}
