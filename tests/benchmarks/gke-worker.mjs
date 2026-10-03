import assert from "node:assert/strict"
import { createServer } from "node:http"
import { setTimeout as sleep } from "node:timers/promises"

import { RemoteActorClient } from "../../sdk/dist/client/remoteClient.js"
import WebSocket from "../../sdk/node_modules/ws/wrapper.mjs"

const origin = process.env.DURABLE_ACTORS_CONTROL_PLANE_URL
const project = process.env.DURABLE_ACTORS_PROJECT_ID
const actor = process.env.BENCH_ACTOR_ID
const key = process.env.DURABLE_ACTORS_SECRET
const client = new RemoteActorClient({ controlPlaneUrl: origin, projectId: project, apiKey: key }, { telemetry() {} })
const sockets = new Set()
const rounds = new Map()
const pending = new WeakMap()
const expectedCloses = new WeakSet()
const failures = []
let grant
let sequence = -10
const initial = new WeakMap()
const heartbeats = new WeakMap()
let firstInstance
let connectionsCreated = 0
let stopping = false

const server = createServer(async (request, response) => {
    try {
        const chunks = []
        for await (const chunk of request) chunks.push(chunk)
        const body = chunks.length ? JSON.parse(Buffer.concat(chunks)) : {}
        const result = await command(request.url, body)
        response.writeHead(200, { "content-type": "application/json" })
        response.end(JSON.stringify(result))
    } catch (error) {
        console.error(error)
        response.writeHead(500, { "content-type": "application/json" })
        response.end(JSON.stringify({ error: "benchmark command failed; inspect load pod logs" }))
    }
})
server.requestTimeout = 0
server.listen(8080, "0.0.0.0")

async function command(path, body) {
    if (path === "/connect") return connectMany(body.count, body.concurrency ?? 8)
    if (path === "/reject-extra") {
        const access = await issueGrant()
        return new Promise((resolve, reject) => {
            const socket = new WebSocket(access.websocketUrl)
            const timer = setTimeout(() => {
                socket.terminate()
                reject(new Error("extra socket was not rejected"))
            }, 30000)
            socket.addEventListener("close", event => {
                clearTimeout(timer)
                resolve({ code: event.code, reason: event.reason })
            })
            socket.addEventListener("error", event => {
                clearTimeout(timer)
                reject(new Error(event.message))
            })
        })
    }
    if (path === "/inspect") return client.invoke("SocketBench", actor, "inspect", [])
    if (path === "/list") return client.invoke("SocketBench", actor, "listConnections", [])
    if (path === "/slow-read") {
        const socket = sockets.values().next().value
        const id = sequence--
        let received = 0
        const completed = new Promise((resolve, reject) => {
            const timer = setTimeout(() => reject(new Error("slow reader did not drain")), 60000)
            pending.set(socket, message => {
                if (message.sequence !== id) return
                assert.equal(message.index, received++)
                assert.equal(message.padding.length, 256 * 1024)
                if (received === 64) {
                    clearTimeout(timer)
                    pending.delete(socket)
                    resolve()
                }
            })
        })
        socket.pause()
        socket.send(JSON.stringify({ sequence: id, broadcast: false, burst: true }))
        await sleep(8000)
        socket.resume()
        await completed
        return { pausedMs: 8000, messages: received, bytes: received * 256 * 1024 }
    }
    if (path === "/observer") {
        const response = await fetch(`${origin}/v1/projects/${project}/observe/actors`, { headers: { authorization: `Bearer ${key}` } })
        assert.ok(response.ok, `observer: ${response.status} ${response.ok ? "" : await response.text()}`)
        const inventory = await response.json()
        const instance = inventory.actors.find(row => row.actorName === "SocketBench").instances.find(row => row.actorId === actor)
        return { connections: instance.connections.length, status: instance.status }
    }
    if (path === "/heartbeat") {
        const socket = sockets.values().next().value
        const started = performance.now()
        await new Promise((resolve, reject) => {
            const timeout = setTimeout(() => reject(new Error("automatic response timed out")), 5000)
            heartbeats.set(socket, () => {
                clearTimeout(timeout)
                resolve()
            })
            socket.send('"ping"')
        })
        return { milliseconds: performance.now() - started }
    }
    if (path === "/echo") return echo(body.seconds ?? 5, body.concurrency ?? 8)
    if (path === "/probe") {
        const reply = await exchange(sockets.values().next().value)
        const original = initial.get(sockets.values().next().value)
        assert.deepEqual(reply.metadata, { joined: original.instance })
        assert.deepEqual(reply.tags, ["bench"])
        return { firstInstance, currentInstance: reply.instance, originalHost: original.host, currentHost: reply.host, attachmentsRetained: true }
    }
    if (path === "/broadcast") {
        sockets
            .values()
            .next()
            .value.send(JSON.stringify({ sequence: body.sequence, broadcast: true, sentAt: Date.now() }))
        return {}
    }
    if (path === "/stats") return { live: sockets.size, connectionsCreated, failures, rounds: Object.fromEntries(rounds) }
    if (path === "/reconnect") {
        const selected = [...sockets].slice(0, body.count)
        await Promise.all(selected.map(close))
        return connectMany(selected.length, body.concurrency ?? 64)
    }
    if (path === "/close") {
        stopping = true
        await Promise.all([...sockets].map(close))
        return { live: sockets.size }
    }
    throw new Error(`unknown command ${path}`)
}

async function connectMany(count, concurrency) {
    const start = performance.now()
    for (let offset = 0; offset < count; offset += concurrency) {
        assert.ok(!stopping, "benchmark stopping")
        if (!grant || grant.connectByMs - Date.now() < 10000) grant = await issueGrant()
        await Promise.all(Array.from({ length: Math.min(concurrency, count - offset) }, () => connect(grant.websocketUrl)))
        assert.equal(failures.length, 0, JSON.stringify(failures.slice(-10)))
    }
    return { live: sockets.size, milliseconds: performance.now() - start }
}

async function issueGrant() {
    const response = await fetch(`${origin}/v1/projects/${project}/actors/SocketBench/${actor}/find-websocket`, {
        method: "POST",
        headers: { authorization: `Bearer ${key}`, "content-type": "application/json" },
        body: JSON.stringify({ metadata: null, authorizationLifetimeMs: 3600000 }),
        signal: AbortSignal.timeout(180000)
    })
    assert.ok(response.ok, `grant: ${response.status} ${response.ok ? "" : await response.text()}`)
    return response.json()
}

async function connect(url) {
    const socket = new WebSocket(url)
    sockets.add(socket)
    return new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error("connection handler readiness timed out")), 180000)
        socket.addEventListener("message", event => {
            const message = JSON.parse(event.data)
            if (message === "pong") {
                heartbeats.get(socket)?.()
                return
            }
            if (message.type === "ready") {
                initial.set(socket, message)
                clearTimeout(timer)
                firstInstance ??= message.instance
                connectionsCreated++
                resolve()
            } else if (message.broadcast) {
                const round = rounds.get(message.sequence) ?? { count: 0, firstAt: Date.now(), lastAt: 0 }
                round.count++
                round.lastAt = Date.now()
                rounds.set(message.sequence, round)
            } else if (message.sequence !== undefined) pending.get(socket)?.(message)
        })
        socket.addEventListener("close", event => {
            sockets.delete(socket)
            clearTimeout(timer)
            if (!expectedCloses.has(socket)) {
                failures.push({ code: event.code, reason: event.reason })
                reject(new Error(`unexpected close ${event.code}: ${event.reason}`))
            }
        })
        socket.addEventListener("error", event => {
            clearTimeout(timer)
            failures.push({ error: event.message ?? "transport error" })
            reject(new Error("transport error"))
        })
    })
}

async function exchange(socket) {
    const id = sequence--
    return new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
            pending.delete(socket)
            reject(new Error("echo timed out"))
        }, 30000)
        pending.set(socket, message => {
            if (message.sequence !== id) return
            clearTimeout(timer)
            pending.delete(socket)
            resolve(message)
        })
        socket.send(JSON.stringify({ sequence: id, broadcast: false }))
    })
}

async function echo(seconds, concurrency) {
    const samples = []
    const start = performance.now()
    await Promise.all(
        [...sockets].slice(0, concurrency).map(async socket => {
            while (performance.now() - start < seconds * 1000) {
                const sent = performance.now()
                await exchange(socket)
                samples.push(performance.now() - sent)
            }
        })
    )
    samples.sort((a, b) => a - b)
    return {
        messages: samples.length,
        perSecond: (samples.length * 1000) / (performance.now() - start),
        p95Ms: samples[Math.floor(samples.length * 0.95)],
        p99Ms: samples[Math.floor(samples.length * 0.99)]
    }
}

async function close(socket) {
    expectedCloses.add(socket)
    const closed = new Promise(resolve => socket.addEventListener("close", resolve, { once: true }))
    socket.close()
    await Promise.race([
        closed,
        sleep(15000).then(() => {
            if (sockets.has(socket)) throw new Error("close timed out")
        })
    ])
}
