import assert from "node:assert/strict"
import { execFileSync, spawn } from "node:child_process"
import { once } from "node:events"
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises"
import { arch, cpus, platform, tmpdir } from "node:os"
import { join, resolve } from "node:path"
import { createInterface } from "node:readline"
import { setTimeout as sleep } from "node:timers/promises"
import { fileURLToPath } from "node:url"
import { parseArgs } from "node:util"

import { RemoteActorClient } from "../../sdk/dist/client/remoteClient.js"

const root = fileURLToPath(new URL("../../", import.meta.url))
const { values } = parseArgs({
    options: {
        connections: { type: "string", default: "1024" },
        seconds: { type: "string", default: "10" },
        rounds: { type: "string", default: "50" },
        url: { type: "string" }
    }
})
const count = positive(values.connections)
const seconds = positive(values.seconds)
const rounds = positive(values.rounds)
const sockets = []
const latencies = []
let failures = 0
const closeReasons = new Map()
let closing = false
const runtime = values.url ? undefined : await startRuntime()
const client = new RemoteActorClient(runtime ? { controlPlaneUrl: runtime.origin, projectId: "local", apiKey: "benchmark-key" } : { controlPlaneUrl: values.url }, { telemetry() {} })
const actorId = `bench-${Date.now()}`
try {
    await client.invoke("SocketBench", actorId, "inspect", [])
    const baseline = memory(runtime?.child.pid)
    const started = performance.now()
    let attached
    for (let offset = 0; offset < count; offset += 16) {
        await Promise.all(
            Array.from({ length: Math.min(16, count - offset) }, async () => {
                const socket = await client.connect("SocketBench", actorId, null)
                socket.addEventListener("error", () => failures++)
                socket.addEventListener("close", event => {
                    if (!closing) {
                        failures++
                        const reason = `${event.code}: ${event.reason}`
                        closeReasons.set(reason, (closeReasons.get(reason) ?? 0) + 1)
                    }
                })
                sockets.push(socket)
            })
        )
        do {
            attached = await client.invoke("SocketBench", actorId, "inspect", [])
            assert.equal(failures, 0, `connections failed during admission: ${JSON.stringify([...closeReasons])}`)
            assert.ok(performance.now() - started < 180000, "connection admission timed out")
            if (attached.connections !== sockets.length) await sleep(10)
        } while (attached.connections !== sockets.length)
    }
    const connectMs = performance.now() - started
    await sleep(1500)
    const idle = memory(runtime?.child.pid)
    const afterIdle = await exchange(sockets[0], { sequence: -1, broadcast: false })
    if (runtime) assert.notEqual(afterIdle.instance, attached.instance, "actor evicts while sockets remain open")
    const echoStarted = performance.now()
    let sequence = 0
    await Promise.all(
        sockets.slice(0, 16).map(async socket => {
            while (performance.now() - echoStarted < seconds * 1000) {
                const sent = performance.now()
                await exchange(socket, { sequence: sequence++, broadcast: false })
                latencies.push(performance.now() - sent)
            }
        })
    )
    const echoMs = performance.now() - echoStarted
    const broadcastLatencies = []
    const broadcastStarted = performance.now()
    for (let i = 0; i < rounds; i++) {
        const message = { sequence: sequence++, broadcast: true }
        const received = sockets.map(socket => receive(socket, message.sequence))
        const sent = performance.now()
        sockets[i % sockets.length].send(message)
        await Promise.all(received)
        broadcastLatencies.push(performance.now() - sent)
    }
    const broadcastMs = performance.now() - broadcastStarted
    assert.equal(failures, 0, "socket errors or unexpected closes")
    console.log(
        JSON.stringify(
            {
                environment: {
                    platform: platform(),
                    arch: arch(),
                    cpu: cpus()[0]?.model,
                    node: process.version,
                    runtime: runtime?.binary,
                    transport: runtime ? "local connection gateway, no TLS" : values.url
                },
                connections: count,
                failures,
                connectMs: Math.round(connectMs),
                rssKiB: { baseline, idle, active: memory(runtime?.child.pid), idleDeltaPerConnection: baseline && idle ? (idle - baseline) / count : null },
                echo: { messages: latencies.length, perSecond: (latencies.length * 1000) / echoMs, ...percentiles(latencies) },
                broadcast: { rounds, deliveries: rounds * count, deliveriesPerSecond: (rounds * count * 1000) / broadcastMs, ...percentiles(broadcastLatencies) }
            },
            null,
            2
        )
    )
} finally {
    closing = true
    for (const socket of sockets) socket.close()
    if (runtime) {
        runtime.child.stdin.end()
        const timer = setTimeout(() => runtime.child.kill("SIGKILL"), 10000)
        if (runtime.child.exitCode === null && runtime.child.signalCode === null) await once(runtime.child, "exit")
        clearTimeout(timer)
        await rm(runtime.project, { recursive: true, force: true })
    }
}

async function startRuntime() {
    const project = await mkdtemp(join(tmpdir(), "terse-socket-bench-"))
    const source = await readFile(new URL("actors.ts", import.meta.url), "utf8")
    await writeFile(join(project, "actors.ts"), source.replace('"durable-actors"', JSON.stringify(join(root, "sdk/dist/index.js"))))
    await writeFile(
        join(project, "tsconfig.json"),
        JSON.stringify({
            compilerOptions: {
                target: "ES2022",
                module: "NodeNext",
                moduleResolution: "NodeNext",
                strict: true,
                skipLibCheck: true,
                types: ["node"],
                typeRoots: [join(root, "sdk/node_modules/@types")]
            },
            include: ["actors.ts"]
        })
    )
    const binary = resolve(process.env.DURABLE_ACTORS_TEST_RUNTIME ?? join(root, "target/debug/durable-actors"))
    const child = spawn(
        binary,
        ["dev", "--port", "0", "--project", project, "--entrypoint", "actors.ts", "--sdk-host", join(root, "sdk/dist/host.js"), "--project-id", "local", "--api-key", "benchmark-key"],
        {
            env: {
                ...process.env,
                DURABLE_ACTORS_PARENT_LIFETIME_STDIN: "1",
                DURABLE_ACTORS_HOST_IDLE_TIMEOUT_MS: "500",
                DURABLE_ACTORS_SOCKET_MAX_CONNECTIONS: process.env.DURABLE_ACTORS_SOCKET_MAX_CONNECTIONS ?? String(count),
                RUST_LOG: "warn"
            },
            stdio: ["pipe", "pipe", "inherit"]
        }
    )
    const lines = createInterface({ input: child.stdout })
    try {
        const origin = await new Promise((resolveOrigin, reject) => {
            const timer = setTimeout(() => reject(new Error("runtime startup timed out")), 60000)
            child.once("error", reject)
            child.once("exit", code => {
                clearTimeout(timer)
                reject(new Error(`runtime exited: ${code}`))
            })
            lines.on("line", line => {
                const match = line.match(/http:\/\/127\.0\.0\.1:\d+/)
                if (match) {
                    clearTimeout(timer)
                    resolveOrigin(match[0])
                }
            })
        })
        return { child, project, origin, binary }
    } catch (error) {
        child.kill()
        await rm(project, { recursive: true, force: true })
        throw error
    }
}

async function exchange(socket, message) {
    const reply = receive(socket, message.sequence)
    socket.send(message)
    return reply
}

function receive(socket, sequence) {
    return new Promise((resolveMessage, reject) => {
        const timer = setTimeout(() => finish(new Error(`message ${sequence} timed out`)), 30000)
        const message = event => {
            if (event.data.sequence === sequence) finish(undefined, event.data)
        }
        const closed = () => finish(new Error("socket closed while waiting for a message"))
        function finish(error, value) {
            clearTimeout(timer)
            socket.removeEventListener("message", message)
            socket.removeEventListener("close", closed)
            if (error) reject(error)
            else resolveMessage(value)
        }
        socket.addEventListener("message", message)
        socket.addEventListener("close", closed)
    })
}

function memory(pid) {
    if (!pid || !["darwin", "linux"].includes(platform())) return null
    const processes = execFileSync("ps", ["-axo", "pid=,ppid=,rss="], { encoding: "utf8" })
        .trim()
        .split("\n")
        .map(line => line.trim().split(/\s+/).map(Number))
    const descendants = new Set([pid])
    let previous
    do {
        previous = descendants.size
        for (const [id, parent] of processes) if (descendants.has(parent)) descendants.add(id)
    } while (previous !== descendants.size)
    return processes.filter(([id]) => descendants.has(id)).reduce((sum, [, , rss]) => sum + rss, 0)
}

function percentiles(samples) {
    samples.sort((a, b) => a - b)
    return Object.fromEntries([50, 95, 99].map(p => [`p${p}Ms`, samples[Math.min(samples.length - 1, Math.floor((samples.length * p) / 100))]]))
}

function positive(value) {
    const number = Number(value)
    assert.ok(Number.isSafeInteger(number) && number > 0, "benchmark parameters must be positive integers")
    return number
}
