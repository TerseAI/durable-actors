import { access } from "node:fs/promises"
import type { Socket } from "node:net"
import { isAbsolute } from "node:path"
import { z } from "zod"

import { ActorSession, connectSocket, parseHostSettings } from "./actor-host.js"
import { ActorWorker, ActorWorkerSupervisor } from "./worker-supervisor.js"

async function runGenericHost(): Promise<never> {
    const worker = new ActorWorker()
    try {
        await worker.warm()
        const settings = parseHostSettings(process.env)
        const socket = await connectSocket(settings.socketPath)
        const assignment = readAssignment(socket)
        socket.write(`${JSON.stringify({ type: "warm", protocol: 16 })}\n`)
        const { entrypoint, actorIdleTimeoutMs } = await assignment
        await waitForCode(entrypoint)
        let available = true
        const session = new ActorSession(
            { ...settings, actorEntrypoint: entrypoint, actorIdleTimeoutMs },
            options =>
                new ActorWorkerSupervisor({
                    ...options,
                    createWorker: (data, onStateChange) => {
                        if (!available) return new ActorWorker(data, onStateChange)
                        available = false
                        worker.load(data, onStateChange)
                        return worker
                    }
                }),
            socket
        )
        await session.start()
        await session.waitUntilDisconnected()
        throw new Error("Rust host disconnected")
    } finally {
        worker.terminate("generic executor stopped")
    }
}

function readAssignment(socket: Socket): Promise<z.infer<typeof assignmentSchema>> {
    return new Promise((resolve, reject) => {
        let buffer = ""
        const fail = (error: Error) => {
            cleanup()
            socket.destroy()
            reject(error)
        }
        const closed = () => fail(new Error("Rust host disconnected before assignment"))
        const read = (chunk: Buffer) => {
            buffer += chunk.toString("utf8")
            if (Buffer.byteLength(buffer) > 8192) return fail(new Error("assignment is too large"))
            if (!buffer.includes("\n")) return
            try {
                const assignment = assignmentSchema.parse(JSON.parse(buffer))
                cleanup()
                resolve(assignment)
            } catch (error) {
                fail(error instanceof Error ? error : new Error(String(error)))
            }
        }
        const cleanup = () => {
            socket.off("data", read)
            socket.off("error", fail)
            socket.off("close", closed)
        }
        socket.on("data", read).once("error", fail).once("close", closed)
    })
}

async function waitForCode(entrypoint: string): Promise<void> {
    const deadline = Date.now() + 60_000
    while (true) {
        try {
            await access(entrypoint)
            return
        } catch (error) {
            if (Date.now() >= deadline) throw error
            await new Promise(resolve => setTimeout(resolve, 10))
        }
    }
}

const assignmentSchema = z.object({
    type: z.literal("load"),
    entrypoint: z.string().endsWith(".mjs").refine(isAbsolute, "entrypoint must be absolute"),
    actorIdleTimeoutMs: z.number().int().positive().max(86_400_000)
})

export { runGenericHost }
