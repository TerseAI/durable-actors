import { Actor, type ActorSocket, Ephemeral, Persisted } from "durable-actors"
import { hostname } from "node:os"

export class SocketBench extends Actor {
    @Ephemeral instance = crypto.randomUUID()
    @Persisted count = 0

    async onConnect(socket: ActorSocket) {
        socket.metadata = { joined: this.instance }
        socket.setTags("bench")
        this.setWebSocketAutoResponse({ request: '"ping"', response: '"pong"' })
        socket.send({ type: "ready", instance: this.instance, host: hostname() })
    }

    async onMessage(socket: ActorSocket, message: { sequence: number; broadcast: boolean; burst?: boolean }) {
        if (message.burst) {
            for (let index = 0; index < 64; index++) socket.send({ sequence: message.sequence, index, padding: "x".repeat(256 * 1024) })
            return
        }
        const reply = { sequence: message.sequence, instance: this.instance, host: hostname(), metadata: socket.metadata, tags: socket.tags }
        if (message.broadcast) {
            this.count++
            this.broadcast({ ...reply, broadcast: true, count: this.count })
        } else socket.send(reply)
    }

    async inspect() {
        return { connections: await this.getConnectionCount(), instance: this.instance, host: hostname(), pid: process.pid }
    }

    async listConnections() {
        const connections = await this.getConnections("bench")
        return { connections: connections.length, allTagged: connections.every(socket => socket.tags.includes("bench")), allAttached: connections.every(socket => socket.metadata !== null) }
    }
}
