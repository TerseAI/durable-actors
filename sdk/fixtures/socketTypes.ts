import { z } from "zod"

import { Actor, Ephemeral, Persisted } from "../src/index.js"
import type { ActorMessageOf, ActorSocket, ActorSocketOf } from "../src/index.js"
import type { JsonObject } from "../src/json.js"

interface ChatroomMetadata {
    userId: string
}

const incoming = z.object({ type: z.literal("post"), text: z.string() })
const outgoing = z.object({ type: z.literal("posted"), text: z.string(), userId: z.string() })
const tag = z.enum(["member", "moderator"])
type Incoming = z.infer<typeof incoming>
type Outgoing = z.infer<typeof outgoing>

class MetadataRoom extends Actor<ChatroomMetadata> {
    async users(): Promise<string[]> {
        return (await this.getConnections()).map(socket => socket.metadata.userId)
    }
}

class TypedRoom extends Actor<ChatroomMetadata, Incoming, Outgoing, z.infer<typeof tag>> {
    static schemas = { metadata: z.object({ userId: z.string() }), incoming, outgoing, tag }
    @Persisted history: string[] = []
    @Ephemeral cache = new Map<string, number>()

    async onMessage(socket: ActorSocketOf<TypedRoom>, message: ActorMessageOf<TypedRoom>): Promise<void> {
        const response = { type: "posted" as const, text: message.text, userId: socket.metadata.userId }
        const connections = await this.getConnections()
        socket.send(response)
        this.broadcast(response, { except: socket })
        connections[0]?.send(response)
        socket.setTags("member", "moderator")
        this.broadcast(response, { tags: ["member"], except: socket })
        this.broadcast(response, { tags: ["member", "moderator"], tagMatch: "any" })
        this.broadcast(response, { tags: ["member", "moderator"], tagMatch: "all" })
        // @ts-expect-error Broadcast tag matching accepts only all or any.
        this.broadcast(response, { tagMatch: "either" })
        const tags: readonly ("member" | "moderator")[] = socket.tags
        void tags
        // @ts-expect-error Socket tags follow the actor's tag union.
        socket.setTags("admin")
        // @ts-expect-error Broadcast filters use the same tag union.
        this.broadcast(response, { tags: ["admin"] })
        // @ts-expect-error Connections preserve the tag union too.
        connections[0]?.setTags("admin")
        // @ts-expect-error Connection metadata follows the actor generic.
        socket.metadata = { userId: 123 }
        // @ts-expect-error Actor output must match its outgoing message type.
        this.broadcast({ type: "post", text: "wrong direction" })
        // @ts-expect-error Connections use the actor's outgoing message type.
        connections[0]?.send({ type: "post", text: "wrong direction" })
    }
}

async function checkReferences(): Promise<void> {
    await MetadataRoom.get("room").connect({ userId: "one" })
    // @ts-expect-error Metadata is typed even without an onConnect hook.
    await MetadataRoom.get("room").connect({ userId: 123 })
    const connection = await TypedRoom.get("room").connect({ userId: "one" })
    connection.send({ type: "post", text: "hello" })
    // @ts-expect-error Client sends must match the actor's incoming messages.
    connection.send({ type: "posted", text: "hello", userId: "one" })
    await TypedRoom.get("room").broadcast({ type: "posted", text: "hello", userId: "one" })
    // @ts-expect-error Reference broadcasts must match outgoing messages.
    await TypedRoom.get("room").broadcast({ type: "post", text: "hello" })
    connection.addEventListener("message", event => {
        if (event.data.type === "state") {
            const state: JsonObject = event.data.state
            // @ts-expect-error Snapshot fields require narrowing from JSON values.
            const history: string[] = state.history
            void [state, history]
        } else if (event.data.type === "state_update") {
            const changes: Partial<JsonObject> = event.data.changes
            void changes
        } else {
            const userId: string = event.data.userId
            // @ts-expect-error Received payload fields retain their declared types.
            const wrong: number = event.data.text
            void [userId, wrong]
        }
    })
}

class InvalidSchemaRoom extends Actor<ChatroomMetadata> {
    static schemas = { metadata: z.object({ userId: z.number() }) }
}

// @ts-expect-error The wire schema must agree with the declared metadata type.
const invalid = () => InvalidSchemaRoom.get("room")

class InvalidTagSchemaRoom extends Actor<ChatroomMetadata, Incoming, Outgoing, "member"> {
    static schemas = { tag: z.enum(["member", "admin"]) }
}

// @ts-expect-error The tag schema must agree with the actor's tag union.
const invalidTags = () => InvalidTagSchemaRoom.get("room")

class InvalidHookRoom extends Actor<ChatroomMetadata, Incoming, Outgoing> {
    // @ts-expect-error Lifecycle hook arguments must agree with actor generics.
    async onMessage(_socket: ActorSocket<number>, _message: number): Promise<void> {}
}

void [checkReferences, invalid, invalidTags, InvalidHookRoom]
