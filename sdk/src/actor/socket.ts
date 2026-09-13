import { ActorProtocolError } from "../errors.js"
import type { JsonValue } from "../json.js"

import type { SocketConnection, SocketEffect, SocketMessage } from "./socketProtocol.js"
import { incomingMessage, outgoingMessage, socketMetadata, socketTags } from "./socketValidation.js"
import type { ActorSchemas, ActorStateMessage, ActorStateUpdate } from "./socketValidation.js"

type ActorSocketState = "connecting" | "open" | "closed"
type ActorSocketMessage = JsonValue

interface ActorSocket<Metadata = JsonValue, Outgoing = JsonValue, Tag extends string = string> {
    readonly id: string
    metadata: Metadata
    readonly tags: readonly Tag[]
    readonly state: ActorSocketState
    send(message: Outgoing): void
    close(code?: number, reason?: string): void
    reject(code?: number, reason?: string): void
    setTags(...tags: Tag[]): void
}

interface ActorBroadcastOptions<Tag extends string = string> {
    readonly except?: Pick<ActorSocket, "id"> | readonly Pick<ActorSocket, "id">[]
    readonly tags?: readonly Tag[]
    readonly tagMatch?: "all" | "any"
}

interface ActorConnection<Send = JsonValue, Receive = Send, State = JsonValue> {
    readonly readyState: number
    send(data: Send): void
    close(code?: number, reason?: string): void
    addEventListener<Type extends keyof ActorConnectionEventMap<Receive, State>>(
        type: Type,
        listener: (event: ActorConnectionEventMap<Receive, State>[Type]) => void
    ): void
    removeEventListener<Type extends keyof ActorConnectionEventMap<Receive, State>>(
        type: Type,
        listener: (event: ActorConnectionEventMap<Receive, State>[Type]) => void
    ): void
}

interface ActorConnectionEventMap<Receive = JsonValue, State = JsonValue> {
    readonly open: { readonly type: "open" }
    readonly message: {
        readonly type: "message"
        readonly data: Receive | ActorStateMessage<State> | ActorStateUpdate<State>
    }
    readonly close: {
        readonly type: "close"
        readonly code: number
        readonly reason: string
        readonly wasClean: boolean
    }
    readonly error: { readonly type: "error" }
}

const scopes = new WeakMap<object, ActorSocketScope>()

function actorConnections<Metadata, Outgoing, Tag extends string>(
    instance: object
): readonly ActorSocket<Metadata, Outgoing, Tag>[] {
    return socketScope(instance).sockets as unknown as readonly ActorSocket<Metadata, Outgoing, Tag>[]
}

function broadcastActor(instance: object, message: unknown, options?: ActorBroadcastOptions): void {
    socketScope(instance).broadcast(message, options)
}

async function runWithActorSockets<T>(
    instance: object,
    connections: readonly SocketConnection[],
    operation: (scope: ActorSocketScope) => Promise<T>,
    publish?: (effects: readonly SocketEffect[]) => Promise<void>,
    schemas: ActorSchemas = {}
): Promise<{ readonly value: T; readonly effects: readonly SocketEffect[] }> {
    const effects: SocketEffect[] = []
    const output = publish === undefined ? undefined : new SocketOutput(publish)
    const scope = new ActorSocketScope(connections, output ?? effects, schemas)
    scopes.set(instance, scope)
    try {
        return { value: await operation(scope), effects }
    } finally {
        scopes.delete(instance)
        await output?.flush()
    }
}

class ActorSocketScope {
    readonly sockets: readonly RuntimeActorSocket[]
    private readonly byId: ReadonlyMap<string, RuntimeActorSocket>

    constructor(
        connections: readonly SocketConnection[],
        readonly effects: Pick<SocketEffect[], "push">,
        private readonly schemas: ActorSchemas
    ) {
        const sockets = connections.map(connection => new RuntimeActorSocket(connection, effects, schemas))
        this.sockets = sockets
        this.byId = new Map(sockets.map(socket => [socket.id, socket]))
    }

    eventSocket(connection: SocketConnection, state: ActorSocketState): RuntimeActorSocket {
        const socket = this.byId.get(connection.id)
        if (socket !== undefined) {
            socket.setState(state)
            return socket
        }
        return new RuntimeActorSocket(connection, this.effects, this.schemas, state)
    }

    connection(connectionId: string): RuntimeActorSocket {
        const socket = this.byId.get(connectionId)
        if (socket === undefined)
            throw new ActorProtocolError(`socket connection ${connectionId} is not attached to the actor`)
        return socket
    }

    broadcast(message: unknown, options: ActorBroadcastOptions = {}): void {
        if (options.tagMatch !== undefined && options.tagMatch !== "all" && options.tagMatch !== "any")
            throw new ActorProtocolError('broadcast tagMatch must be "all" or "any"')
        this.effects.push({
            type: "broadcast",
            message: socketMessage(message, this.schemas),
            except_connection_ids: excludedSocketIds(options.except),
            tags: socketTags(options.tags ?? [], this.schemas),
            ...(options.tagMatch === undefined ? {} : { tag_match: options.tagMatch })
        })
    }
}

class RuntimeActorSocket<Metadata = JsonValue> implements ActorSocket<Metadata> {
    private metadataValue: Metadata
    private tagsValue: readonly string[]

    constructor(
        connection: SocketConnection,
        private readonly effects: Pick<SocketEffect[], "push">,
        private readonly schemas: ActorSchemas,
        private stateValue: ActorSocketState = "open"
    ) {
        this.id = connection.id
        this.metadataValue = socketMetadata(connection.metadata, schemas) as Metadata
        this.tagsValue = socketTags(connection.tags, schemas)
    }

    readonly id: string

    get state(): ActorSocketState {
        return this.stateValue
    }

    get metadata(): Metadata {
        return this.metadataValue
    }

    set metadata(value: Metadata) {
        const metadata = socketMetadata(value, this.schemas)
        this.metadataValue = metadata as Metadata
        this.effects.push({ type: "set_metadata", connection_id: this.id, metadata })
    }

    get tags(): readonly string[] {
        return this.tagsValue
    }

    send(message: ActorSocketMessage): void {
        if (this.stateValue === "closed") throw new ActorProtocolError("cannot send on a closed actor socket")
        this.effects.push({ type: "send", connection_id: this.id, message: socketMessage(message, this.schemas) })
    }

    close(code = 1000, reason = ""): void {
        validateClose(code, reason)
        if (this.stateValue === "closed") return
        this.stateValue = "closed"
        this.effects.push({ type: "close", connection_id: this.id, code, reason })
    }

    reject(code = 1008, reason = "connection rejected"): void {
        if (this.stateValue !== "connecting")
            throw new ActorProtocolError("only a connecting actor socket can be rejected")
        validateClose(code, reason)
        this.stateValue = "closed"
        this.effects.push({ type: "reject", connection_id: this.id, code, reason })
    }

    setTags(...tags: string[]): void {
        const unique = socketTags(tags, this.schemas)
        this.tagsValue = unique
        this.effects.push({ type: "set_tags", connection_id: this.id, tags: unique })
    }

    setState(state: ActorSocketState): void {
        this.stateValue = state
    }
}

class SocketOutput {
    private pending: Promise<void> | undefined
    private queued: SocketEffect[] = []
    private queuedBytes = 0
    private failure: unknown

    constructor(private readonly publish: (effects: readonly SocketEffect[]) => Promise<void>) {}

    push(...effects: SocketEffect[]): number {
        if (this.failure !== undefined) throw this.failure
        const bytes = Buffer.byteLength(JSON.stringify(effects))
        if (this.queued.length + effects.length > 512 || this.queuedBytes + bytes > 24 * 1024 * 1024)
            throw new ActorProtocolError("actor socket output queue is full")
        this.queued.push(...effects)
        this.queuedBytes += bytes
        this.pending ??= this.drain()
        void this.pending.catch(() => undefined)
        return this.queued.length
    }

    async flush(): Promise<void> {
        await this.pending
        if (this.failure !== undefined) throw this.failure
    }

    private async drain(): Promise<void> {
        try {
            while (this.queued.length > 0) {
                const batch = this.queued
                this.queued = []
                this.queuedBytes = 0
                await this.publish(batch)
            }
        } catch (error) {
            this.failure = error
            throw error
        } finally {
            this.pending = undefined
        }
    }
}

function socketScope(instance: object): ActorSocketScope {
    const scope = scopes.get(instance)
    if (scope === undefined)
        throw new ActorProtocolError("actor connections are available only during an actor invocation")
    return scope
}

function socketMessage(message: unknown, schemas: ActorSchemas = {}): SocketMessage {
    if (ArrayBuffer.isView(message) || message instanceof ArrayBuffer)
        throw new ActorProtocolError("socket messages must be JSON values, not bytes")
    return { type: "text", data: JSON.stringify(outgoingMessage(message, schemas)) }
}

function decodeSocketMessage(message: SocketMessage, schemas: ActorSchemas = {}): ActorSocketMessage {
    if (message.type !== "text") throw new ActorProtocolError("socket messages must be JSON text frames")
    try {
        return incomingMessage(JSON.parse(message.data), schemas)
    } catch (error) {
        throw new ActorProtocolError("socket message is not valid JSON", { cause: error })
    }
}

function validateClose(code: number, reason: string): void {
    if (!Number.isInteger(code) || (code !== 1000 && (code < 3000 || code > 4999)))
        throw new ActorProtocolError("socket close codes must be 1000 or between 3000 and 4999")
    if (Buffer.byteLength(reason) > 123)
        throw new ActorProtocolError("socket close reasons must not exceed 123 UTF-8 bytes")
}

function excludedSocketIds(except: ActorBroadcastOptions["except"]): readonly string[] {
    if (except === undefined) return []
    return Array.isArray(except) ? except.map(socket => socket.id) : [(except as ActorSocket).id]
}

export { actorConnections, broadcastActor, decodeSocketMessage, runWithActorSockets, socketMessage }
export type {
    ActorSocketScope,
    ActorBroadcastOptions,
    ActorConnection,
    ActorConnectionEventMap,
    ActorSocket,
    ActorSocketMessage,
    ActorSocketState
}
