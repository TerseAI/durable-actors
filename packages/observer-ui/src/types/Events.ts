import type { ActorResidency, WaitingOperation } from "./Actors.js"
import type { ActorLog } from "./Logs.js"
import type { RequestTrace } from "./Requests.js"

/** An instance as its latest lifecycle report leaves it. */
export interface InstanceChange {
    actorName: string
    actorId: string
    status: ActorResidency
    occurredAtMs: number
    reportedAtMs: number
    /** When the status turns unknown unless another report arrives; null once stopped. */
    staleAtMs: number | null
    lastActiveAtMs: number | null
    waiting: WaitingOperation[] | null
}

export interface ConnectionOpened {
    connectionId: string
    actorName: string
    actorId: string
    gatewayId: string
    openedAtMs: number
    metadata: unknown
}

export interface ConnectionClosed {
    connectionId: string
    actorName: string
    actorId: string
    gatewayId: string
    openedAtMs: number
    closedAtMs: number
    closeCode: number | null
    closeReason: string | null
    messagesIn: number | null
    messagesOut: number | null
}

export interface GatewayAlive {
    gatewayId: string
    atMs: number
}

/** The actor classes a deployment leaves deployed; empty once it is removed. */
export interface Deployment {
    atMs: number
    actors: { actorName: string; methods: string[] }[]
}

/** One change on the project's live channels, as `observe/events` sends it. */
export type ObserveEvent =
    | { type: "request"; item: RequestTrace }
    | { type: "log"; item: ActorLog }
    | { type: "instance"; item: InstanceChange }
    | { type: "connectionOpened"; item: ConnectionOpened }
    | { type: "connectionClosed"; item: ConnectionClosed }
    | { type: "gatewayAlive"; item: GatewayAlive }
    | { type: "deployment"; item: Deployment }

export const observeEventTypes: ObserveEvent["type"][] = ["request", "log", "instance", "connectionOpened", "connectionClosed", "gatewayAlive", "deployment"]
