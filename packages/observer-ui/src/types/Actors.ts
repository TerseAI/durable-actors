export interface ActorType {
    actorName: string
    methods?: string[]
    live: number
    dormant: number
    unknown: number
    instances: ActorInstance[]
}

export interface ActorInventory {
    actors: ActorType[]
    /** When each gateway holding an open connection last showed it was alive. */
    gateways: Record<string, number>
}

export type ActorResidency = "live" | "dormant" | "unknown"

export interface WaitingOperation {
    id: string
    operation: string
}

export interface ActorInstance {
    actorId: string
    status: ActorResidency
    lastActiveAtMs?: number | null
    /** When the control plane received the instance's latest report. */
    reportedAtMs?: number | null
    /** When a live or dormant status turns unknown unless another report arrives. */
    staleAtMs?: number | null
    connections: ActorConnection[]
    waiting?: WaitingOperation[] | null
}

export interface ActorConnection {
    id: string
    gatewayId: string
    metadata: unknown
}
