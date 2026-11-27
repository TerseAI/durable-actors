export type ActorResidency = "live" | "dormant" | "unknown"

export interface WaitingOperation {
    id: string
    operation: string
}

export interface InstanceRow {
    actorId: string
    status: ActorResidency
    lastActiveAtMs: number | null
    databaseBytes: number | null
    socketCount: number
}

export interface InstancePage {
    live: number
    dormant: number
    unknown: number
    total: number
    instances: InstanceRow[]
    methods?: string[]
}

export type InstanceSortKey = "instance" | "sockets" | "lastActive" | "disk"

export interface InstancePageQuery {
    actorName: string
    actorId?: string
    search?: string
    status?: ActorResidency
    sort?: InstanceSortKey
    descending?: boolean
    offset?: number
    limit?: number
}

export interface ObservedActorType {
    actorName: string
    live: number
    dormant: number
    unknown: number
}

export interface ActorTypesPage {
    live: number
    dormant: number
    total: number
    largest: number
    openSockets: number
    gateways: Record<string, number>
    actors: ObservedActorType[]
}
