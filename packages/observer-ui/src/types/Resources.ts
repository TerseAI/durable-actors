export type KillReason = "out_of_memory" | "evicted" | "killed"

export interface ResourceSample {
    atMs: number
    cpuMillicores: number | null
    memoryBytes: number
    memoryLimitBytes: number | null
}

export interface DatabaseSize {
    atMs: number
    bytes: number
}

export interface ActorKill {
    atMs: number
    reason: KillReason
    message: string | null
}

export interface ActorResources {
    samples: ResourceSample[]
    database: DatabaseSize[]
    kills: ActorKill[]
}
