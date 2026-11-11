export type KillReason = "out_of_memory" | "evicted" | "killed"

/** What an activation reserved, as billed. A missing limit is uncapped. */
export interface ResourceLimits {
    cpuMillis: number
    cpuLimitMillis?: number
    memoryMib: number
    memoryLimitMib?: number
}

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
