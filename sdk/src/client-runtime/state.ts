import type { JsonObject } from "./json.js"

/** Initial snapshot of public `@Emittable` fields. */
export interface ActorStateMessage<State = JsonObject> {
    readonly type: "state"
    readonly state: State
    readonly version?: number
}

/** Saved state update. Merge `changes` and delete fields listed in `removed`. */
export interface ActorStateUpdate<State = JsonObject> {
    readonly type: "state_update"
    readonly changes: Partial<State>
    readonly removed: readonly string[]
    readonly version: number
}
