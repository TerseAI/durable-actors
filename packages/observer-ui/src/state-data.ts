export interface StateAttribution {
    operation: string
    connectionId: string | null
    committedAtMs: number
    interleaved: boolean
}

export interface StateRecord {
    stateVersion: number
    ownerEpoch: number
    requestId: string
    attribution: StateAttribution | null
}

export interface StateSnapshot extends StateRecord {
    state: Record<string, unknown>
}

export interface StateQuery {
    actorName: string
    actorId: string
    version?: number
}

export interface StateResponse {
    snapshot: StateSnapshot | null
    schema: Record<string, unknown> | null
}

export interface StateHistoryPage {
    records: StateRecord[]
    nextBefore: number | null
}

export function parseStateResponse(value: unknown): StateResponse {
    if (!object(value) || !(value.snapshot === null || (record(value.snapshot) && object(value.snapshot) && object(value.snapshot.state))) || !(value.schema === null || object(value.schema)))
        throw new Error("Invalid state response")
    return value as unknown as StateResponse
}

export function parseStateHistory(value: unknown): StateHistoryPage {
    if (!object(value) || !Array.isArray(value.records) || !value.records.every(record) || !(value.nextBefore === null || positive(value.nextBefore))) throw new Error("Invalid state history")
    return value as unknown as StateHistoryPage
}

function record(value: unknown): boolean {
    if (!object(value) || !positive(value.stateVersion) || !positive(value.ownerEpoch) || typeof value.requestId !== "string") return false
    const attribution = value.attribution
    return (
        attribution === null ||
        (object(attribution) &&
            typeof attribution.operation === "string" &&
            (attribution.connectionId === null || typeof attribution.connectionId === "string") &&
            typeof attribution.committedAtMs === "number" &&
            Number.isSafeInteger(attribution.committedAtMs) &&
            attribution.committedAtMs >= 0 &&
            typeof attribution.interleaved === "boolean")
    )
}

function positive(value: unknown): value is number {
    return typeof value === "number" && Number.isSafeInteger(value) && value > 0
}
function object(value: unknown): value is Record<string, unknown> {
    return !!value && typeof value === "object" && !Array.isArray(value)
}
