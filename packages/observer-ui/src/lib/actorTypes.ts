export type ActorTypeSummary = {
    actorName: string
    total: number
    live: number
    dormant: number
}

// Relative so the routes work both standalone and nested under a host app's route.
export const actorTypePath = (actorName: string) => `actors/${encodeURIComponent(actorName)}`
export const instancePath = (actorId: string) => `instances/${encodeURIComponent(actorId)}`
