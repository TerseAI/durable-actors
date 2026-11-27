import type { Query } from "../lib/query.js"

import type { ObserveEvent } from "./Events.js"
import type { LevelCounts, LogLevel } from "./Logs.js"

/** Where an entry comes from: the actor's console, its method calls, its WebSockets (connections and their callbacks), or the platform. */
export type EntrySource = "console" | "rpc" | "websocket" | "system"

/** A record the Logs view lists, in the shape `observe/events` streams it. */
export type EntryRecord = Extract<ObserveEvent, { type: "request" | "log" | "connectionOpened" | "connectionClosed" | "kill" | "lifecycle" }>

export interface EntryPage {
    records: EntryRecord[]
    nextCursor: string | null
    /** Older records may still be missing: a source with more pages has not reached them yet. */
    cutoffMs: number | null
}

/** A class's or one instance's entries, narrowed by the Logs search. */
export interface EntryFilter {
    actorName: string
    /** The instance a page is about; a class's page leaves it null and narrows with `instance:` instead. */
    actorId: string | null
    query: Query
}

export type SourceCounts = Record<EntrySource, LevelCounts>

/** Bucket `i` covers `fromMs + i * bucketMs` up to the next bucket. */
export interface EntryHistogram {
    fromMs: number
    bucketMs: number
    buckets: SourceCounts[]
}
