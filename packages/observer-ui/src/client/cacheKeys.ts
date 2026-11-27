import { countQuery } from "../lib/entries.js"
import type { RequestFilter } from "../lib/views/requests.js"
import type { EntryFilter } from "../types/Entries.js"

// Every key carries the base URL so observers for different projects never share cached data.

export const requestHistogramKey = (baseUrl: string, { actorName, actorId }: RequestFilter) => ["requests/histogram", baseUrl, actorName, actorId] as const

export const residencyKey = (baseUrl: string, actorName: string, actorId?: string) => ["residency", baseUrl, actorName, actorId] as const

export const resourcesKey = (baseUrl: string, actorName: string, actorId: string) => ["resources", baseUrl, actorName, actorId] as const

export const eventHistoryKey = (baseUrl: string, types: string[], actorName?: string, actorId?: string) => ["events/history", baseUrl, types.join(), actorName, actorId] as const

export const webSocketsKey = (baseUrl: string) => ["websockets", baseUrl] as const

export const entriesKey = (baseUrl: string, { actorName, actorId, query }: EntryFilter) => ["entries", baseUrl, actorName, actorId, JSON.stringify(query)] as const

export const entryHistogramKey = (baseUrl: string, { actorName, actorId, query }: EntryFilter) => ["entries/histogram", baseUrl, actorName, actorId, JSON.stringify(countQuery(query))] as const

export const streamedEntriesKey = (baseUrl: string, filter: EntryFilter) => ["entries/streamed", ...entriesKey(baseUrl, filter).slice(1)] as const
