import type { RequestFilter } from "../lib/views/requests.js"
import type { ActorTypesPage, InstancePage, InstancePageQuery } from "../types/Actors.js"
import type { EntryFilter, EntryHistogram, EntryPage } from "../types/Entries.js"
import type { EventHistory, RecordedEvent } from "../types/Events.js"
import type { RequestHistogram, RequestHistoryQuery, RequestTracePage } from "../types/Requests.js"
import type { ResidencyHistory } from "../types/Residency.js"
import type { ResourceUsage } from "../types/Resources.js"
import type { SocketSession } from "../types/Sockets.js"
import type { TimeBounds } from "../types/TimeRange.js"

import { EntryReader } from "./entryReader.js"

const requestPageSize = 500

export const defaultBaseUrl = "/api/observe"

export interface ObserverClient {
    listRequests(query: RequestHistoryQuery): Promise<RequestTracePage>
    getRequestHistogram(filter: RequestFilter, bounds: TimeBounds, bucketMs: number): Promise<RequestHistogram>
    listResidency(actorName: string, bounds: TimeBounds, actorId?: string): Promise<ResidencyHistory>
    getResources(actorName: string, actorId: string, bounds: TimeBounds, bucketMs: number): Promise<ResourceUsage>
    listEventHistory(types: RecordedEvent["type"][], bounds: TimeBounds, actorName?: string, actorId?: string): Promise<EventHistory>
    listWebSockets(): Promise<SocketSession[]>
    listEntries(filter: EntryFilter, bounds: TimeBounds, cursor: string | null): Promise<EntryPage>
    getEntryHistogram(filter: EntryFilter, bounds: TimeBounds, bucketMs: number): Promise<EntryHistogram>
}

export class HttpObserverClient implements ObserverClient {
    private readonly entries = new EntryReader(<T>(path: string) => this.get<T>(path))

    constructor(private readonly baseUrl: string = defaultBaseUrl) {}

    async listObservedActorTypes(offset: number, limit: number): Promise<ActorTypesPage> {
        return await this.get<ActorTypesPage>(`actor-types?offset=${offset}&limit=${limit}`)
    }

    async listInstances(query: InstancePageQuery): Promise<InstancePage> {
        const params = new URLSearchParams()
        for (const [key, value] of Object.entries(query)) if (value !== undefined) params.set(key, String(value))
        return await this.get<InstancePage>(`instances?${params}`)
    }

    async listRequests({ fromMs, toMs, limit = requestPageSize, cursor, actorName, actorId }: RequestHistoryQuery): Promise<RequestTracePage> {
        const query = new URLSearchParams({ fromMs: String(fromMs), toMs: String(toMs), limit: String(limit) })

        if (cursor) query.set("cursor", cursor)
        if (actorName) query.set("actorName", actorName)
        if (actorId) query.set("actorId", actorId)

        return await this.get<RequestTracePage>(`requests?${query}`)
    }

    async getRequestHistogram({ actorName, actorId }: RequestFilter, { fromMs, toMs }: TimeBounds, bucketMs: number): Promise<RequestHistogram> {
        const query = new URLSearchParams({ fromMs: String(fromMs), toMs: String(toMs), bucketMs: String(bucketMs) })
        if (actorName) query.set("actorName", actorName)
        if (actorId) query.set("actorId", actorId)
        return await this.get<RequestHistogram>(`requests/histogram?${query}`)
    }

    async listResidency(actorName: string, { fromMs, toMs }: TimeBounds, actorId?: string): Promise<ResidencyHistory> {
        const query = new URLSearchParams({ actorName, fromMs: String(fromMs), toMs: String(toMs) })
        if (actorId) query.set("actorId", actorId)
        return await this.get<ResidencyHistory>(`residency?${query}`)
    }

    async getResources(actorName: string, actorId: string, { fromMs, toMs }: TimeBounds, bucketMs: number): Promise<ResourceUsage> {
        const query = new URLSearchParams({ actorName, actorId, fromMs: String(fromMs), toMs: String(toMs), bucketMs: String(bucketMs) })
        return await this.get<ResourceUsage>(`resources?${query}`)
    }

    async listEventHistory(types: RecordedEvent["type"][], { fromMs, toMs }: TimeBounds, actorName?: string, actorId?: string): Promise<EventHistory> {
        const query = new URLSearchParams({ types: types.join(), fromMs: String(fromMs), toMs: String(toMs) })
        if (actorName) query.set("actorName", actorName)
        if (actorId) query.set("actorId", actorId)
        return await this.get<EventHistory>(`events/history?${query}`)
    }

    async listWebSockets(): Promise<SocketSession[]> {
        return await this.get<SocketSession[]>("websockets")
    }

    async listEntries(filter: EntryFilter, bounds: TimeBounds, cursor: string | null): Promise<EntryPage> {
        return await this.entries.listEntries(filter, bounds, cursor)
    }

    async getEntryHistogram(filter: EntryFilter, bounds: TimeBounds, bucketMs: number): Promise<EntryHistogram> {
        return await this.entries.getEntryHistogram(filter, bounds, bucketMs)
    }

    private async get<T>(path: string): Promise<T> {
        const response = await fetch(`${this.baseUrl.replace(/\/$/u, "")}/${path}`, {
            method: "GET",
            // Hosts may serve the API from another origin and authenticate with cookies.
            credentials: "include",
            redirect: "error",
            headers: { accept: "application/json" }
        })

        if (!response.ok) {
            throw new Error(`Connection check failed (HTTP ${response.status}).`)
        }

        return (await response.json()) as T
    }
}
