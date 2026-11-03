import type { ActorInventory } from "../types/Actors.js";
import type { RequestTracePage } from "../types/Requests.js";
import type { SocketSession } from "../types/Sockets.js";
import type { TimeBounds } from "../types/TimeRange.js";

const baseUrl = "/api/observe";

type totalActorInstancesPayload = Map<string, ActorInventory>

export interface ObserverClient {
    listActorTypes(): Promise<ActorInventory>
    listRequests(bounds: TimeBounds): Promise<RequestTracePage>
    listWebSockets(): Promise<SocketSession[]>
}

export class HttpObserverClient implements ObserverClient {
    async listActorTypes(): Promise<ActorInventory> {
        return await this.get<ActorInventory>("actors");
    }

    async listRequests({ fromMs, toMs }: TimeBounds): Promise<RequestTracePage> {
        const query = new URLSearchParams({ fromMs: String(fromMs), limit: "500" })
        if (toMs !== undefined) query.set("toMs", String(toMs))
        return await this.get<RequestTracePage>(`requests?${query}`);
    }

    async listWebSockets(): Promise<SocketSession[]> {
        return await this.get<SocketSession[]>("websockets");
    }

    private async get<T>(path: string): Promise<T> {
        const response = await fetch(`${baseUrl.replace(/\/$/u, "")}/${path}`, {
            method: "GET",
            credentials: "same-origin",
            redirect: "error",
            headers: { accept: "application/json" }
        })

        if (!response.ok) {
            throw new Error(`Connection check failed (HTTP ${response.status}).`)
        }

        return await response.json() as T
    }
}