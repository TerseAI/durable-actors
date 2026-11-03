import type { ActorInventory } from "../types/Actors.js";

const baseUrl = "/api/observe";

interface ObserverClient {
    listActorTypes(): Promise<ActorInventory>
}

export class HttpObserverClient implements ObserverClient {
    async listActorTypes(): Promise<ActorInventory> {
        return await this.get<ActorInventory>("actors");
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