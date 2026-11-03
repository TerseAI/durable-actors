import useSWR from "swr"
import type { ActorInventory } from "../types/Actors.js"
import { HttpObserverClient } from "../client/observerClient.js"

export function useActors() {
    const { data, error, isLoading, mutate } = useSWR<ActorInventory | null>(
        "actors",
        () => new HttpObserverClient().listActorTypes(),
    )

    if (data == null || data == undefined) {
        throw new Error("Failed to load actors")
    }

    return {
        actorTypes: data,
        isLoading,
        isError: Boolean(error),
        error,
        mutate
    }
}