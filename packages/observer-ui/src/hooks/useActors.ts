import useSWR from "swr"
import type { ActorInventory } from "../types/Actors.js"
import { HttpObserverClient } from "../client/observerClient.js"

export function useActors() {
    const { data, error, isLoading, mutate } = useSWR<ActorInventory>(
        "actors",
        () => new HttpObserverClient().listActorTypes(),
    )

    return {
        actorTypes: data ?? null,
        isLoading,
        isError: Boolean(error),
        error,
        mutate
    }
}