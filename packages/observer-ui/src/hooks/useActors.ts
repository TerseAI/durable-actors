import useSWR from "swr"
import useSWRSubscription, { type SWRSubscriptionOptions } from "swr/subscription"
import type { ActorInventory } from "../types/Actors.js"
import { HttpObserverClient } from "../client/observerClient.js"
import { subscribeToActors } from "../client/streams/actorStream.js"

export function useActors() {
    const { data, error, isLoading } = useSWR<ActorInventory>(
        "actors",
        () => new HttpObserverClient().listActorTypes(),
    )
    const { data: live } = useSWRSubscription(
        "actors/events",
        (_key: string, { next }: SWRSubscriptionOptions<ActorInventory>) => subscribeToActors(inventory => next(null, inventory)),
    )

    return {
        actorTypes: live ?? data ?? null,
        isLoading,
        isError: !live && Boolean(error),
        error,
    }
}
