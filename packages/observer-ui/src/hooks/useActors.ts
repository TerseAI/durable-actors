import { actorsKey } from "../client/cacheKeys.js"
import type { EventBus } from "../client/events.js"
import { HttpObserverClient } from "../client/observerClient.js"
import { applyActorEvents } from "../lib/views/actors.js"

import { useLiveView } from "./useLiveView.js"

export function useActors(baseUrl: string, events: EventBus | null) {
    const { data, error, isLoading } = useLiveView(actorsKey(baseUrl), () => new HttpObserverClient(baseUrl).listActorTypes(), applyActorEvents, events)
    return {
        inventory: data ?? null,
        isLoading,
        isError: !data && Boolean(error),
        error
    }
}
