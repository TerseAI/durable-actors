import { webSocketsKey } from "../client/cacheKeys.js"
import type { EventBus } from "../client/events.js"
import { HttpObserverClient } from "../client/observerClient.js"
import { applySocketEvents } from "../lib/views/sockets.js"

import { useLiveView } from "./useLiveView.js"

export function useWebSockets(baseUrl: string, events: EventBus | null) {
    const { data, error, isLoading } = useLiveView(webSocketsKey(baseUrl), () => new HttpObserverClient(baseUrl).listWebSockets(), applySocketEvents, events)
    return {
        sessions: data ?? null,
        isLoading,
        isError: !data && Boolean(error),
        error
    }
}
