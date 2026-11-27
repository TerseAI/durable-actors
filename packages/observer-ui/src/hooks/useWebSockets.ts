import { webSocketsKey } from "../client/cacheKeys.js"
import type { EventBus } from "../client/events.js"
import { HttpObserverClient } from "../client/observerClient.js"
import { applySocketEvents } from "../lib/views/sockets.js"

import { useLiveView } from "./useLiveView.js"

export function useWebSockets(baseUrl: string, events: EventBus | null) {
    const { data, error, isLoading } = useLiveView(
        webSocketsKey(baseUrl),
        async () => ({ sessions: await new HttpObserverClient(baseUrl).listWebSockets(), gateways: {} as Record<string, number> }),
        applySocketEvents,
        events
    )
    return {
        sessions: data?.sessions ?? null,
        gateways: data?.gateways,
        isLoading,
        isError: !data && Boolean(error),
        error
    }
}
