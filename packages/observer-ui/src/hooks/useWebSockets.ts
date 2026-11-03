import useSWR from "swr"
import type { SocketSession } from "../types/Sockets.js"
import { HttpObserverClient } from "../client/observerClient.js"

// There is no WebSocket event stream; live open connections come from the actor inventory stream instead.
const refreshIntervalMs = 15_000

export function useWebSockets() {
    const { data, error, isLoading } = useSWR<SocketSession[]>(
        "websockets",
        () => new HttpObserverClient().listWebSockets(),
        { refreshInterval: refreshIntervalMs },
    )

    return {
        sessions: data ?? null,
        isLoading,
        isError: !data && Boolean(error),
        error,
    }
}
