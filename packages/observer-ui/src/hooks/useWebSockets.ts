import { useEffect } from "react"

import useSWR from "swr"

import { webSocketsKey } from "../client/cacheKeys.js"
import { HttpObserverClient } from "../client/observerClient.js"
import type { ActorInventory } from "../types/Actors.js"
import type { SocketSession } from "../types/Sockets.js"

export function useWebSockets(baseUrl: string, inventory: ActorInventory | null) {
    const { data, error, isLoading, mutate } = useSWR<SocketSession[]>(webSocketsKey(baseUrl), () => new HttpObserverClient(baseUrl).listWebSockets())
    // The inventory stream pushes every open and close, so the history is re-read only then.
    const open = inventory?.actors.flatMap(actor => actor.instances.flatMap(instance => instance.connections.map(connection => connection.id))).join(",")
    useEffect(() => {
        if (open !== undefined) void mutate()
    }, [open, mutate])

    return {
        sessions: data ?? null,
        isLoading,
        isError: !data && Boolean(error),
        error
    }
}
