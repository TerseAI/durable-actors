import { useEffect } from "react"

import useSWR from "swr"

import { useData } from "../client/DatatContext.js"
import { HttpObserverClient } from "../client/observerClient.js"
import type { InstancePageQuery } from "../types/Actors.js"
import type { ObserveEvent } from "../types/Events.js"

const refreshDelayMs = 5_000

/** A change can move an instance across a page boundary, so refresh the current page after relevant events. */
export function useInstancePage(query: InstancePageQuery, live: boolean) {
    const { baseUrl } = useData()
    const { actorName, actorId } = query
    return useInventoryPage(["instances", baseUrl, query], () => new HttpObserverClient(baseUrl).listInstances(query), live, actorName, actorId)
}

export function useActorTypesPage(offset: number, limit: number) {
    const { baseUrl } = useData()
    return useInventoryPage(["actor-types", baseUrl, offset, limit], () => new HttpObserverClient(baseUrl).listObservedActorTypes(offset, limit), true)
}

function useInventoryPage<T>(key: readonly unknown[], read: () => Promise<T>, live: boolean, actorName?: string, actorId?: string) {
    const { events } = useData()
    const { data, error, mutate, isValidating } = useSWR(key, read, { revalidateOnFocus: false })
    useEffect(() => {
        if (!live || !events) return
        let timer: ReturnType<typeof setTimeout> | undefined
        let stopped = false
        let dirty = false
        function refresh() {
            dirty = true
            if (timer !== undefined || stopped) return
            timer = setTimeout(
                async () => {
                    dirty = false
                    try {
                        await mutate()
                    } catch {
                        // SWR exposes the read error to the view.
                    } finally {
                        timer = undefined
                        if (dirty) refresh()
                    }
                },
                actorName === undefined ? 15_000 : refreshDelayMs
            )
        }
        const unsubscribe = events.subscribe(batch => {
            if (batch.some(event => affectsInventory(event, actorName, actorId))) refresh()
        })
        const unwatch = events.onReconnect(refresh)
        return () => {
            stopped = true
            clearTimeout(timer)
            unsubscribe()
            unwatch()
        }
    }, [actorId, actorName, events, live, mutate])
    return { data, error, isValidating, retry: () => void mutate() }
}

function affectsInventory(event: ObserveEvent, actorName?: string, actorId?: string): boolean {
    switch (event.type) {
        case "deployment":
            return true
        case "instance":
        case "connectionOpened":
        case "connectionClosed":
            return (actorName === undefined || event.item.actorName === actorName) && (actorId === undefined || event.item.actorId === actorId)
        default:
            return false
    }
}
