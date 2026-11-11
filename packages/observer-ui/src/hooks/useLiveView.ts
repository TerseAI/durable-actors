import { useEffect, useRef, useState } from "react"

import useSWR, { type Key, type SWRConfiguration } from "swr"

import { EventBus, openEventStream } from "../client/events.js"
import type { ObserveEvent } from "../types/Events.js"

export type ApplyEvents<T> = (data: T, events: ObserveEvent[]) => T

/**
 * The standard way a view stays current: read the store when the view opens, then apply each
 * event from the stream. Without a bus the view is a plain read.
 */
export function useLiveView<T>(key: Key, read: () => Promise<T>, apply: ApplyEvents<T>, bus: EventBus | null, config?: SWRConfiguration<T>) {
    const applyRef = useRef(apply)
    applyRef.current = apply
    const view = useSWR<T>(
        key,
        async () => {
            const data = await read()
            return bus ? applyRef.current(data, bus.recentEvents()) : data
        },
        config
    )
    const { mutate } = view
    useEffect(() => {
        if (!bus) return
        const unsubscribe = bus.subscribe(events => void mutate(data => (data === undefined ? data : applyRef.current(data, events)), { revalidate: false }))
        const unwatch = bus.onReconnect(() => void mutate())
        return () => {
            unsubscribe()
            unwatch()
        }
    }, [bus, mutate])
    return view
}

/** One `observe/events` stream for as long as the caller is mounted with a query. */
export function useEventStream(baseUrl: string, query: URLSearchParams | null): EventBus | null {
    const [bus] = useState(() => new EventBus())
    const serialized = query?.toString() ?? null
    useEffect(() => {
        if (serialized === null) return
        bus.reset()
        return openEventStream(baseUrl, new URLSearchParams(serialized), bus)
    }, [baseUrl, bus, serialized])
    return serialized === null ? null : bus
}
