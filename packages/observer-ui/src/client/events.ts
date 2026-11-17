import { type ObserveEvent, observeEventTypes } from "../types/Events.js"

// Covers the time between an event reaching the stream and its write reaching the store.
const recentMs = 10_000

type Listener = (events: ObserveEvent[]) => void

/**
 * The events one `observe/events` stream delivers. Views read the store when they open, then
 * apply each event; the recent ones are applied again on top of every read so a change the store
 * has yet to receive is not lost.
 */
export class EventBus {
    private readonly listeners = new Set<Listener>()
    private readonly reconnectListeners = new Set<() => void>()
    private recent: { atMs: number; event: ObserveEvent }[] = []

    subscribe(listener: Listener): () => void {
        this.listeners.add(listener)
        return () => this.listeners.delete(listener)
    }

    /** Runs after the stream reconnects, when events may have been missed. */
    onReconnect(listener: () => void): () => void {
        this.reconnectListeners.add(listener)
        return () => this.reconnectListeners.delete(listener)
    }

    /** Forgets the recent events when the stream starts over with a different query. */
    reset(): void {
        this.recent = []
    }

    recentEvents(): ObserveEvent[] {
        const since = Date.now() - recentMs
        return this.recent.filter(entry => entry.atMs >= since).map(entry => entry.event)
    }

    publish(events: ObserveEvent[]): void {
        const now = Date.now()
        this.recent = [...this.recent.filter(entry => entry.atMs >= now - recentMs), ...events.map(event => ({ atMs: now, event }))]
        for (const listener of this.listeners) listener(events)
    }

    reconnected(): void {
        for (const listener of this.reconnectListeners) listener()
    }
}

export function openEventStream(baseUrl: string, query: URLSearchParams, bus: EventBus): () => void {
    const source = new EventSource(`${baseUrl}/events?${query}`, { withCredentials: true })
    for (const type of observeEventTypes) {
        source.addEventListener(type, message => bus.publish(JSON.parse(message.data).map((item: unknown) => ({ type, item }) as ObserveEvent)))
    }
    let opened = false
    source.addEventListener("open", () => {
        if (opened) bus.reconnected()
        opened = true
    })
    return () => source.close()
}
