import { useEffect, useState } from "react"

import type { ActorInventory, ObserverClient, RequestTracePage } from "./client.js"

export function useInventory(client: ObserverClient) {
    const [attempt, setAttempt] = useState(0)
    const [snapshot, setSnapshot] = useState<{ client: ObserverClient; inventory: ActorInventory; updatedAt: number }>()
    const [loading, setLoading] = useState(true)
    const [failed, setFailed] = useState(false)
    useEffect(() => {
        const controller = new AbortController()
        let timer: ReturnType<typeof setTimeout> | undefined
        let retryDelay = 1_000
        setFailed(false)
        setLoading(true)
        void refresh()
        return () => {
            controller.abort()
            clearTimeout(timer)
        }
        function receive(inventory: ActorInventory) {
            if (controller.signal.aborted) return
            setSnapshot({ client, inventory, updatedAt: Date.now() })
            setFailed(false)
            setLoading(false)
            retryDelay = 1_000
        }
        async function refresh() {
            try {
                if (client.watchActors) {
                    await client.watchActors(receive, controller.signal)
                    if (!controller.signal.aborted) throw new Error("Stream ended")
                } else {
                    setLoading(true)
                    receive(await client.listActors(controller.signal))
                }
            } catch {
                if (!controller.signal.aborted) setFailed(true)
            } finally {
                if (!controller.signal.aborted) {
                    setLoading(false)
                    timer = setTimeout(refresh, client.watchActors ? retryDelay : 5_000)
                    retryDelay = Math.min(retryDelay * 2, 10_000)
                }
            }
        }
    }, [client, attempt])
    return {
        updatedAt: snapshot?.client === client ? snapshot.updatedAt : undefined,
        inventory: snapshot?.client === client ? snapshot.inventory : undefined,
        loading,
        failed,
        retry: () => setAttempt(value => value + 1)
    }
}

export function useRequests(client: Pick<ObserverClient, "watchRequests">) {
    const [page, setPage] = useState<RequestTracePage>()
    const [failed, setFailed] = useState(false)
    const [attempt, setAttempt] = useState(0)
    useEffect(() => setPage(undefined), [client])
    useEffect(() => {
        const controller = new AbortController()
        let timer: ReturnType<typeof setTimeout> | undefined
        let delay = 1000
        async function watch() {
            try {
                if (!client.watchRequests) throw new Error("Request traces unavailable")
                await client.watchRequests(incoming => {
                    if (controller.signal.aborted) return
                    setPage(current => mergePages(current, incoming))
                    setFailed(false)
                    delay = 1000
                }, controller.signal)
                if (!controller.signal.aborted) throw new Error("Request stream disconnected")
            } catch {
                if (!controller.signal.aborted) setFailed(true)
            } finally {
                if (!controller.signal.aborted) {
                    timer = setTimeout(watch, delay)
                    delay = Math.min(delay * 2, 10000)
                }
            }
        }
        setFailed(false)
        void watch()
        return () => {
            controller.abort()
            clearTimeout(timer)
        }
    }, [client, attempt])
    return { page, failed, retry: () => setAttempt(value => value + 1) }
}

function mergePages(current: RequestTracePage | undefined, incoming: RequestTracePage): RequestTracePage {
    const records = new Map((current?.epoch === incoming.epoch ? current.records : []).map(record => [record.sequence, record]))
    for (const record of incoming.records) records.set(record.sequence, record)
    return {
        ...incoming,
        records: [...records.values()]
            .filter(record => record.sequence > incoming.evicted)
            .sort((a, b) => b.sequence - a.sequence)
            .slice(0, incoming.capacity)
    }
}
