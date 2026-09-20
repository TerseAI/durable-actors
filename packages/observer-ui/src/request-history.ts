import { useEffect, useRef, useState } from "react"

import type { ObserverClient, RequestTracePage } from "./client.js"
import { historyPage, historyQuery } from "./request-sql.js"
import type { HistoryFilters } from "./request-sql.js"

export function useRequestHistory(client: Pick<ObserverClient, "query">, query: HistoryFilters | undefined) {
    const [page, setPage] = useState<RequestTracePage>()
    const [loading, setLoading] = useState(false)
    const [failed, setFailed] = useState(false)
    const load = useRef<(cursor?: string) => void>(() => {})
    useEffect(() => {
        setPage(undefined)
        setFailed(false)
        setLoading(false)
        if (!query) return
        const controller = new AbortController()
        let busy = false
        load.current = (cursor?: string) => {
            if (busy || controller.signal.aborted) return
            busy = true
            setLoading(true)
            setFailed(false)
            void fetchPage(cursor)
        }
        async function fetchPage(cursor?: string) {
            try {
                if (!client.query) throw new Error("History is unavailable")
                const incoming = historyPage(await client.query(historyQuery(query!, cursor), controller.signal), cursor)
                if (controller.signal.aborted) return
                setPage(current => {
                    if (!cursor || !current || current.epoch !== incoming.epoch) return incoming
                    const records = new Map(current.records.map(record => [record.eventId ?? record.sequence, record]))
                    for (const record of incoming.records) records.set(record.eventId ?? record.sequence, record)
                    return { ...incoming, reset: current.reset || incoming.reset, records: [...records.values()] }
                })
            } catch {
                if (!controller.signal.aborted) setFailed(true)
            } finally {
                busy = false
                if (!controller.signal.aborted) setLoading(false)
            }
        }
        load.current()
        return () => controller.abort()
    }, [client, query])
    return { page, loading, failed, loadOlder: () => load.current(page?.nextCursor ?? undefined), retry: () => load.current(page?.nextCursor ?? undefined) }
}
