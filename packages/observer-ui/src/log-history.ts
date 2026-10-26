import { useEffect, useRef, useState } from "react"

import type { ObserverClient } from "./client.js"
import { type ActorLog, type LogPage, type LogQuery, mergeLogs } from "./log-data.js"

export function useLogs(client: Pick<ObserverClient, "listLogs" | "watchLogs">, query: LogQuery) {
    const key = JSON.stringify(query)
    const [records, setRecords] = useState<ActorLog[]>([])
    const [nextCursor, setNextCursor] = useState<string | null>(null)
    const [loading, setLoading] = useState(false)
    const [historyFailed, setHistoryFailed] = useState(false)
    const [liveFailed, setLiveFailed] = useState(false)
    const [persistenceFailed, setPersistenceFailed] = useState(false)
    const [historyStatus, setHistoryStatus] = useState<LogPage>()
    const [liveStatus, setLiveStatus] = useState<LogPage>()
    const load = useRef<(cursor?: string) => void>(() => {})
    useEffect(() => {
        const filters = JSON.parse(key) as LogQuery
        const live = new AbortController()
        let history = new AbortController()
        let timer: ReturnType<typeof setTimeout> | undefined
        let retryDelay = 1_000
        let connections = 0
        setRecords([])
        setNextCursor(null)
        setHistoryStatus(undefined)
        setLiveStatus(undefined)
        setHistoryFailed(false)
        setLiveFailed(false)
        setPersistenceFailed(false)
        setLoading(false)
        load.current = cursor => void readHistory(cursor)
        if (client.watchLogs) void watch()
        if (client.listLogs) void readHistory()
        return () => {
            live.abort()
            history.abort()
            clearTimeout(timer)
        }

        async function readHistory(cursor?: string) {
            history.abort()
            const request = (history = new AbortController())
            setLoading(true)
            try {
                const page = await client.listLogs!({ ...filters, cursor, limit: 100 }, request.signal)
                if (request.signal.aborted) return
                setRecords(current => mergeLogs(current, page.records))
                setNextCursor(page.nextCursor)
                setHistoryStatus(page)
                setPersistenceFailed(page.persistenceFailed)
                setHistoryFailed(false)
            } catch {
                if (!request.signal.aborted) setHistoryFailed(true)
            } finally {
                if (!request.signal.aborted) setLoading(false)
            }
        }

        async function watch() {
            if (connections++ > 0 && client.listLogs) void readHistory()
            try {
                await client.watchLogs!(
                    page => {
                        if (live.signal.aborted) return
                        setRecords(current => mergeLogs(current, page.records))
                        setLiveStatus(page)
                        setPersistenceFailed(page.persistenceFailed)
                        setLiveFailed(false)
                        retryDelay = 1_000
                    },
                    live.signal,
                    filters
                )
                if (!live.signal.aborted) throw new Error("Log stream disconnected")
            } catch {
                if (!live.signal.aborted) setLiveFailed(true)
            } finally {
                if (!live.signal.aborted) {
                    timer = setTimeout(watch, retryDelay)
                    retryDelay = Math.min(retryDelay * 2, 10_000)
                }
            }
        }
    }, [client, key])
    return {
        records,
        nextCursor,
        loading,
        historyFailed,
        liveFailed,
        persistenceFailed,
        dropped: Math.max(historyStatus?.dropped ?? 0, liveStatus?.dropped ?? 0),
        loadOlder: () => load.current(nextCursor ?? undefined),
        retryHistory: () => load.current()
    }
}
