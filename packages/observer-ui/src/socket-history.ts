import { useEffect, useState } from "react"

import type { ObserverClient } from "./client.js"
import { sessionLimit, sessionRows, sessionsQuery } from "./socket-sessions.js"
import type { SocketSessionRow } from "./socket-sessions.js"

const refreshInterval = 5_000

export function useSocketHistory(client: Pick<ObserverClient, "query">, fromMs: number | undefined) {
    const [attempt, setAttempt] = useState(0)
    const [result, setResult] = useState<{ client: ObserverClient["query"]; fromMs: number | undefined; rows: SocketSessionRow[]; updatedAt: number }>()
    const [failed, setFailed] = useState(false)
    const [loading, setLoading] = useState(!!client.query)
    useEffect(() => {
        if (!client.query) return
        const controller = new AbortController()
        let timer: ReturnType<typeof setTimeout> | undefined
        setFailed(false)
        setLoading(true)
        void load()
        return () => {
            controller.abort()
            clearTimeout(timer)
        }
        async function load() {
            try {
                const rows = sessionRows(await client.query!(sessionsQuery(fromMs), controller.signal))
                if (controller.signal.aborted) return
                setResult({ client: client.query, fromMs, rows, updatedAt: Date.now() })
                setFailed(false)
            } catch {
                if (!controller.signal.aborted) setFailed(true)
            } finally {
                if (!controller.signal.aborted) {
                    setLoading(false)
                    timer = setTimeout(load, refreshInterval)
                }
            }
        }
    }, [client, fromMs, attempt])
    const current = result && result.client === client.query && result.fromMs === fromMs ? result : undefined
    return {
        supported: !!client.query,
        rows: current?.rows,
        updatedAt: current?.updatedAt,
        capped: (current?.rows.length ?? 0) >= sessionLimit,
        loading,
        failed,
        retry: () => setAttempt(value => value + 1)
    }
}
