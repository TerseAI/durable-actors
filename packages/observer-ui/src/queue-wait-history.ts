import { useEffect, useState } from "react"

import type { ObserverClient } from "./client.js"
import { queueWaitQuery, queueWaitRows, queueWaitWindowMs } from "./queue-wait.js"
import type { QueueWaitRow } from "./queue-wait.js"

const refreshInterval = 10_000

export function useQueueWaits(client: Pick<ObserverClient, "query">, actorName?: string) {
    const [result, setResult] = useState<{ client: ObserverClient["query"]; actorName?: string; rows: QueueWaitRow[] }>()
    const [failed, setFailed] = useState(false)
    useEffect(() => {
        if (!client.query) return
        const controller = new AbortController()
        let timer: ReturnType<typeof setTimeout> | undefined
        setFailed(false)
        void load()
        return () => {
            controller.abort()
            clearTimeout(timer)
        }
        async function load() {
            try {
                const fromMs = Math.floor((Date.now() - queueWaitWindowMs) / 60_000) * 60_000
                const rows = queueWaitRows(await client.query!(queueWaitQuery(fromMs, actorName), controller.signal))
                if (controller.signal.aborted) return
                setResult({ client: client.query, actorName, rows })
                setFailed(false)
            } catch {
                if (!controller.signal.aborted) setFailed(true)
            } finally {
                if (!controller.signal.aborted) timer = setTimeout(load, refreshInterval)
            }
        }
    }, [client, actorName])
    const current = result && result.client === client.query && result.actorName === actorName ? result : undefined
    return { supported: !!client.query, rows: current?.rows, failed }
}
