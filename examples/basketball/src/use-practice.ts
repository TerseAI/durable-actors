import { useCallback, useEffect, useState } from "react"

import type { PracticeApi } from "./client.js"
import type { EventKind, PracticeSummary } from "./types.js"

export function usePractice(session: string, api: Pick<PracticeApi, "load" | "record" | "undo">) {
    const [data, setData] = useState<PracticeSummary | null>(null)
    const [busy, setBusy] = useState(true)
    const [error, setError] = useState("")
    const [revision, setRevision] = useState(0)

    useEffect(() => {
        const controller = new AbortController()
        setData(null)
        setBusy(true)
        setError("")
        api.load(session, controller.signal)
            .then(setData)
            .catch(error => {
                if (!controller.signal.aborted) setError(message(error))
            })
            .finally(() => {
                if (!controller.signal.aborted) setBusy(false)
            })
        return () => controller.abort()
    }, [session, api, revision])

    const mutate = useCallback(async (operation: () => Promise<PracticeSummary>) => {
        setBusy(true)
        setError("")
        try {
            setData(await operation())
        } catch (error) {
            setError(message(error))
        } finally {
            setBusy(false)
        }
    }, [])

    return {
        data,
        busy,
        error,
        refresh: () => setRevision(value => value + 1),
        record: (kind: EventKind, made: boolean) => mutate(() => api.record(session, kind, made)),
        undo: () => data?.recent[0] && mutate(() => api.undo(session, data.recent[0]!.id))
    }
}

function message(error: unknown): string {
    return error instanceof Error ? error.message : "Could not reach your practice. Refresh and try again."
}
