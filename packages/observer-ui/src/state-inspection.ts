import { useEffect, useRef, useState } from "react"

import type { ObserverClient } from "./client.js"
import type { StateHistoryPage, StateRecord, StateResponse } from "./state-data.js"

export function useStateInspection(client: ObserverClient, actorName: string, actorId: string) {
    const [current, setCurrent] = useState<StateResponse>()
    const [history, setHistory] = useState<StateHistoryPage>()
    const [failed, setFailed] = useState(false)
    const [loading, setLoading] = useState(true)
    const [knownVersion, setKnownVersion] = useState(0)
    const load = useRef<(older?: boolean) => void>(() => {})
    useEffect(() => {
        const controller = new AbortController()
        let busy = false
        let pending = false
        let page: StateHistoryPage | undefined
        let timer: ReturnType<typeof setTimeout>
        setCurrent(undefined)
        setHistory(undefined)
        setKnownVersion(0)
        async function refresh(older = false) {
            if (busy) {
                pending = true
                return
            }
            busy = true
            setLoading(true)
            try {
                const [snapshot, incoming] = await Promise.all([
                    client.getState!({ actorName, actorId }, controller.signal),
                    client.listStateHistory!({ actorName, actorId, ...(older && page?.nextBefore ? { before: page.nextBefore } : {}) }, controller.signal)
                ])
                if (controller.signal.aborted) return
                setCurrent(snapshot)
                const records = new Map<number, StateRecord>(page?.records.map(record => [record.stateVersion, record]))
                for (const record of incoming.records) records.set(record.stateVersion, record)
                const overlaps = incoming.records.some(record => page?.records.some(previous => previous.stateVersion === record.stateVersion))
                const nextBefore = older || !page || !overlaps ? incoming.nextBefore : page.nextBefore
                page = { ...incoming, nextBefore, records: [...records.values()].sort((a, b) => b.stateVersion - a.stateVersion) }
                setHistory(page)
                setFailed(false)
            } catch {
                if (!controller.signal.aborted) setFailed(true)
            } finally {
                busy = false
                if (!controller.signal.aborted) {
                    setLoading(false)
                    clearTimeout(timer)
                    timer = setTimeout(() => void refresh(), pending ? 500 : 15000)
                    pending = false
                }
            }
        }
        load.current = older => {
            clearTimeout(timer)
            void refresh(older)
        }
        void refresh()
        if (client.watchRequests)
            void client
                .watchRequests(page => {
                    let version = 0
                    for (const record of page.records) if (record.actorName === actorName && record.actorId === actorId) version = Math.max(version, record.stateVersion ?? 0)
                    if (version > 0) setKnownVersion(previous => Math.max(previous, version))
                }, controller.signal)
                .catch(() => {})
        return () => {
            controller.abort()
            clearTimeout(timer)
        }
    }, [client, actorName, actorId])
    useEffect(() => {
        if (knownVersion > 0) load.current()
    }, [knownVersion])
    return { current, history, failed, loading, knownVersion, refresh: () => load.current(), loadOlder: () => load.current(true) }
}
