import { useEffect } from "react"

import { useData } from "../client/DatatContext.js"
import { residencyKey } from "../client/cacheKeys.js"
import { HttpObserverClient } from "../client/observerClient.js"
import type { TimeWindow } from "../types/TimeRange.js"

import { useWindowedSWR } from "./useWindowedSWR.js"

export function useResidency(actorName: string, timeWindow: TimeWindow) {
    const { baseUrl, actorTypes } = useData()
    const { data, error, isLoading, mutate } = useWindowedSWR(residencyKey(baseUrl, actorName), timeWindow, bounds => new HttpObserverClient(baseUrl).listResidency(actorName, bounds))
    // The inventory stream pushes every live/dormant change, so live windows re-read the history only then.
    const statuses = actorTypes?.actors
        .find(type => type.actorName === actorName)
        ?.instances.map(instance => `${instance.actorId}:${instance.status}`)
        .join(",")
    useEffect(() => {
        if (timeWindow.live && statuses !== undefined) void mutate()
    }, [statuses, timeWindow.live, mutate])

    return {
        residency: data ?? null,
        isLoading,
        isError: !data && Boolean(error),
        error
    }
}
