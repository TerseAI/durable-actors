import { type ReactNode, createContext, useContext } from "react"

import { useEventStream } from "../hooks/useLiveView.js"
import { useNow } from "../hooks/useNow.js"
import { useRequestHistogram } from "../hooks/useRequests.js"
import type { RequestHistogram } from "../types/Requests.js"

import { useTimeWindow } from "./TimeRangeContext.js"
import type { EventBus } from "./events.js"
import { defaultBaseUrl } from "./observerClient.js"

// Statuses that change without an event (unknown, lost) are worked out on this tick.
const statusTickMs = 5_000

const projectEvents = new URLSearchParams({ signals: "requests,platform" })

type DataContextType = {
    baseUrl: string
    events: EventBus | null
    requestHistogram: RequestHistogram | undefined
    isRequestsError: boolean
    /** The time current statuses are worked out at: the clock while live, held while paused. */
    nowMs: number
}

const DataContext = createContext<DataContextType | undefined>(undefined)

export const DataProvider = ({ baseUrl = defaultBaseUrl, children }: { baseUrl?: string; children: ReactNode }) => {
    const timeWindow = useTimeWindow()
    // Pausing closes the stream, so every view keeps what it showed until live updates resume.
    const events = useEventStream(baseUrl, timeWindow.live ? projectEvents : null)
    const now = useNow(timeWindow.live ? statusTickMs : null)
    const { histogram: requestHistogram, isError: isRequestsError } = useRequestHistogram(baseUrl, timeWindow, {}, events)

    return (
        <DataContext.Provider
            value={{
                baseUrl,
                events,
                requestHistogram,
                isRequestsError,
                nowMs: now
            }}
        >
            {children}
        </DataContext.Provider>
    )
}

export const useData = (): DataContextType => {
    const context = useContext(DataContext)
    if (!context) {
        throw new Error("useData must be used within a DataProvider")
    }
    return context
}
