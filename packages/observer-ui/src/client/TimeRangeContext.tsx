import { type ReactNode, createContext, useContext, useEffect, useMemo, useState } from "react"

import { lastHour, resolveWindow } from "../lib/timeRange.js"
import type { TimeRange, TimeWindow } from "../types/TimeRange.js"

const liveTickMs = 5_000

const TimeWindowContext = createContext<TimeWindow | undefined>(undefined)

export function TimeRangeProvider({ range = lastHour, children }: { range?: TimeRange; children: ReactNode }) {
    const now = useNow(range.mode === "live")
    const timeWindow = useMemo(() => resolveWindow(range, now), [range, now])
    return <TimeWindowContext.Provider value={timeWindow}>{children}</TimeWindowContext.Provider>
}

export function useTimeWindow(): TimeWindow {
    const context = useContext(TimeWindowContext)
    if (!context) throw new Error("useTimeWindow must be used within a TimeRangeProvider")
    return context
}

function useNow(ticking: boolean): number {
    const [now, setNow] = useState(Date.now)
    useEffect(() => {
        if (!ticking) return
        setNow(Date.now())
        const timer = setInterval(() => setNow(Date.now()), liveTickMs)
        return () => clearInterval(timer)
    }, [ticking])
    return now
}
