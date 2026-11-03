import useSWR, { type SWRConfiguration } from "swr"

import type { TimeBounds, TimeWindow } from "../types/TimeRange.js"

// Live keys use the duration rather than the moving bounds so the clock tick doesn't refetch, and each live fetch reads up to now.
export function useWindowedSWR<T>(key: unknown[], { fromMs, toMs, live }: TimeWindow, fetch: (bounds: TimeBounds) => Promise<T>, config?: SWRConfiguration<T>) {
    const durationMs = toMs - fromMs
    return useSWR<T>(live ? [...key, "live", durationMs] : [...key, fromMs, toMs], () => fetch(live ? { fromMs: Date.now() - durationMs } : { fromMs, toMs }), config)
}
