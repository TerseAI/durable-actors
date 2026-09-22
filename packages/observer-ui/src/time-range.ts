export type TimeRange = { kind: "relative"; minutes: number } | { kind: "absolute"; fromMs: number; toMs: number } | { kind: "all" }

export interface ResolvedRange {
    fromMs?: number
    toMs?: number
}

export const relativePresets = [
    { minutes: 5, label: "Last 5 minutes" },
    { minutes: 15, label: "Last 15 minutes" },
    { minutes: 60, label: "Last hour" },
    { minutes: 6 * 60, label: "Last 6 hours" },
    { minutes: 24 * 60, label: "Last 24 hours" },
    { minutes: 7 * 24 * 60, label: "Last 7 days" }
] as const

export const defaultTimeRange: TimeRange = { kind: "relative", minutes: 60 }

// Relative windows are floored to the minute so polling queries stay identical between ticks.
export function resolveRange(range: TimeRange, now: number): ResolvedRange {
    switch (range.kind) {
        case "relative":
            return { fromMs: Math.floor((now - range.minutes * 60_000) / 60_000) * 60_000 }
        case "absolute":
            return { fromMs: range.fromMs, toMs: range.toMs }
        case "all":
            return {}
    }
}

export function rangeLabel(range: TimeRange): string {
    switch (range.kind) {
        case "relative":
            return relativePresets.find(preset => preset.minutes === range.minutes)?.label ?? `Last ${range.minutes} minutes`
        case "absolute":
            return `${formatStamp(range.fromMs, range.toMs)} – ${formatStamp(range.toMs, range.fromMs)}`
        case "all":
            return "All retained"
    }
}

// Lower-case phrasing for sentences such as "12 sessions in the last hour".
export function rangePhrase(range: TimeRange): string {
    switch (range.kind) {
        case "relative":
            return `in the ${rangeLabel(range).toLocaleLowerCase()}`
        case "absolute":
            return `between ${rangeLabel(range)}`
        case "all":
            return "in retained history"
    }
}

export function sameRange(a: TimeRange, b: TimeRange): boolean {
    return a.kind === b.kind && (a.kind !== "relative" || a.minutes === (b as typeof a).minutes) && (a.kind !== "absolute" || (a.fromMs === (b as typeof a).fromMs && a.toMs === (b as typeof a).toMs))
}

export function toLocalInput(ms: number): string {
    const date = new Date(ms)
    const pad = (value: number) => String(value).padStart(2, "0")
    return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}`
}

function formatStamp(ms: number, other: number): string {
    const date = new Date(ms)
    const sameDay = date.toDateString() === new Date(other).toDateString()
    const time = date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", hour12: false })
    return sameDay && ms > other ? time : `${date.toLocaleDateString([], { month: "short", day: "numeric" })} ${time}`
}
