const count = new Intl.NumberFormat("en-US")
const decimal = new Intl.NumberFormat("en-US", { maximumFractionDigits: 1 })
const signedCount = new Intl.NumberFormat("en-US", { signDisplay: "exceptZero" })
const percent = new Intl.NumberFormat("en-US", { style: "percent", minimumFractionDigits: 1, maximumFractionDigits: 1 })
const signedPercent = new Intl.NumberFormat("en-US", { style: "percent", signDisplay: "exceptZero", minimumFractionDigits: 1, maximumFractionDigits: 1 })

const clock = new Intl.DateTimeFormat("en-US", { hour: "2-digit", minute: "2-digit", hourCycle: "h23" })
const dateTime = new Intl.DateTimeFormat("en-US", { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit", hourCycle: "h23" })

export const formatClock = (ms: number) => clock.format(ms)
export const formatTimeSpan = (fromMs: number, toMs: number) =>
    `${dateTime.format(fromMs)} – ${new Date(fromMs).toDateString() === new Date(toMs).toDateString() ? clock.format(toMs) : dateTime.format(toMs)}`
export const formatCount = (value: number) => count.format(value)
export const formatDecimal = (value: number) => decimal.format(value)
export const formatSignedCount = (value: number) => signedCount.format(value)
export const formatPercent = (ratio: number) => percent.format(ratio)
export const formatSignedPercent = (ratio: number) => signedPercent.format(ratio)
export const formatMs = (ms: number) => `${count.format(Math.round(ms))}\u00a0ms`
export const formatPlural = (value: number, noun: string) => `${count.format(value)} ${value === 1 ? noun : `${noun}s`}`

export function formatAgo(ms: number, now: number): string {
    const seconds = Math.max(0, Math.round((now - ms) / 1000))
    if (seconds < 60) return `${seconds}\u00a0s ago`
    if (seconds < 3600) return `${Math.floor(seconds / 60)}\u00a0min ago`
    if (seconds < 86_400) return `${Math.floor(seconds / 3600)}\u00a0h ago`
    return `${Math.floor(seconds / 86_400)}\u00a0d ago`
}
