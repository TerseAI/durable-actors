const count = new Intl.NumberFormat("en-US")
const decimal = new Intl.NumberFormat("en-US", { maximumFractionDigits: 1 })
const signedCount = new Intl.NumberFormat("en-US", { signDisplay: "exceptZero" })
const percent = new Intl.NumberFormat("en-US", { style: "percent", minimumFractionDigits: 1, maximumFractionDigits: 1 })
const signedPercent = new Intl.NumberFormat("en-US", { style: "percent", signDisplay: "exceptZero", minimumFractionDigits: 1, maximumFractionDigits: 1 })

const clock = new Intl.DateTimeFormat("en-US", { hour: "2-digit", minute: "2-digit", hourCycle: "h23" })

export const formatClock = (ms: number) => clock.format(ms)
export const formatCount = (value: number) => count.format(value)
export const formatDecimal = (value: number) => decimal.format(value)
export const formatSignedCount = (value: number) => signedCount.format(value)
export const formatPercent = (ratio: number) => percent.format(ratio)
export const formatSignedPercent = (ratio: number) => signedPercent.format(ratio)
