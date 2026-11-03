import { useId } from "react"

const width = 300
const height = 64
const topInset = 8
const bottomInset = 16

export type SparklineProps = {
    values: number[]
    color: string
    label: string
}

export function Sparkline({ values, color, label }: SparklineProps) {
    const gradientId = useId()
    const line = linePath(values)
    return (
        <svg viewBox={`0 0 ${width} ${height}`} preserveAspectRatio="none" role="img" aria-label={label} className="mt-auto block h-16 w-full overflow-visible">
            <defs>
                <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
                    <stop offset="0" style={{ stopColor: color, stopOpacity: 0.16 }} />
                    <stop offset="1" style={{ stopColor: color, stopOpacity: 0 }} />
                </linearGradient>
            </defs>
            <line x1="0" y1={height - 0.5} x2={width} y2={height - 0.5} className="stroke-border" vectorEffect="non-scaling-stroke" />
            <path d={`${line} L${width},${height} L0,${height} Z`} fill={`url(#${gradientId})`} />
            <path d={line} fill="none" style={{ stroke: color }} strokeWidth={1.75} strokeLinejoin="round" vectorEffect="non-scaling-stroke" />
        </svg>
    )
}

function linePath(values: number[]): string {
    const min = Math.min(...values)
    const span = Math.max(...values) - min || 1
    const step = width / Math.max(values.length - 1, 1)
    const plotHeight = height - topInset - bottomInset
    return values.map((value, index) => `${index ? "L" : "M"}${(index * step).toFixed(1)},${(topInset + (1 - (value - min) / span) * plotHeight).toFixed(1)}`).join(" ")
}
