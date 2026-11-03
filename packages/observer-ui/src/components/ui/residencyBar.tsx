import type { CSSProperties } from "react"

import { cn } from "../../lib/utils.js"

const liveTicks = "bg-[repeating-linear-gradient(90deg,var(--success)_0_3px,transparent_3px_6px)]"
const dormantTicks = "bg-[repeating-linear-gradient(90deg,var(--border)_0_3px,transparent_3px_6px)]"

export type ResidencyBarProps = {
    liveShare: number
    label: string
    pulse?: boolean
    className?: string
    style?: CSSProperties
}

export function ResidencyBar({ liveShare, label, pulse, className, style }: ResidencyBarProps) {
    return (
        <div role="img" aria-label={label} className={cn("relative flex overflow-hidden", className)} style={style}>
            <div className={cn("relative", liveTicks)} style={{ width: `${liveShare * 100}%` }}>
                {pulse && liveShare > 0 && <span className="absolute inset-y-0 right-0.75 w-0.75 animate-pulse bg-success motion-reduce:animate-none" />}
            </div>
            <div className={cn("flex-1", dormantTicks)} />
        </div>
    )
}
