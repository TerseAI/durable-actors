import { type ReactNode, useId } from "react"

export type StatCardProps = {
    label: string
    aside?: ReactNode
    value: string
    unit: string
    children?: ReactNode
}

export function StatCard({ label, aside, value, unit, children }: StatCardProps) {
    const labelId = useId()
    return (
        <article aria-labelledby={labelId} className="flex min-h-59 min-w-0 flex-col rounded-xl border bg-card px-5.5 pt-5 pb-4.5 text-card-foreground shadow-(--shadow-control)">
            <div className="flex items-center justify-between gap-3 text-[13px] font-medium text-muted-foreground">
                <span id={labelId} className="truncate">
                    {label}
                </span>
                {aside}
            </div>
            <p className="mt-3.5 flex items-baseline gap-2 text-[44px] leading-none font-bold tracking-[-0.035em] tabular-nums">
                {value}
                <small className="text-[15px] font-medium tracking-normal text-muted-foreground">{unit}</small>
            </p>
            {children}
        </article>
    )
}
