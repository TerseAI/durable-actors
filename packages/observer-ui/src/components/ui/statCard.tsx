export type StatCardProps = {
    label: string
    value: string | number
    detail?: string
}

export function StatCard({ label, value, detail }: StatCardProps) {
    return (
        <div className="flex min-w-0 flex-col rounded-xl border bg-card px-4 py-3 text-card-foreground">
            <dt className="truncate text-xs font-medium text-muted-foreground">{label}</dt>
            <dd className="mt-1 text-2xl font-semibold tracking-tight tabular-nums">{value}</dd>
            {detail && <dd className="mt-0.5 truncate text-xs text-muted-foreground">{detail}</dd>}
        </div>
    )
}
