export type StatCardProps = {
    title: string;
    description: string;
}

export function StatCard({ title, description }: StatCardProps) {
    return (
        <div className="flex flex-col gap-2">
            <h2 className="text-lg font-semibold">{title}</h2>
            <p className="text-sm text-muted-foreground">{description}</p>
        </div>
    )
}