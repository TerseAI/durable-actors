import { type ReactNode, useState } from "react"
import { Link } from "react-router"

import { ArrowDown, ArrowUp } from "lucide-react"

import { formatAgo, formatBytes, formatCount } from "../../format.js"
import { useDebouncedValue } from "../../hooks/useDebouncedValue.js"
import { useInstancePage } from "../../hooks/useInstancePage.js"
import { type ActorTypeSummary, instancePath } from "../../lib/actorTypes.js"
import { cn } from "../../lib/utils.js"
import type { InstanceRow, InstanceSortKey } from "../../types/Actors.js"
import { Button } from "../ui/button.js"
import { Pagination } from "../ui/pagination.js"
import { SearchField } from "../ui/searchField.js"
import { SectionCard } from "../ui/sectionCard.js"
import { SegmentedControl } from "../ui/segmentedControl.js"
import { Skeleton } from "../ui/skeleton.js"

type InstanceFilter = "all" | "live" | "dormant"
type Sort = { key: InstanceSortKey; descending: boolean }

type Column = {
    key: InstanceSortKey
    label: string
    width: string
    title?: string
    render: (instance: InstanceRow, now: number) => ReactNode
}

const pageRows = 25
const defaultSort: Sort = { key: "lastActive", descending: true }

const columns: Column[] = [
    { key: "instance", label: "Instance", width: "minmax(0,2fr)", render: instance => <InstanceLink instance={instance} /> },
    {
        key: "sockets",
        label: "Sockets",
        width: "minmax(0,0.6fr)",
        title: "Open WebSocket connections",
        render: instance => formatCount(instance.socketCount)
    },
    {
        key: "lastActive",
        label: "Last active",
        width: "minmax(0,0.9fr)",
        render: (instance, now) => (instance.lastActiveAtMs ? formatAgo(instance.lastActiveAtMs, now) : "—")
    },
    {
        key: "disk",
        label: "Disk used",
        width: "minmax(0,0.9fr)",
        title: "SQLite database size in the latest report",
        render: instance => (instance.databaseBytes == null ? "—" : formatBytes(instance.databaseBytes))
    }
]
const gridTemplateColumns = columns.map(column => column.width).join(" ")

export function useInstancesTable(actorName: string, live: boolean) {
    const [filter, setFilter] = useState<InstanceFilter>("all")
    const [searchText, setSearchText] = useState("")
    const [sort, setSort] = useState<Sort>(defaultSort)
    const [page, setPage] = useState(1)
    const search = useDebouncedValue(searchText.trim(), 250)
    const { data, error, isValidating, retry } = useInstancePage(
        {
            actorName,
            search,
            status: filter === "all" ? undefined : filter,
            sort: sort.key,
            descending: sort.descending,
            offset: (page - 1) * pageRows,
            limit: pageRows
        },
        live
    )
    const [summary, setSummary] = useState(data)
    if (data && data !== summary) setSummary(data)
    const pageCount = Math.max(1, Math.ceil((data?.total ?? 0) / pageRows))
    if (data && page > pageCount) setPage(pageCount)

    function resetPage<T>(update: (value: T) => void) {
        return (value: T) => {
            update(value)
            setPage(1)
        }
    }

    return {
        summary: summary && { actorName, total: summary.live + summary.dormant + summary.unknown, live: summary.live, dormant: summary.dormant },
        methods: summary?.methods,
        data,
        error,
        retry,
        isValidating,
        page,
        pageCount,
        setPage,
        filter,
        searchText,
        sort,
        onSearch: resetPage(setSearchText),
        onFilter: resetPage(setFilter),
        onSort: resetPage(setSort)
    }
}

export function InstancesCard({ table, now }: { table: ReturnType<typeof useInstancesTable>; now: number }) {
    const { summary, data, error, retry, isValidating, page, pageCount, setPage, filter, searchText, sort, onSearch, onFilter, onSort } = table
    return (
        <SectionCard
            title="Instances"
            aside={
                summary && (
                    <div className="flex flex-wrap items-center gap-2">
                        <SearchField value={searchText} onChange={onSearch} placeholder="Search instances" label="Search instances" />
                        <InstanceFilters summary={summary} value={filter} onChange={onFilter} />
                    </div>
                )
            }
            busy={!data && (!error || isValidating)}
            className="pb-3.5"
        >
            {!data && error && !isValidating ? (
                <div className="flex items-center gap-2 py-4 text-[13px] text-muted-foreground">
                    <p role="alert">Couldn't load instances.</p>
                    <Button type="button" variant="outline" size="xs" onClick={retry}>
                        Retry
                    </Button>
                </div>
            ) : data ? (
                <>
                    <InstanceTable rows={data.instances} sort={sort} onSort={onSort} now={now} />
                    {data.total > 0 && <Pagination page={page} pageCount={pageCount} pageSize={pageRows} total={data.total} noun="instances" onPage={setPage} />}
                </>
            ) : (
                <TableSkeleton />
            )}
        </SectionCard>
    )
}

function InstanceFilters({ summary: { total, live, dormant }, value, onChange }: { summary: ActorTypeSummary; value: InstanceFilter; onChange: (filter: InstanceFilter) => void }) {
    return (
        <SegmentedControl
            label="Filter instances"
            value={value}
            onChange={onChange}
            options={[
                { value: "all", label: "All", badge: formatCount(total) },
                { value: "live", label: "Live", badge: formatCount(live) },
                { value: "dormant", label: "Dormant", badge: formatCount(dormant) }
            ]}
        />
    )
}

type InstanceTableProps = { rows: InstanceRow[]; sort: Sort; onSort: (sort: Sort) => void; now: number }

function InstanceTable({ rows, sort, onSort, now }: InstanceTableProps) {
    if (!rows.length) return <p className="py-4 text-[13px] text-muted-foreground">No instances match these filters.</p>
    return (
        <div className="mt-3.5 overflow-x-auto">
            <div role="table" aria-label="Instances" className="min-w-[480px]">
                <div role="row" className="grid h-7.5 items-center gap-x-4 border-b px-2 text-xs text-muted-foreground" style={{ gridTemplateColumns }}>
                    {columns.map(column => (
                        <SortHeader key={column.key} column={column} sort={sort} onSort={onSort} />
                    ))}
                </div>
                {rows.map(row => (
                    <div
                        key={row.actorId}
                        role="row"
                        className="grid min-h-8.5 items-center gap-x-4 border-b border-border/55 px-2 text-[12.5px] tabular-nums last:border-b-0 hover:bg-muted/40"
                        style={{ gridTemplateColumns }}
                    >
                        {columns.map(column => (
                            <div key={column.key} role="cell" className={cn("min-w-0 truncate", column.key !== "instance" && "text-right text-muted-foreground")}>
                                {column.render(row, now)}
                            </div>
                        ))}
                    </div>
                ))}
            </div>
        </div>
    )
}

function SortHeader({ column, sort, onSort }: { column: Column; sort: Sort; onSort: (sort: Sort) => void }) {
    const active = sort.key === column.key
    // Names read A to Z first; every measure reads largest first.
    const next = active ? { key: column.key, descending: !sort.descending } : { key: column.key, descending: column.key !== "instance" }
    return (
        <span role="columnheader" aria-sort={active ? (sort.descending ? "descending" : "ascending") : "none"} className={cn("min-w-0", column.key !== "instance" && "text-right")}>
            <button
                type="button"
                title={column.title}
                onClick={() => onSort(next)}
                className={cn("inline-flex max-w-full items-center gap-1 rounded-sm outline-none hover:text-foreground focus-visible:ring-2 focus-visible:ring-ring", active && "text-foreground")}
            >
                <span className="truncate">{column.label}</span>
                {active && (sort.descending ? <ArrowDown className="size-3 shrink-0" /> : <ArrowUp className="size-3 shrink-0" />)}
            </button>
        </span>
    )
}

function InstanceLink({ instance }: { instance: InstanceRow }) {
    const live = instance.status === "live"
    return (
        <span className="flex min-w-0 items-center gap-2">
            <span title={live ? "Live" : "Dormant"} className={cn("size-2 shrink-0 rounded-full", live ? "bg-success" : "shadow-[inset_0_0_0_1.5px_var(--muted-foreground)] opacity-50")}>
                <span className="sr-only">{live ? "Live" : "Dormant"}</span>
            </span>
            <Link
                to={instancePath(instance.actorId)}
                translate="no"
                title={instance.actorId}
                className="truncate font-mono text-foreground underline decoration-border underline-offset-3 hover:decoration-foreground"
            >
                {instance.actorId}
            </Link>
        </span>
    )
}

function TableSkeleton() {
    return (
        <div aria-hidden className="mt-3.5">
            {Array.from({ length: 6 }, (_, index) => (
                <div key={index} className="grid h-8.5 items-center gap-x-4 border-b border-border/55 px-2" style={{ gridTemplateColumns }}>
                    {columns.map(column => (
                        <Skeleton key={column.key} className={cn("h-3.5", column.key === "instance" ? "w-3/4" : "ml-auto w-12")} />
                    ))}
                </div>
            ))}
        </div>
    )
}
