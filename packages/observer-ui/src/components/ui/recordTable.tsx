import { type ReactNode, useState } from "react"

import { ChevronRight, Columns3 } from "lucide-react"

import { cn } from "../../lib/utils.js"

import { Button } from "./button.js"
import { Popover, PopoverContent, PopoverTrigger } from "./popover.js"
import { EmptyList, ListScroller, TextButton } from "./recordList.js"
import { Skeleton } from "./skeleton.js"

const rowGrid = "grid items-center gap-x-4"
const storagePrefix = "durable-actors-observer:columns:"

export type Column<R> = {
    id: string
    label: string
    /** A grid track, as "104px" or "minmax(0,1fr)". */
    width: string
    align?: "right"
    /** Explains the column on hover. */
    title?: string
    /** Whether the Columns menu offers to hide it. */
    hideable?: boolean
    hiddenByDefault?: boolean
    render: (row: R) => ReactNode
}

export type RecordTableProps<R> = {
    columns: Column<R>[]
    rows: R[] | null
    rowKey: (row: R) => string
    /** Plural, as in "No logs in this time range." */
    noun: string
    filtered: boolean
    onClearFilters: () => void
    onReachEnd?: () => void
    loadingOlder?: boolean
    onOpen: (row: R, trigger: HTMLElement) => void
    selectedKey?: string | null
}

export function RecordTable<R>({ columns, rows, rowKey, noun, filtered, onClearFilters, onReachEnd, loadingOlder, onOpen, selectedKey }: RecordTableProps<R>) {
    const gridTemplateColumns = `${columns.map(column => column.width).join(" ")} 16px`
    if (!rows || (!rows.length && loadingOlder)) return <TableSkeleton columns={columns} gridTemplateColumns={gridTemplateColumns} />
    if (!rows.length) return <EmptyList noun={noun} filtered={filtered} onClearFilters={onClearFilters} onLoadOlder={onReachEnd} />
    return (
        <ListScroller onReachEnd={onReachEnd}>
            <TableHeader columns={columns} gridTemplateColumns={gridTemplateColumns} />
            <ol>
                {rows.map(row => {
                    const key = rowKey(row)
                    const open = (trigger: HTMLElement) => onOpen(row, trigger)
                    return (
                        <li key={key} className={cn("border-b border-border/55 px-2 last:border-b-0", selectedKey === key && "bg-muted/50")}>
                            <div
                                onClick={event => open(event.currentTarget.querySelector<HTMLButtonElement>('[aria-haspopup="dialog"]') ?? event.currentTarget)}
                                className={`${rowGrid} min-h-8.5 cursor-pointer text-[12.5px]`}
                                style={{ gridTemplateColumns }}
                            >
                                {columns.map(column => (
                                    <div key={column.id} className={cn("min-w-0 truncate", column.align === "right" && "text-right")}>
                                        {column.render(row)}
                                    </div>
                                ))}
                                <RowButton onPress={open} />
                            </div>
                        </li>
                    )
                })}
            </ol>
            {loadingOlder ? (
                <div aria-hidden className="border-t border-border/55">
                    <SkeletonRows columns={columns} gridTemplateColumns={gridTemplateColumns} rows={3} />
                </div>
            ) : (
                // A short list never scrolls, so it asks for older records here.
                onReachEnd && (
                    <p className="border-t border-border/55 py-2.5 text-center text-xs text-muted-foreground">
                        <TextButton onClick={onReachEnd}>Load older {noun}</TextButton>
                    </p>
                )
            )}
        </ListScroller>
    )
}

/** The columns the table shows: every column except those hidden. */
export function shownColumns<R>(columns: Column<R>[], hidden: ReadonlySet<string>): Column<R>[] {
    return columns.filter(column => !column.hideable || !hidden.has(column.id))
}

/** Which columns a table hides, remembered per browser; storage can be unavailable, so it falls back to the defaults. */
export function useHiddenColumns<R>(table: string, columns: Column<R>[]): [ReadonlySet<string>, (hidden: ReadonlySet<string>) => void] {
    const [hidden, setHidden] = useState<ReadonlySet<string>>(() => readHidden(table, columns))
    const update = (next: ReadonlySet<string>) => {
        setHidden(next)
        try {
            localStorage.setItem(storagePrefix + table, JSON.stringify([...next]))
        } catch {
            // Keep the choice for this page only.
        }
    }
    return [hidden, update]
}

export function ColumnsMenu<R>({ columns, hidden, onChange }: { columns: Column<R>[]; hidden: ReadonlySet<string>; onChange: (hidden: ReadonlySet<string>) => void }) {
    const toggle = (id: string) => {
        const next = new Set(hidden)
        if (!next.delete(id)) next.add(id)
        onChange(next)
    }
    return (
        <Popover>
            <PopoverTrigger asChild>
                <Button variant="outline" size="sm" className="h-8 text-xs">
                    <Columns3 />
                    Columns
                </Button>
            </PopoverTrigger>
            <PopoverContent align="end" className="w-44 p-1.5">
                {columns
                    .filter(column => column.hideable)
                    .map(column => (
                        <label key={column.id} className="flex cursor-pointer items-center gap-2.5 rounded-sm px-2 py-1.5 text-[13px] hover:bg-muted">
                            <input type="checkbox" checked={!hidden.has(column.id)} onChange={() => toggle(column.id)} className="accent-primary" />
                            {column.label}
                        </label>
                    ))}
            </PopoverContent>
        </Popover>
    )
}

function readHidden<R>(table: string, columns: Column<R>[]): ReadonlySet<string> {
    try {
        const stored = localStorage.getItem(storagePrefix + table)
        if (stored) return new Set(JSON.parse(stored) as string[])
    } catch {
        // Fall through to the defaults.
    }
    return new Set(columns.filter(column => column.hiddenByDefault).map(column => column.id))
}

type HeaderProps<R> = { columns: Column<R>[]; gridTemplateColumns: string }

function TableHeader<R>({ columns, gridTemplateColumns }: HeaderProps<R>) {
    return (
        <div className={`${rowGrid} sticky top-0 z-[1] h-7.5 border-b bg-card px-2 text-xs text-muted-foreground`} style={{ gridTemplateColumns }}>
            {columns.map(column => (
                <span key={column.id} title={column.title} className={cn("truncate", column.align === "right" && "text-right")}>
                    {column.label}
                </span>
            ))}
        </div>
    )
}

function RowButton({ onPress }: { onPress: (trigger: HTMLElement) => void }) {
    return (
        <button
            type="button"
            aria-haspopup="dialog"
            aria-label="Show details"
            onClick={event => {
                event.stopPropagation()
                onPress(event.currentTarget)
            }}
            className="rounded-sm text-muted-foreground outline-none hover:text-foreground focus-visible:ring-2 focus-visible:ring-ring"
        >
            <ChevronRight className="size-3.5" />
        </button>
    )
}

function TableSkeleton<R>({ columns, gridTemplateColumns }: { columns: Column<R>[]; gridTemplateColumns: string }) {
    return (
        <div aria-hidden>
            <TableHeader columns={columns} gridTemplateColumns={gridTemplateColumns} />
            <SkeletonRows columns={columns} gridTemplateColumns={gridTemplateColumns} rows={6} />
        </div>
    )
}

function SkeletonRows<R>({ columns, gridTemplateColumns, rows }: { columns: Column<R>[]; gridTemplateColumns: string; rows: number }) {
    return Array.from({ length: rows }, (_, row) => (
        <div key={row} className={`${rowGrid} h-8.5 border-b border-border/55 px-2 last:border-b-0`} style={{ gridTemplateColumns }}>
            {columns.map(column => (
                <Skeleton key={column.id} className={cn("h-3.5", column.align === "right" && "ml-auto")} style={{ width: column.width.endsWith("px") ? "70%" : `${35 + ((row * 23) % 50)}%` }} />
            ))}
        </div>
    ))
}
