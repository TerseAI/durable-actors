import { ChevronLeft, ChevronRight } from "lucide-react"

import { formatCount } from "../../format.js"

import { Button } from "./button.js"

export function Pagination({ page, pageCount, pageSize, total, noun, onPage }: { page: number; pageCount: number; pageSize: number; total: number; noun: string; onPage: (page: number) => void }) {
    const first = (page - 1) * pageSize + 1
    const last = Math.min(page * pageSize, total)
    return (
        <div className="flex min-h-10 flex-wrap items-center justify-between gap-x-4 gap-y-2 border-t pt-2 text-xs text-muted-foreground tabular-nums">
            <span role="status">
                {formatCount(first)}–{formatCount(last)} of {formatCount(total)} {noun}
            </span>
            {pageCount > 1 && (
                <nav aria-label={`Pages of ${noun}`} className="flex items-center gap-2">
                    <Button type="button" variant="outline" size="icon-sm" aria-label="Previous page" disabled={page === 1} onClick={() => onPage(page - 1)}>
                        <ChevronLeft aria-hidden />
                    </Button>
                    <span>
                        Page {formatCount(page)} of {formatCount(pageCount)}
                    </span>
                    <Button type="button" variant="outline" size="icon-sm" aria-label="Next page" disabled={page === pageCount} onClick={() => onPage(page + 1)}>
                        <ChevronRight aria-hidden />
                    </Button>
                </nav>
            )}
        </div>
    )
}
