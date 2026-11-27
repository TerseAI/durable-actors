import type { ReactNode } from "react"

import { cn } from "../../lib/utils.js"

const loadAheadPx = 240

export type Tone = "faint" | "muted" | "warn" | "danger" | "good"

const tones: Record<Tone, string> = {
    faint: "border-border/60 text-muted-foreground/70",
    muted: "text-muted-foreground",
    warn: "border-warning/35 bg-warning/10 text-warning",
    danger: "border-danger/40 bg-danger/15 text-danger",
    good: "border-success/30 bg-success/10 text-success"
}

/** Scrolls on its own and asks for older records as its end comes into view. */
export function ListScroller({ onReachEnd, children }: { onReachEnd: (() => void) | undefined; children: ReactNode }) {
    return (
        <div
            className="max-h-[640px] overflow-y-auto"
            onScroll={event => {
                const box = event.currentTarget
                if (onReachEnd && box.scrollHeight - box.scrollTop - box.clientHeight < loadAheadPx) onReachEnd()
            }}
        >
            {children}
        </div>
    )
}

export type EmptyListProps = {
    noun: string
    filtered: boolean
    onClearFilters: () => void
    /** Older records remain unread, so matches may still exist. */
    onLoadOlder?: () => void
}

export function EmptyList({ noun, filtered, onClearFilters, onLoadOlder }: EmptyListProps) {
    return (
        <p className="py-8 text-center text-[13px] text-muted-foreground">
            {filtered ? `No ${noun} match these filters${onLoadOlder ? " among the newest records" : ""}.` : onLoadOlder ? `No recent ${noun}.` : `No ${noun} in this time range.`}
            {onLoadOlder && (
                <>
                    {" "}
                    <TextButton onClick={onLoadOlder}>Load older</TextButton>
                </>
            )}
            {filtered && (
                <>
                    {" "}
                    <TextButton onClick={onClearFilters}>Clear filters</TextButton>
                </>
            )}
        </p>
    )
}

export function TextButton({ onClick, children }: { onClick: () => void; children: ReactNode }) {
    return (
        <button type="button" onClick={onClick} className="font-medium text-foreground underline decoration-border underline-offset-3 hover:decoration-foreground">
            {children}
        </button>
    )
}

export function Tag({ tone, className, children }: { tone: Tone; className?: string; children: ReactNode }) {
    return <span className={cn("justify-self-start rounded-sm border px-1.5 py-px text-[9.5px] font-bold tracking-[0.07em] whitespace-nowrap uppercase", tones[tone], className)}>{children}</span>
}

/** Marks each case-insensitive match of the search in the text. */
export function highlight(text: string, search: string): ReactNode {
    if (!search) return text
    const lower = text.toLowerCase()
    const needle = search.toLowerCase()
    const parts: ReactNode[] = []
    let at = 0
    for (let index = lower.indexOf(needle); index >= 0; index = lower.indexOf(needle, at)) {
        parts.push(
            text.slice(at, index),
            <mark key={index} className="rounded-[2px] bg-primary/25 text-foreground shadow-[inset_0_-1px_0_var(--primary)]">
                {text.slice(index, index + needle.length)}
            </mark>
        )
        at = index + needle.length
    }
    parts.push(text.slice(at))
    return parts
}
