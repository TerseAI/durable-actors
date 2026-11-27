import { Fragment, type ReactNode, useState } from "react"

import { Check, Copy } from "lucide-react"

import { formatCount } from "../../format.js"
import { type Json, summarizeJson } from "../../lib/logs.js"

import { Button } from "./button.js"
import { highlight } from "./recordList.js"

// A quoted key with its colon, a quoted string, or a number, boolean, or null in pretty-printed JSON.
const jsonToken = /("(?:\\.|[^"\\])*")(?=\s*:)\s*:|("(?:\\.|[^"\\])*")|-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?|\b(?:true|false|null)\b/gu
// Highlighting and laying out more than this stalls the page; copying still gets the whole value.
const shownChars = 20_000

export type JsonContainer = Json[] | { [key: string]: Json }

/** One line: the value's headline field, then its first fields. */
export function JsonSummary({ value, search, tagged = false }: { value: JsonContainer; search: string; tagged?: boolean }) {
    const { headline, fields } = summarizeJson(value)
    return (
        <span className="flex min-w-0 items-center gap-2 font-mono">
            {tagged && <span className="shrink-0 rounded-sm border border-chart-2/40 px-1 font-sans text-[9px] font-bold tracking-[0.07em] text-chart-2">JSON</span>}
            <span className="truncate">
                {headline && <span className="mr-2.5 font-medium text-foreground">{highlight(headline, search)}</span>}
                {fields.map(([key, field], index) => (
                    <Fragment key={key}>
                        {index > 0 && <span className="text-muted-foreground"> · </span>}
                        <span className="text-muted-foreground">{key} </span>
                        <span className="text-foreground/95">{highlight(field, search)}</span>
                    </Fragment>
                ))}
            </span>
        </span>
    )
}

/** Pretty-printed and highlighted; a very large value shows its beginning. */
export function JsonBlock({ value }: { value: unknown }) {
    const text = JSON.stringify(value, null, 2) ?? "null"
    const cut = text.length > shownChars
    return (
        <>
            <pre className="max-h-96 overflow-auto rounded-lg border bg-(--code-bg) px-3.5 py-2.5 font-mono text-xs leading-relaxed break-words whitespace-pre-wrap">
                {highlightJson(cut ? text.slice(0, shownChars) : text)}
                {cut && "\n…"}
            </pre>
            {cut && (
                <p className="mt-1.5 text-xs text-muted-foreground">
                    Showing the first {formatCount(shownChars)} of {formatCount(text.length)} characters. Copy it for the whole value.
                </p>
            )}
        </>
    )
}

export function CopyJsonButton({ value }: { value: unknown }) {
    const [copied, setCopied] = useState(false)
    return (
        <Button
            variant="outline"
            size="xs"
            onClick={() =>
                void navigator.clipboard.writeText(JSON.stringify(value, null, 2)).then(() => {
                    setCopied(true)
                    setTimeout(() => setCopied(false), 1500)
                })
            }
        >
            {copied ? <Check /> : <Copy />}
            {copied ? "Copied" : "Copy JSON"}
        </Button>
    )
}

function highlightJson(text: string): ReactNode[] {
    const parts: ReactNode[] = []
    let at = 0
    for (const match of text.matchAll(jsonToken)) {
        const [token, key, string] = match
        parts.push(
            text.slice(at, match.index),
            <span key={match.index} className={key ? "text-(--tk-plain)" : string ? "text-(--tk-string)" : "text-(--tk-type)"}>
                {key ?? token}
            </span>
        )
        if (key) parts.push(token.slice(key.length))
        at = match.index + token.length
    }
    parts.push(text.slice(at))
    return parts
}
