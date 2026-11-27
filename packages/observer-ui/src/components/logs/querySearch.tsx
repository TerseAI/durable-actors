import { type KeyboardEvent, type ReactNode, useLayoutEffect, useRef, useState } from "react"

import { Search, X } from "lucide-react"

import { formatCount } from "../../format.js"
import { type QueryKey, type Token, formatValue, isQueryKey, keyOf, tokenize } from "../../lib/query.js"
import { cn } from "../../lib/utils.js"

export type Suggestion = { value: string; label?: string; count?: number }

export type QuerySearchProps = {
    value: string
    onChange: (value: string) => void
    /** The keys to suggest, with what each one filters on. */
    keys: [QueryKey, string][]
    valuesFor: (key: QueryKey) => Suggestion[]
    placeholder: string
}

type Context = { kind: "key"; token: Token; prefix: string; negated: boolean } | { kind: "value"; token: Token; key: QueryKey; partial: string; replaceFrom: number; grouped: boolean; spaced: boolean }

type Option = { id: string; label: ReactNode; aside: ReactNode; accept: () => void }

const maxOptions = 8
const listId = "logs-search-suggestions"
const textStyle = "font-mono text-[12.5px] leading-9 whitespace-pre"

/** The Logs search: free text plus `key:value` filters, with suggestions for the key or value at the caret. */
export function QuerySearch({ value, onChange, keys, valuesFor, placeholder }: QuerySearchProps) {
    const input = useRef<HTMLInputElement>(null)
    const mirror = useRef<HTMLDivElement>(null)
    const pendingCaret = useRef<number | null>(null)
    const [caret, setCaret] = useState(value.length)
    const [focused, setFocused] = useState(false)
    const [dismissed, setDismissed] = useState(false)
    const [active, setActive] = useState(0)
    const context = contextAt(value, caret)
    const options = focused && !dismissed && context ? optionsFor(context, value, keys, valuesFor, edit) : []
    const open = options.length > 0
    const activeIndex = Math.min(active, options.length - 1)

    useLayoutEffect(() => {
        if (pendingCaret.current === null || !input.current) return
        input.current.setSelectionRange(pendingCaret.current, pendingCaret.current)
        setCaret(pendingCaret.current)
        pendingCaret.current = null
        syncScroll()
    }, [value])

    function edit(start: number, end: number, insert: string) {
        pendingCaret.current = start + insert.length
        setActive(0)
        onChange(value.slice(0, start) + insert + value.slice(end))
    }

    function syncScroll() {
        if (input.current && mirror.current) mirror.current.style.transform = `translateX(${-input.current.scrollLeft}px)`
    }

    function readCaret() {
        setCaret(input.current?.selectionStart ?? value.length)
        syncScroll()
    }

    function onKeyDown(event: KeyboardEvent<HTMLInputElement>) {
        if (event.key === "ArrowDown" || event.key === "ArrowUp") {
            event.preventDefault()
            setDismissed(false)
            if (open) setActive((activeIndex + (event.key === "ArrowDown" ? 1 : options.length - 1)) % options.length)
        } else if ((event.key === "Enter" || event.key === "Tab") && open) {
            event.preventDefault()
            options[activeIndex]!.accept()
        } else if (event.key === "Escape") {
            if (open) setDismissed(true)
            else onChange("")
        }
    }

    return (
        <div className="relative mt-3">
            <div className="flex h-9 items-center gap-2 rounded-lg border bg-background/40 px-2.5 focus-within:border-ring focus-within:ring-2 focus-within:ring-ring/40">
                <Search aria-hidden className="size-3.5 shrink-0 text-muted-foreground" />
                <div className="relative h-full min-w-0 flex-1 overflow-hidden">
                    <div ref={mirror} aria-hidden className={cn("pointer-events-none absolute inset-y-0 left-0 text-foreground", textStyle)}>
                        <Highlighted text={value} />
                    </div>
                    <input
                        ref={input}
                        value={value}
                        maxLength={512}
                        spellCheck={false}
                        autoComplete="off"
                        placeholder={placeholder}
                        role="combobox"
                        aria-label="Search logs"
                        aria-expanded={open}
                        aria-controls={listId}
                        aria-activedescendant={open ? options[activeIndex]!.id : undefined}
                        onChange={event => {
                            setDismissed(false)
                            setActive(0)
                            setCaret(event.target.selectionStart ?? event.target.value.length)
                            onChange(event.target.value)
                        }}
                        onSelect={readCaret}
                        onScroll={syncScroll}
                        onFocus={() => setFocused(true)}
                        onBlur={() => setFocused(false)}
                        onKeyDown={onKeyDown}
                        className={cn(
                            "relative h-full w-full bg-transparent text-transparent caret-foreground outline-none selection:bg-chart-2/30 placeholder:font-sans placeholder:text-muted-foreground",
                            textStyle
                        )}
                    />
                </div>
                {value && (
                    <button
                        type="button"
                        aria-label="Clear search"
                        onClick={() => onChange("")}
                        className="grid size-4.5 shrink-0 place-items-center rounded-full bg-muted text-muted-foreground hover:text-foreground"
                    >
                        <X className="size-2.5" />
                    </button>
                )}
            </div>
            {open && <SuggestionList options={options} activeIndex={activeIndex} onHover={setActive} />}
        </div>
    )
}

function SuggestionList({ options, activeIndex, onHover }: { options: Option[]; activeIndex: number; onHover: (index: number) => void }) {
    return (
        <div className="absolute top-full left-0 z-30 mt-1.5 w-full max-w-xl overflow-hidden rounded-lg border bg-popover text-popover-foreground shadow-lg">
            <ul id={listId} role="listbox" aria-label="Suggestions" className="py-1">
                {options.map((option, index) => (
                    <li
                        key={option.id}
                        id={option.id}
                        role="option"
                        aria-selected={index === activeIndex}
                        onPointerMove={() => onHover(index)}
                        // Keeps focus in the input, which the suggestion edits.
                        onMouseDown={event => event.preventDefault()}
                        onClick={option.accept}
                        className="flex h-8 cursor-pointer items-center justify-between gap-4 px-3 text-[13px] aria-selected:bg-muted"
                    >
                        <span className="min-w-0 truncate font-mono text-[12.5px]">{option.label}</span>
                        <span className="shrink-0 text-xs text-muted-foreground tabular-nums">{option.aside}</span>
                    </li>
                ))}
            </ul>
            <div className="flex flex-wrap items-center gap-x-4 gap-y-1 border-t px-3 py-2 text-xs text-muted-foreground">
                <span>
                    Negation <Syntax>-key:a</Syntax>
                </span>
                <span>
                    Union <Syntax>key:(a OR b)</Syntax>
                </span>
            </div>
        </div>
    )
}

function Syntax({ children }: { children: ReactNode }) {
    return <code className="ml-1 rounded-sm border px-1.5 py-px font-mono text-[11.5px] text-foreground/90">{children}</code>
}

// Draws the query behind the transparent input: filter keys as tokens, their values in the ID colour.
function Highlighted({ text }: { text: string }) {
    const parts: ReactNode[] = []
    let at = 0
    for (const token of tokenize(text)) {
        parts.push(text.slice(at, token.start))
        const key = keyOf(token.text)
        if (key) {
            const keyEnd = token.text.indexOf(":") + 1
            parts.push(
                <span key={token.start} className="rounded-sm bg-muted [box-shadow:0_0_0_2px_var(--muted)]">
                    {token.text.slice(0, keyEnd)}
                </span>,
                <span key={`${token.start}-value`} className="text-chart-2">
                    {token.text.slice(keyEnd)}
                </span>
            )
        } else parts.push(<span key={token.start}>{token.text}</span>)
        at = token.end
    }
    parts.push(text.slice(at))
    return <>{parts}</>
}

function contextAt(text: string, caret: number): Context | null {
    const token = tokenize(text).find(candidate => candidate.start <= caret && caret <= candidate.end) ?? { text: "", start: caret, end: caret }
    const before = token.text.slice(0, caret - token.start)
    const typingKey = /^(-?)([a-z]*)$/u.exec(before)
    if (typingKey) return { kind: "key", token, prefix: typingKey[2]!, negated: typingKey[1] === "-" }
    const typingValue = /^(-?)([a-z]+):(\(?)(.*)$/su.exec(before)
    if (!typingValue || !isQueryKey(typingValue[2])) return null
    const grouped = typingValue[3] === "("
    const rest = typingValue[4]!
    const last = grouped ? rest.slice(rest.search(/\S*$/u)) : rest
    // Right after `OR`, the next value starts a new word.
    if (last === "OR") return { kind: "value", token, key: typingValue[2], partial: "", replaceFrom: caret, grouped, spaced: true }
    return { kind: "value", token, key: typingValue[2], partial: last.replace(/^"/u, ""), replaceFrom: caret - last.length, grouped, spaced: false }
}

function optionsFor(context: Context, text: string, keys: [QueryKey, string][], valuesFor: (key: QueryKey) => Suggestion[], edit: (start: number, end: number, insert: string) => void): Option[] {
    if (context.kind === "key") {
        const { token, prefix, negated } = context
        // Keys show on an empty search, and otherwise once a word starts like one.
        if (!prefix && text.trim()) return []
        return keys
            .filter(([key]) => key.startsWith(prefix) && key !== prefix)
            .map(([key, description]) => ({
                id: `${listId}-key-${key}`,
                label: `${negated ? "-" : ""}${key}:`,
                aside: description,
                accept: () => edit(token.start, token.end, `${negated ? "-" : ""}${key}:`)
            }))
    }
    const { token, key, partial, replaceFrom, grouped, spaced } = context
    const needle = partial.toLowerCase()
    return valuesFor(key)
        .filter(suggestion => (suggestion.label ?? suggestion.value).toLowerCase().includes(needle) || suggestion.value.toLowerCase().includes(needle))
        .slice(0, maxOptions)
        .map(suggestion => ({
            id: `${listId}-value-${suggestion.value}`,
            label: suggestion.label ?? suggestion.value,
            aside: suggestion.count === undefined ? null : formatCount(suggestion.count),
            accept: () => {
                const insert = `${spaced ? " " : ""}${formatValue(suggestion.value)}`
                const caret = replaceFrom + (spaced ? 0 : partial.length + (text[replaceFrom] === '"' ? 1 : 0))
                if (grouped) edit(replaceFrom, caret, insert)
                else edit(replaceFrom, token.end, token.end === text.length ? `${insert} ` : insert)
            }
        }))
}
