import type { EntryRecord } from "../types/Entries.js"

import type { Entry } from "./entries.js"

export const queryKeys = ["source", "level", "instance", "request", "connection", "operation", "outcome"] as const
export type QueryKey = (typeof queryKeys)[number]

/** `key:value` or `key:(a OR b)`, negated by a leading `-`. Clauses on the same key all apply. */
export type Clause = { key: QueryKey; values: string[]; negated: boolean }

export type Query = { clauses: Clause[]; terms: string[]; excludedTerms: string[] }

/** A whitespace-separated piece of the query; parentheses and quotes keep their spaces inside one token. */
export type Token = { text: string; start: number; end: number }

export const emptyQuery: Query = { clauses: [], terms: [], excludedTerms: [] }

const clausePattern = /^(-?)([a-z]+):(.*)$/su

export function parseQuery(text: string): Query {
    const query: Query = { clauses: [], terms: [], excludedTerms: [] }
    for (const { text: token } of tokenize(text)) {
        const clause = readClause(token)
        if (clause) query.clauses.push(clause)
        else if (clause === undefined && token !== "-") {
            if (token.startsWith("-")) query.excludedTerms.push(unquote(token.slice(1)))
            else query.terms.push(unquote(token))
        }
    }
    return query
}

export function tokenize(text: string): Token[] {
    const tokens: Token[] = []
    let start = -1
    let depth = 0
    let quoted = false
    for (let index = 0; index <= text.length; index++) {
        const char = text[index]
        if (index === text.length || (/\s/u.test(char!) && depth === 0 && !quoted)) {
            if (start >= 0) tokens.push({ text: text.slice(start, index), start, end: index })
            start = -1
            continue
        }
        if (start < 0) start = index
        if (char === '"') quoted = !quoted
        else if (!quoted && char === "(") depth++
        else if (!quoted && char === ")") depth = Math.max(0, depth - 1)
    }
    return tokens
}

/** The key a token filters on, when it is a clause: `-source:(a OR b)` gives "source". */
export function keyOf(token: string): QueryKey | null {
    const key = clausePattern.exec(token)?.[2]
    return isQueryKey(key) ? key : null
}

export function isQueryKey(value: string | undefined): value is QueryKey {
    return queryKeys.includes(value as QueryKey)
}

export function matches(entry: Entry, { clauses, terms, excludedTerms }: Query): boolean {
    for (const { key, values, negated } of clauses) {
        const value = fieldOf(entry, key)
        if ((value !== null && values.includes(value)) === negated) return false
    }
    const text = searchText(entry.record).toLowerCase()
    return terms.every(term => text.includes(term.toLowerCase())) && !excludedTerms.some(term => text.includes(term.toLowerCase()))
}

/** Whether an entry whose `key` is `value` can pass the clauses on that key. */
export function allows({ clauses }: Query, key: QueryKey, value: string): boolean {
    return clauses.every(clause => clause.key !== key || clause.values.includes(value) !== clause.negated)
}

/** Whether only entries that have `key` can match: entries of a source without it never do. */
export function requires({ clauses }: Query, key: QueryKey): boolean {
    return clauses.some(clause => clause.key === key && !clause.negated)
}

/** A value every match has, which a server can filter by. */
export function requiredValue({ clauses }: Query, key: QueryKey): string | null {
    return clauses.find(clause => clause.key === key && !clause.negated && clause.values.length === 1)?.values[0] ?? null
}

/** Whether the clauses on `key` amount to at most one required value. */
export function isSimple({ clauses }: Query, key: QueryKey): boolean {
    const own = clauses.filter(clause => clause.key === key)
    return own.length === 0 || (own.length === 1 && !own[0]!.negated && own[0]!.values.length === 1)
}

export function withoutKeys(query: Query, keys: QueryKey[]): Query {
    return { ...query, clauses: query.clauses.filter(clause => !keys.includes(clause.key)) }
}

/** The query text narrowed to `key:value`, replacing what it said about that key before. */
export function withClause(text: string, key: QueryKey, value: string): string {
    const kept = tokenize(text).filter(token => keyOf(token.text) !== key)
    return [...kept.map(token => token.text), `${key}:${formatValue(value)}`].join(" ")
}

export function formatValue(value: string): string {
    return /[\s()"]/u.test(value) || value === "OR" ? `"${value.replaceAll('"', "")}"` : value
}

/**
 * The search a URL describes. `source=rpc,websocket` is `source:(rpc OR websocket)`, `-source=console` is
 * `-source:console`, a repeated parameter adds another clause, and `q` takes free text or the whole syntax.
 */
export function queryFromParams(params: URLSearchParams): Query {
    const query = parseQuery(params.get("q") ?? "")
    for (const key of queryKeys)
        for (const negated of [false, true])
            for (const value of params.getAll(`${negated ? "-" : ""}${key}`)) {
                const values = value
                    .split(",")
                    .map(part => part.trim())
                    .filter(Boolean)
                if (values.length) query.clauses.push({ key, values, negated })
            }
    return query
}

/** The URL parameters that describe a search, with null for each one it leaves out. */
export function queryParams({ clauses, terms, excludedTerms }: Query): Record<string, string[] | string | null> {
    const params: Record<string, string[] | string | null> = { q: [...terms.map(formatTerm), ...excludedTerms.map(term => `-${formatTerm(term)}`)].join(" ") || null }
    for (const key of queryKeys)
        for (const negated of [false, true]) {
            const values = clauses.filter(clause => clause.key === key && clause.negated === negated).map(clause => clause.values.join(","))
            params[`${negated ? "-" : ""}${key}`] = values.length ? values : null
        }
    return params
}

export function formatQuery({ clauses, terms, excludedTerms }: Query): string {
    const formatted = sortedClauses(clauses).map(({ key, values, negated }) => {
        const value = values.length === 1 ? formatValue(values[0]!) : `(${values.map(formatValue).join(" OR ")})`
        return `${negated ? "-" : ""}${key}:${value}`
    })
    return [...formatted, ...terms.map(formatTerm), ...excludedTerms.map(term => `-${formatTerm(term)}`)].join(" ")
}

/** The same text for searches that mean the same, whatever order their clauses came in. */
export function canonicalQuery(query: Query): string {
    return JSON.stringify({ ...query, clauses: sortedClauses(query.clauses) })
}

function sortedClauses(clauses: Clause[]): Clause[] {
    return [...clauses].sort((a, b) => queryKeys.indexOf(a.key) - queryKeys.indexOf(b.key) || Number(a.negated) - Number(b.negated))
}

// Quotes a term that would otherwise read as a filter, a negation or several words.
function formatTerm(term: string): string {
    return /[\s()":]/u.test(term) || term.startsWith("-") ? `"${term.replaceAll('"', "")}"` : term
}

export function fieldOf({ source, level, record }: Entry, key: QueryKey): string | null {
    switch (key) {
        case "source":
            return source
        case "level":
            return level
        case "instance":
            return record.item.actorId
        case "request":
            return record.type === "request" || record.type === "log" ? record.item.requestId : null
        case "connection":
            return record.type === "request" || record.type === "connectionOpened" || record.type === "connectionClosed" ? record.item.connectionId : null
        case "operation":
            return record.type === "request" ? record.item.operation : null
        case "outcome":
            return record.type === "request" ? record.item.outcome : null
    }
}

// Mirrors what the server's log search matches; other records match on their IDs and words.
function searchText(record: EntryRecord): string {
    switch (record.type) {
        case "request":
            return `${record.item.operation} ${record.item.requestId} ${record.item.connectionId ?? ""}`
        case "log":
            return record.item.message
        case "connectionOpened":
            return `${record.item.connectionId} ${JSON.stringify(record.item.metadata ?? "")}`
        case "connectionClosed":
            return `${record.item.connectionId} ${record.item.closeReason ?? ""}`
        case "kill":
            return record.item.reason
        case "lifecycle":
            return record.item.kind === "started" ? "woke" : "slept"
    }
}

/** A clause, null for an unfinished one such as `source:`, or undefined when the token is free text. */
function readClause(token: string): Clause | null | undefined {
    const match = clausePattern.exec(token)
    if (!match || !isQueryKey(match[2])) return undefined
    const values = readValues(match[3]!)
    return values.length ? { key: match[2], values, negated: match[1] === "-" } : null
}

function readValues(raw: string): string[] {
    if (!raw.startsWith("(")) return unquote(raw) ? [unquote(raw)] : []
    const inner = raw.slice(1, raw.endsWith(")") ? -1 : undefined)
    return tokenize(inner)
        .map(token => token.text)
        .filter(value => value !== "OR")
        .map(unquote)
        .filter(Boolean)
}

function unquote(value: string): string {
    return value.replace(/^"/u, "").replace(/"$/u, "")
}
