import { useId, useState } from "react"
import { useLocation, useNavigationType } from "react-router"

import { type Query, canonicalQuery, formatQuery, parseQuery, queryFromParams, queryParams } from "../lib/query.js"

import { useDebouncedValue } from "./useDebouncedValue.js"
import { useUrlParams } from "./useUrlParams.js"

const searchDelayMs = 300

/**
 * The Logs search, kept in the URL as one parameter per filter so any link opens the same filters. The text
 * stays as typed; the URL changes with each edit, and a change that comes from elsewhere replaces the text.
 */
export function useLogQuery(): { text: string; setText: (text: string) => void; query: Query } {
    const { params, update } = useUrlParams()
    const { state } = useLocation()
    const navigationType = useNavigationType()
    const editorId = useId()
    const fromUrl = queryFromParams(params)
    const urlKey = canonicalQuery(fromUrl)
    const [text, setTypedText] = useState(() => formatQuery(fromUrl))
    const [seenUrlKey, setSeenUrlKey] = useState(urlKey)
    if (urlKey !== seenUrlKey) {
        setSeenUrlKey(urlKey)
        // Our URL updates can arrive after the draft has changed again.
        if (navigationType !== "REPLACE" || state?.logQueryEditor !== editorId) setTypedText(formatQuery(fromUrl))
    }
    const setText = (next: string) => {
        setTypedText(next)
        update(queryParams(parseQuery(next)), { state: { logQueryEditor: editorId } })
    }
    return { text, setText, query: parseQuery(useDebouncedValue(text, searchDelayMs)) }
}
