import { type NavigateOptions, useSearchParams } from "react-router"

/** A value per parameter: several values repeat it, and null or "" removes it. */
export type ParamChanges = Record<string, string | string[] | null>

/** Page state kept in the URL, so a view can be linked to and survives switching tabs. */
export function useUrlParams() {
    const [params, setParams] = useSearchParams()
    // Applies every change in one navigation: separate calls in one event would each start from the same URL.
    const update = (changes: ParamChanges, { push = false, state }: { push?: boolean; state?: NavigateOptions["state"] } = {}) =>
        setParams(
            previous => {
                const next = new URLSearchParams(previous)
                for (const [name, value] of Object.entries(changes)) {
                    next.delete(name)
                    for (const each of Array.isArray(value) ? value : [value]) if (each) next.append(name, each)
                }
                return next
            },
            { replace: !push, state }
        )
    return { params, update }
}
