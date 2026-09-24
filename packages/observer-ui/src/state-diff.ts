import { create } from "jsondiffpatch"
import { format } from "jsondiffpatch/formatters/html"

const differ = create({ arrays: { detectMove: false } })
export function stateDiff(before: object, after: object): string {
    const delta = differ.diff(before, after)
    return delta ? (format(delta, before) ?? "") : ""
}
