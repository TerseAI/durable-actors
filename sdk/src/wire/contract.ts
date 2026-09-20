import type { JSONSchema7 } from "json-schema"

interface SocketContract {
    readonly version: 1
    readonly actorName: string
    readonly schema: JSONSchema7
    readonly emittable: readonly string[]
}

export type { SocketContract }
