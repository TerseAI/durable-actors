import { ActorSerializationError } from "./errors.js"

type JsonPrimitive = string | number | boolean | null
type JsonObject = { readonly [key: string]: JsonValue }
type JsonValue = JsonPrimitive | JsonObject | readonly JsonValue[]

function cloneJson(value: unknown, label: string): JsonValue {
    try {
        const encoded = JSON.stringify(value)
        if (encoded === undefined) throw new ActorSerializationError(`${label} must be JSON serializable`)
        return JSON.parse(encoded) as JsonValue
    } catch (error) {
        if (error instanceof ActorSerializationError) throw error
        throw new ActorSerializationError(`${label} must be JSON serializable`, { cause: error })
    }
}

export { cloneJson }
export type { JsonValue }
