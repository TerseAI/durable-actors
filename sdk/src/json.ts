import { z } from "zod"

import { cloneJson } from "./client-runtime/json.js"
import { ActorSerializationError } from "./errors.js"

type JsonPrimitive = string | number | boolean | null
type JsonObject = { readonly [key: string]: JsonValue }
type JsonValue = JsonPrimitive | JsonObject | readonly JsonValue[]

function cloneJsonObject(value: unknown, label: string): JsonObject {
    const cloned = cloneJson(value, label)
    if (!isJsonObject(cloned)) throw new ActorSerializationError(`${label} must be a JSON object`)
    return cloned
}

function isJsonObject(value: JsonValue): value is JsonObject {
    return typeof value === "object" && value !== null && !Array.isArray(value)
}

const jsonValueSchema: z.ZodType<JsonValue> = z.lazy(() =>
    z.union([
        z.string(),
        z.number(),
        z.boolean(),
        z.null(),
        z.array(jsonValueSchema),
        z.record(z.string(), jsonValueSchema)
    ])
)

export { cloneJson, cloneJsonObject, isJsonObject, jsonValueSchema }
export type { JsonPrimitive, JsonObject, JsonValue }
