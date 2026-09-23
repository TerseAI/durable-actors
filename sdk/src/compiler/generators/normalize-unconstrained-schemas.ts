import type { JSONSchema7, JSONSchema7Definition } from "json-schema"

function normalizeUnconstrainedSchemas(schema: JSONSchema7Definition): JSONSchema7Definition {
    if (typeof schema === "boolean") return schema
    const annotations = [
        "$schema",
        "$comment",
        "title",
        "description",
        "default",
        "examples",
        "readOnly",
        "writeOnly",
        "definitions"
    ]
    // The declaration library treats named empty schemas as objects; a true branch keeps them unconstrained.
    if (Object.keys(schema).every(key => annotations.includes(key))) return { ...schema, anyOf: [true] }
    return normalizeChildren(schema)
}

function normalizeChildren(schema: JSONSchema7): JSONSchema7 {
    const result = { ...schema }
    for (const key of ["definitions", "properties", "patternProperties"] as const)
        if (result[key])
            result[key] = Object.fromEntries(
                Object.entries(result[key]).map(([name, child]) => [name, normalizeUnconstrainedSchemas(child)])
            )
    for (const key of [
        "items",
        "additionalItems",
        "additionalProperties",
        "contains",
        "propertyNames",
        "not",
        "if",
        "then",
        "else",
        "allOf",
        "anyOf",
        "oneOf"
    ] as const) {
        const child = result[key]
        if (child !== undefined)
            Object.assign(result, {
                [key]: Array.isArray(child)
                    ? child.map(normalizeUnconstrainedSchemas)
                    : normalizeUnconstrainedSchemas(child)
            })
    }
    return result
}

export { normalizeUnconstrainedSchemas }
