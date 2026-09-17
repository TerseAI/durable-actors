import type { JSONSchema7, JSONSchema7Definition } from "json-schema"

import { ActorDefinitionError } from "../errors.js"

function extractPublicSchema(schema: JSONSchema7, roots: readonly string[]): JSONSchema7 {
    const definitions = schema.definitions ?? {}
    const names = new Map(roots.map(name => [name, name]))
    const pending = [...roots]
    let nextType = 0
    const rename = (reference: string) => {
        if (!reference.startsWith("#/definitions/"))
            throw new ActorDefinitionError(`public contracts require local type references: ${reference}`)
        const original = decodeURIComponent(reference.slice("#/definitions/".length))
            .replaceAll("~1", "/")
            .replaceAll("~0", "~")
        if (!Object.hasOwn(definitions, original))
            throw new ActorDefinitionError(`public contract type reference is missing: ${reference}`)
        if (!names.has(original)) {
            let name: string
            do name = `Type${nextType++}`
            while (roots.includes(name))
            names.set(original, name)
            pending.push(original)
        }
        return `#/definitions/${names.get(original)!}`
    }
    const publicDefinitions: [string, JSONSchema7Definition][] = []
    for (let index = 0; index < pending.length; index++) {
        const original = pending[index]
        publicDefinitions.push([
            names.get(original)!,
            rewriteReferences(definitions[original], rename) as JSONSchema7Definition
        ])
    }
    return { $schema: schema.$schema, definitions: Object.fromEntries(publicDefinitions) }
}

function rewriteReferences(value: unknown, rename: (reference: string) => string): unknown {
    if (Array.isArray(value)) return value.map(item => rewriteReferences(item, rename))
    if (!value || typeof value !== "object") return value
    return Object.fromEntries(
        Object.entries(value).map(([key, child]) => [
            key,
            key === "$ref" && typeof child === "string" ? rename(child) : rewriteReferences(child, rename)
        ])
    )
}

export { extractPublicSchema }
