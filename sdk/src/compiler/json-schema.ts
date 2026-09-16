import type { JSONSchema7, JSONSchema7Definition } from "json-schema"
import ts from "typescript"
import { JsonSchemaGenerator, getDefaultArgs } from "typescript-json-schema"

import { ActorDefinitionError } from "../errors.js"

function jsonSchema(checker: ts.TypeChecker, types: Record<string, ts.Type>): JSONSchema7 {
    const generator = new JsonSchemaGenerator([], types, {}, {}, checker, {
        ...getDefaultArgs(),
        required: true,
        strictNullChecks: true,
        rejectDateType: true,
        ref: true,
        aliasRef: false,
        topRef: false,
        defaultProps: false
    })
    const names = Object.keys(types).filter(name => !(types[name].flags & ts.TypeFlags.Never))
    const generated = generator.getSchemaForSymbols(names) as JSONSchema7
    const definitions: Record<string, JSONSchema7Definition> = { ...generated.definitions }
    for (const name of Object.keys(types)) if (types[name].flags & ts.TypeFlags.Never) definitions[name] = false
    return { $schema: "http://json-schema.org/draft-07/schema#", definitions }
}

function assertJsonType(
    checker: ts.TypeChecker,
    type: ts.Type,
    label: string,
    optional = false,
    seen = new Set<ts.Type>()
): void {
    if (type.flags & ts.TypeFlags.Undefined) {
        if (optional) return
        throw new ActorDefinitionError(
            `${label} must be JSON-compatible; undefined is only allowed for optional properties`
        )
    }
    if (type.isUnionOrIntersection()) {
        for (const member of type.types) assertJsonType(checker, member, label, optional, seen)
        return
    }
    if (seen.has(type)) return
    seen.add(type)
    if (
        type.flags &
        (ts.TypeFlags.StringLike |
            ts.TypeFlags.NumberLike |
            ts.TypeFlags.BooleanLike |
            ts.TypeFlags.Null |
            ts.TypeFlags.Never)
    )
        return
    if (checker.isArrayType(type) || checker.isTupleType(type) || type.getSymbol()?.name === "ReadonlyArray") {
        for (const element of checker.getTypeArguments(type as ts.TypeReference))
            assertJsonType(checker, element, label, false, seen)
        return
    }
    if (
        !(type.flags & ts.TypeFlags.Object) ||
        type.getCallSignatures().length ||
        type.getConstructSignatures().length ||
        (type.getSymbol()?.flags ?? 0) & ts.SymbolFlags.Class
    )
        throw new ActorDefinitionError(
            `${label} must be JSON-compatible; unsupported type ${checker.typeToString(type)}`
        )
    for (const property of type.getProperties()) {
        const declaration = property.valueDeclaration ?? property.declarations?.[0]
        if (!declaration || property.name.startsWith("__@"))
            throw new ActorDefinitionError(`${label} must be JSON-compatible; symbol properties are not supported`)
        assertJsonType(
            checker,
            checker.getTypeOfSymbolAtLocation(property, declaration),
            `${label}.${property.name}`,
            !!(property.flags & ts.SymbolFlags.Optional),
            seen
        )
    }
    for (const index of checker.getIndexInfosOfType(type)) assertJsonType(checker, index.type, label, false, seen)
}

export { assertJsonType, jsonSchema }
