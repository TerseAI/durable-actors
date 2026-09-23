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
    const seen = new Set<JSONSchema7>()
    for (const [name, type] of Object.entries(types))
        preserveTypeNames(checker, type, definitions[name], definitions, seen)
    return { $schema: "http://json-schema.org/draft-07/schema#", definitions }
}

function preserveTypeNames(
    checker: ts.TypeChecker,
    type: ts.Type,
    schema: JSONSchema7Definition | undefined,
    definitions: Record<string, JSONSchema7Definition>,
    seen: Set<JSONSchema7>
): void {
    if (!schema || typeof schema === "boolean" || seen.has(schema)) return
    seen.add(schema)
    if (schema.$ref) {
        const key = decodeURIComponent(schema.$ref.slice("#/definitions/".length))
            .replaceAll("~1", "/")
            .replaceAll("~0", "~")
        preserveTypeNames(checker, type, definitions[key], definitions, seen)
        return
    }
    const name = sourceTypeName(type)
    if (type.isUnion()) {
        const members = type.types.filter(member => !(member.flags & ts.TypeFlags.Undefined))
        if (members.length !== 1) {
            if (name) schema.title ??= name
            return
        }
        type = members[0]
    }
    const title = name ?? sourceTypeName(type)
    if (title) schema.title ??= title
    const visit = (child: ts.Type, definition: JSONSchema7Definition | undefined) =>
        preserveTypeNames(checker, child, definition, definitions, seen)
    if (checker.isArrayType(type) || checker.isTupleType(type) || type.getSymbol()?.name === "ReadonlyArray") {
        const elements = checker.getTypeArguments(type as ts.TypeReference)
        if (Array.isArray(schema.items))
            schema.items.forEach((item, index) => {
                if (elements[index]) visit(elements[index], item)
            })
        else if (elements[0]) visit(elements[0], schema.items)
        return
    }
    for (const property of type.getProperties()) {
        const declaration = property.valueDeclaration ?? property.declarations?.[0]
        if (declaration)
            visit(checker.getTypeOfSymbolAtLocation(property, declaration), schema.properties?.[property.name])
    }
    const indexType = checker.getIndexTypeOfType(type, ts.IndexKind.String)
    if (indexType) visit(indexType, schema.additionalProperties)
}

function sourceTypeName(type: ts.Type): string | undefined {
    const symbol = type.aliasSymbol ?? type.getSymbol()
    if (symbol?.declarations?.some(declaration => declaration.getSourceFile().hasNoDefaultLib)) return undefined
    return symbol?.declarations?.some(
        declaration =>
            ts.isInterfaceDeclaration(declaration) ||
            ts.isTypeAliasDeclaration(declaration) ||
            ts.isEnumDeclaration(declaration)
    )
        ? symbol.name
        : undefined
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
            ts.TypeFlags.Unknown |
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
    for (const index of checker.getIndexInfosOfType(type)) assertJsonType(checker, index.type, label, true, seen)
}

export { assertJsonType, jsonSchema, sourceTypeName }
