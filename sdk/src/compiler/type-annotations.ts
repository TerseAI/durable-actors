import type { JSONSchema7, JSONSchema7Definition } from "json-schema"
import ts from "typescript"
import { z } from "zod"

const templateLiteral = z
    .strictObject({
        texts: z.array(z.string()).min(2),
        types: z.array(z.enum(["string", "number", "bigint"]))
    })
    .refine(value => value.texts.length === value.types.length + 1, "invalid template literal spans")

const typeAnnotation = z.strictObject({
    undefined: z.literal(true).optional(),
    templateLiteral: templateLiteral.optional()
})

type TypeAnnotation = z.infer<typeof typeAnnotation>
type AnnotatedSchema = JSONSchema7 & { "x-typescript"?: TypeAnnotation }

function readTypeAnnotation(schema: JSONSchema7): TypeAnnotation | undefined {
    const annotation = (schema as AnnotatedSchema)["x-typescript"]
    return annotation === undefined ? undefined : typeAnnotation.parse(annotation)
}

function annotateType(schema: JSONSchema7, annotation: TypeAnnotation): void {
    const annotated = schema as AnnotatedSchema
    annotated["x-typescript"] = { ...annotated["x-typescript"], ...annotation }
}

function schemaForTypeScript(schema: JSONSchema7Definition): JSONSchema7Definition {
    if (typeof schema === "boolean") return schema
    const annotation = readTypeAnnotation(schema)
    const result: JSONSchema7 & { tsType?: string } = lowerChildren(schema)
    if (annotation?.templateLiteral) result.tsType = templateSource(annotation.templateLiteral)
    else if (isUnconstrained(schema)) result.tsType = "unknown"
    if (!annotation?.undefined) return result
    const { title, description, ...value } = result
    const undefinedType: JSONSchema7 & { tsType: string } = { tsType: "undefined" }
    return { ...(title && { title }), ...(description && { description }), anyOf: [value, undefinedType] }
}

function lowerChildren(schema: JSONSchema7): JSONSchema7 {
    const result: JSONSchema7 & { tsType?: string } = { ...schema }
    delete (result as AnnotatedSchema)["x-typescript"]
    for (const key of ["definitions", "properties", "patternProperties"] as const)
        if (result[key])
            result[key] = Object.fromEntries(
                Object.entries(result[key]).map(([name, child]) => [name, schemaForTypeScript(child)])
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
                [key]: Array.isArray(child) ? child.map(schemaForTypeScript) : schemaForTypeScript(child)
            })
    }
    return result
}

function isUnconstrained(schema: JSONSchema7): boolean {
    const nonValidationKeywords = [
        "$schema",
        "$comment",
        "title",
        "description",
        "default",
        "examples",
        "readOnly",
        "writeOnly",
        "definitions",
        "x-typescript"
    ]
    return Object.keys(schema).every(key => nonValidationKeywords.includes(key))
}

function templateSource(template: z.infer<typeof templateLiteral>): string {
    const kinds = {
        string: ts.SyntaxKind.StringKeyword,
        number: ts.SyntaxKind.NumberKeyword,
        bigint: ts.SyntaxKind.BigIntKeyword
    } as const
    const node = ts.factory.createTemplateLiteralType(
        ts.factory.createTemplateHead(template.texts[0]),
        template.types.map((type, index) =>
            ts.factory.createTemplateLiteralTypeSpan(
                ts.factory.createKeywordTypeNode(kinds[type]),
                index === template.types.length - 1
                    ? ts.factory.createTemplateTail(template.texts[index + 1])
                    : ts.factory.createTemplateMiddle(template.texts[index + 1])
            )
        )
    )
    return ts
        .createPrinter()
        .printNode(ts.EmitHint.Unspecified, node, ts.createSourceFile("type.ts", "", ts.ScriptTarget.Latest))
}

export { annotateType, readTypeAnnotation, schemaForTypeScript }
