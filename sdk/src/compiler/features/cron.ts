import ts from "typescript"

import type { CronSchedule } from "../../actor/cron.js"
import { cronOptionsSchema } from "../../actor/cron.js"
import { definitionDiagnostic } from "../actor-compiler.js"
import { AnnotationKind } from "../types.js"
import type { DecoratorResult, DecoratorUse, ParsedActor } from "../types.js"

function readCron(use: DecoratorUse): DecoratorResult {
    const node = use.target
    const call = use.node.expression
    const expression = ts.isCallExpression(call) && call.arguments.length <= 2 ? call.arguments[0] : undefined
    if (!expression || !ts.isStringLiteralLike(expression) || expression.text.trim().split(/\s+/u).length !== 5)
        return invalid(use, "@Cron requires a literal five-field cron expression")
    const retries = readRetries(ts.isCallExpression(call) ? call.arguments[1] : undefined)
    if (retries === undefined) return invalid(use, "@Cron options require a literal nonnegative integer retries count")
    if (!ts.isMethodDeclaration(node) || !node.body || node.asteriskToken || node.typeParameters?.length)
        return invalid(use, "@Cron requires a public instance async method")
    const modifiers = ts.getModifiers(node) ?? []
    if (
        !modifiers.some(modifier => modifier.kind === ts.SyntaxKind.AsyncKeyword) ||
        modifiers.some(modifier =>
            [ts.SyntaxKind.StaticKeyword, ts.SyntaxKind.PrivateKeyword, ts.SyntaxKind.ProtectedKeyword].includes(
                modifier.kind
            )
        )
    )
        return invalid(use, "@Cron requires a public instance async method")
    if (
        (!ts.isIdentifier(node.name) && !ts.isStringLiteral(node.name)) ||
        ["onConnect", "onMessage", "onDisconnect"].includes(node.name.text)
    )
        return invalid(use, "@Cron cannot decorate a lifecycle hook or computed method")
    if (
        node.parameters.length !== 1 ||
        node.parameters[0].dotDotDotToken ||
        node.parameters[0].questionToken ||
        node.parameters[0].initializer
    )
        return invalid(use, "@Cron requires one CronEvent parameter")
    return {
        annotations: [{ kind: AnnotationKind.Cron, node: use.node, expression: expression.text, retries }],
        diagnostics: []
    }
}

function readRetries(options: ts.Expression | undefined): number | undefined {
    if (options === undefined) return 0
    if (!ts.isObjectLiteralExpression(options)) return undefined
    if (!options.properties.length) return 0
    if (options.properties.length !== 1) return undefined
    const property = options.properties[0]
    if (
        !ts.isPropertyAssignment(property) ||
        (!ts.isIdentifier(property.name) && !ts.isStringLiteral(property.name)) ||
        property.name.text !== "retries" ||
        !ts.isNumericLiteral(property.initializer)
    )
        return undefined
    const parsed = cronOptionsSchema.safeParse({ retries: Number(property.initializer.text) })
    return parsed.success ? parsed.data.retries : undefined
}

function validateCrons(actor: ParsedActor) {
    const schedules: CronSchedule[] = []
    const diagnostics: ts.Diagnostic[] = []
    for (const member of actor.members) {
        const seen = new Set<string>()
        for (const annotation of member.annotations) {
            if (annotation.kind !== AnnotationKind.Cron) continue
            if (seen.has(annotation.expression))
                diagnostics.push(definitionDiagnostic(member.node, "duplicate cron schedule"))
            seen.add(annotation.expression)
            schedules.push({
                method: (member.node.name as ts.Identifier | ts.StringLiteral).text,
                expression: annotation.expression,
                ...(annotation.retries ? { retries: annotation.retries } : {})
            })
        }
    }
    schedules.sort((a, b) => a.method.localeCompare(b.method) || a.expression.localeCompare(b.expression))
    return { schedules, diagnostics }
}

function invalid(use: DecoratorUse, message: string): DecoratorResult {
    return { annotations: [], diagnostics: [definitionDiagnostic(use.target, message)] }
}

export { readCron, validateCrons }
