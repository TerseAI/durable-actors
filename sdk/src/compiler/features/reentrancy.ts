import ts from "typescript"

import { definitionDiagnostic } from "../actor-compiler.js"
import { AnnotationKind } from "../types.js"
import type { DecoratorResult, DecoratorUse, ParsedActor } from "../types.js"

function readReentrancy(use: DecoratorUse): DecoratorResult {
    const node = use.target
    const modifiers = ts.canHaveModifiers(node) ? (ts.getModifiers(node) ?? []) : []
    const invalid =
        use.called ||
        !ts.isMethodDeclaration(node) ||
        !node.body ||
        !modifiers.some(modifier => modifier.kind === ts.SyntaxKind.AsyncKeyword) ||
        modifiers.some(modifier =>
            [ts.SyntaxKind.StaticKeyword, ts.SyntaxKind.PrivateKeyword, ts.SyntaxKind.ProtectedKeyword].includes(
                modifier.kind
            )
        ) ||
        !(ts.isIdentifier(node.name) || ts.isStringLiteral(node.name))
    return invalid
        ? {
              annotations: [],
              diagnostics: [
                  definitionDiagnostic(
                      node,
                      "@Reentrant requires a public instance async method; use it without parentheses"
                  )
              ]
          }
        : { annotations: [{ kind: AnnotationKind.Reentrancy, node: use.node }], diagnostics: [] }
}

function validateReentrancy(actor: ParsedActor) {
    const methods: string[] = []
    const diagnostics: ts.Diagnostic[] = []
    for (const member of actor.members) {
        const annotations = member.annotations.filter(annotation => annotation.kind === AnnotationKind.Reentrancy)
        if (annotations.length > 1) diagnostics.push(definitionDiagnostic(member.node, "@Reentrant cannot be repeated"))
        if (annotations.length === 1) methods.push((member.node.name as ts.Identifier | ts.StringLiteral).text)
    }
    return { methods, diagnostics }
}

export { readReentrancy, validateReentrancy }
