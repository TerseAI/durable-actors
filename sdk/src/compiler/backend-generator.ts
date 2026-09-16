import { compile } from "json-schema-to-typescript"
import type { JSONSchema } from "json-schema-to-typescript"
import ts from "typescript"

import type { ActorApi, RpcContract, RpcMethod, TypeReference } from "../wire/public-contract.js"

async function backendFiles(actors: readonly ActorApi[]): Promise<ReadonlyMap<string, string>> {
    const files = new Map<string, string>()
    for (const actor of actors) files.set(`${actor.actorType}.backend.ts`, await backendSource(actor))
    files.set(
        "backend.ts",
        actors.length === 0
            ? "export {}\n"
            : actors.map(actor => `export { ${actor.actorType} } from "./${actor.actorType}.backend.js"\n`).join("")
    )
    return files
}

async function backendSource(actor: ActorApi): Promise<string> {
    const { declarations, types, stubName } = await rpcDeclarations(actor.rpc)
    const methods = actor.rpc.methods.map((method, index) => methodDeclaration(method, index, types)).join("\n")
    const descriptors = actor.rpc.methods.map(method => ({ name: method.name, result: method.result.kind }))
    return `import { createActorStub as $createActorStub } from "little-actors/backend"

${declarations}
export interface ${stubName} {
${methods}
}

export const ${actor.actorType} = {
    get(actorId: string, transport?: import("little-actors/backend").ActorRpcTransport): ${stubName} {
        return $createActorStub<${stubName}>(${JSON.stringify(actor.actorType)}, actorId, ${JSON.stringify(descriptors)}, transport)
    }
}
`
}

async function rpcDeclarations(rpc: RpcContract) {
    const { properties, definitions } = rpcSchemaTypes(rpc)
    const code = await compile(
        {
            ...rpc.schema,
            definitions,
            type: "object",
            properties,
            required: Object.keys(properties),
            additionalProperties: false
        } as JSONSchema,
        "RpcTypes",
        {
            $refOptions: { resolve: { file: false, http: false } },
            bannerComment: "",
            unknownAny: true,
            additionalProperties: false
        }
    )
    return readRpcDeclarations(code)
}

function rpcSchemaTypes(rpc: RpcContract) {
    const properties: Record<string, TypeReference | boolean> = Object.create(null)
    const definitions = { ...rpc.schema.definitions }
    const add = (name: string, type: TypeReference, displayName: string) => {
        const key = type.$ref.slice("#/definitions/".length)
        const definition = definitions[key]
        properties[name] = typeof definition === "boolean" ? definition : type
        if (definition && typeof definition === "object" && !definition.title && !definition.$ref)
            definitions[key] = { ...definition, title: displayName }
    }
    rpc.methods.forEach((method, methodIndex) => {
        method.parameters.forEach((parameter, index) =>
            add(`m${methodIndex}p${index}`, parameter.type, `${method.name} ${parameter.name}`)
        )
        if (method.result.kind === "value") add(`m${methodIndex}result`, method.result.type, `${method.name} result`)
    })
    return { properties, definitions }
}

function readRpcDeclarations(code: string) {
    const source = ts.createSourceFile("types.ts", code, ts.ScriptTarget.Latest, true)
    const root = source.statements.find(
        statement => ts.isInterfaceDeclaration(statement) && statement.name.text === "RpcTypes"
    ) as ts.InterfaceDeclaration
    const types = new Map(
        root.members
            .filter(ts.isPropertySignature)
            .map(property => [(property.name as ts.Identifier).text, property.type!.getText(source)])
    )
    const statements = source.statements.filter(statement => statement !== root)
    const declarations = statements.map(statement => statement.getText(source)).join("\n\n")
    return { declarations, types, stubName: stubTypeName(statements) }
}

function stubTypeName(statements: readonly ts.Statement[]): string {
    const declarationNames = new Set(
        statements.flatMap(statement =>
            ts.isInterfaceDeclaration(statement) ||
            ts.isTypeAliasDeclaration(statement) ||
            ts.isEnumDeclaration(statement)
                ? [statement.name.text]
                : []
        )
    )
    let stubName = "Stub"
    while (declarationNames.has(stubName)) stubName += "_"
    return stubName
}

function methodDeclaration(method: RpcMethod, index: number, types: ReadonlyMap<string, string>): string {
    const used = new Set<string>()
    const parameters = method.parameters
        .map((parameter, parameterIndex) => {
            const name = parameterName(parameter.name, parameterIndex, used)
            const prefix = parameter.rest ? "..." : ""
            const optional = parameter.optional ? "?" : ""
            return `${prefix}${name}${optional}: ${types.get(`m${index}p${parameterIndex}`)!}`
        })
        .join(", ")
    const result = method.result.kind === "void" ? "void" : types.get(`m${index}result`)!
    return `    ${JSON.stringify(method.name)}(${parameters}): Promise<${result}>`
}

function parameterName(proposed: string, index: number, used: Set<string>): string {
    const scanner = ts.createScanner(ts.ScriptTarget.Latest, false, ts.LanguageVariant.Standard, proposed)
    const identifier = scanner.scan() === ts.SyntaxKind.Identifier && scanner.scan() === ts.SyntaxKind.EndOfFileToken
    let name = identifier ? proposed : `arg${index}`
    while (used.has(name)) name += "_"
    used.add(name)
    return name
}

export { backendFiles }
