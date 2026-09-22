import { compile } from "json-schema-to-typescript"
import type { JSONSchema } from "json-schema-to-typescript"
import ts from "typescript"

import type { SocketContract } from "../../wire/contract.js"
import type { ActorApi, RpcContract, RpcMethod, TypeReference } from "../../wire/public-contract.js"

import { usageComment } from "./usage-comment.js"

async function backendSource(
    actors: readonly ActorApi[],
    contracts: readonly SocketContract[],
    wireNames: ReadonlyMap<string, readonly string[]>
): Promise<string> {
    const sources = []
    for (const { actorName } of contracts) {
        const actor = actors.find(actor => actor.actorName === actorName)
        sources.push(
            actor
                ? await actorSource(actor, wireNames.get(actorName) ?? [])
                : { declarations: "", descriptor: actorDescriptor(actorName) }
        )
    }
    const exampleType = contracts[0]?.actorName
    const exampleActor = actors[0]?.actorName
    return `${usageComment("Types for actor state and methods.", exampleType && `type State = actors.${exampleType}.State`)}
export declare namespace actors {
${sources.map(source => source.declarations).join("\n")}
}

${usageComment("Call actor methods from your backend.", exampleActor && `const actor = actors.${exampleActor}.get("actor-id")`)}
export const actors = {
${sources.map(source => source.descriptor).join(",\n")}
}
`
}

async function actorSource(actor: ActorApi, wireNames: readonly string[]) {
    const { declarations, types, stubName, methodsName } = await rpcDeclarations(actor.rpc, wireNames)
    const methods = actor.rpc.methods.map((method, index) => methodDeclaration(method, index, types)).join("\n")
    const descriptors = actor.rpc.methods.map(method => ({ name: method.name, result: method.result.kind }))
    const stub = `actors.${actor.actorName}.${stubName}`
    return {
        declarations: `export namespace ${actor.actorName} {
${declarations}
export interface ${stubName} {
${methods}
}
${methodTypes(actor.actorName, actor.rpc.methods, stubName, methodsName)}
}`,
        descriptor: actorDescriptor(
            actor.actorName,
            `get(actorId: string, transport?: import("durable-actors/backend").ActorRpcTransport): ${stub} {
        return $createActorStub<${stub}>(${JSON.stringify(actor.actorName)}, actorId, ${JSON.stringify(descriptors)}, transport)
    }`
        )
    }
}

function actorDescriptor(actorName: string, rpc?: string): string {
    return `[${JSON.stringify(actorName)}]: {
    ${rpc ? `${rpc},` : ""}
    ${usageComment("Allow a frontend connection after your backend checks the user's access.", `const grant = await actors.${actorName}.prepareWebsocket({ actorId: "actor-id", metadata })`)}
    prepareWebsocket(
        authorization: Omit<actors.${actorName}.Authorization, "actorName">,
        options: import("durable-actors/proxy").SocketProxyOptions = {},
        dependencies: import("durable-actors/proxy").SocketProxyDependencies = {}
    ): Promise<import("durable-actors/proxy").SocketGrant> {
        return new ActorProxy(options, dependencies).handle({ ...authorization, actorName: ${JSON.stringify(actorName)} })
    }
}`
}

function methodTypes(actorName: string, methods: readonly RpcMethod[], stubName: string, methodsName: string): string {
    const entries = methods.map(
        ({ name }) => `[${JSON.stringify(name)}]: {
    Args: Parameters<${stubName}[${JSON.stringify(name)}]>
    Result: Awaited<ReturnType<${stubName}[${JSON.stringify(name)}]>>
}`
    )
    const namespaces = methods
        .filter(method => isIdentifier(method.name))
        .map(
            ({ name }) => `export namespace ${name} {
    export type Args = ${methodsName}[${JSON.stringify(name)}]["Args"]
    export type Result = ${methodsName}[${JSON.stringify(name)}]["Result"]
}`
        )
    return `interface ${methodsName} {
${entries.join("\n")}
}
export interface Methods extends ${methodsName} {}
${usageComment("Types for a method's arguments and return value.", methods[0] && `type Args = actors.${actorName}.Methods[${JSON.stringify(methods[0].name)}]["Args"]\ntype Result = actors.${actorName}.Methods[${JSON.stringify(methods[0].name)}]["Result"]`)}
export namespace Methods {
${namespaces.join("\n")}
}`
}

async function rpcDeclarations(rpc: RpcContract, wireNames: readonly string[]) {
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
            additionalProperties: false,
            customName: (schema, key) => {
                const name = schema.title || schema.$id || key
                const reserved = [
                    ...wireNames,
                    "State",
                    "Metadata",
                    "Incoming",
                    "Outgoing",
                    "Connection",
                    "Authorization",
                    "Methods",
                    "Parameters",
                    "ReturnType",
                    "Awaited"
                ]
                let available = name
                while (reserved.includes(available)) available += "Data"
                return available === name ? undefined : available
            }
        }
    )
    return readRpcDeclarations(code, wireNames)
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

function readRpcDeclarations(code: string, wireNames: readonly string[]) {
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
    return {
        declarations,
        types,
        stubName: availableTypeName(statements, "Stub", wireNames),
        methodsName: availableTypeName(statements, "$MethodTypes", wireNames)
    }
}

function availableTypeName(statements: readonly ts.Statement[], proposed: string, reserved: readonly string[]): string {
    const declarationNames = new Set(
        statements.flatMap(statement =>
            ts.isInterfaceDeclaration(statement) ||
            ts.isTypeAliasDeclaration(statement) ||
            ts.isEnumDeclaration(statement)
                ? [statement.name.text]
                : []
        )
    )
    let name = proposed
    while (declarationNames.has(name) || reserved.includes(name)) name += "_"
    return name
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
    let name = isIdentifier(proposed) ? proposed : `arg${index}`
    while (used.has(name)) name += "_"
    used.add(name)
    return name
}

function isIdentifier(name: string): boolean {
    const scanner = ts.createScanner(ts.ScriptTarget.Latest, false, ts.LanguageVariant.Standard, name)
    return scanner.scan() === ts.SyntaxKind.Identifier && scanner.scan() === ts.SyntaxKind.EndOfFileToken
}

export { backendSource }
