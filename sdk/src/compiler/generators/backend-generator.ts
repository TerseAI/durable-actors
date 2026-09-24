import ts from "typescript"

import type { ActorApi, RpcMethod } from "../../wire/public-contract.js"

import { usageComment } from "./usage-comment.js"

function backendSource(actors: readonly ActorApi[]): string {
    const sources = actors.map(actorSource)
    const exampleActor = actors[0]?.actorName
    return `${usageComment("Types for actor state and methods.", exampleActor && `type State = actors.${exampleActor}.State`)}
export declare namespace actors {
${sources.map(source => source.declarations).join("\n")}
}

${usageComment("Call actor methods from your backend.", exampleActor && `const actor = actors.${exampleActor}.get("actor-id")`)}
export const actors = {
${sources.map(source => source.descriptor).join(",\n")}
}
`
}

function actorSource(actor: ActorApi) {
    const stubName = "Stub"
    const methodsName = "$MethodTypes"
    const descriptors = actor.rpc.methods.map(method => ({ name: method.name, result: method.result.kind }))
    const stub = `actors.${actor.actorName}.${stubName}`
    return {
        declarations: `export namespace ${actor.actorName} {
export type ${stubName} = $ActorTypes[${JSON.stringify(actor.actorName)}]["Methods"]
${methodTypes(actor.actorName, actor.rpc.methods, stubName, methodsName)}
}`,
        descriptor: actorDescriptor(
            actor.actorName,
            `get(actorId: string, transport?: import("./runtime/index.js").ActorRpcTransport): ${stub} {
        return $createActorStub<${stub}>(${JSON.stringify(actor.actorName)}, actorId, ${JSON.stringify(descriptors)}, transport)
    }`
        )
    }
}

function actorDescriptor(actorName: string, rpc: string): string {
    return `[${JSON.stringify(actorName)}]: {
    ${rpc},
    ${usageComment("Allow a frontend connection after your backend checks the user's access.", `const grant = await actors.${actorName}.prepareWebsocket({ actorId: "actor-id", metadata })`)}
    prepareWebsocket(
        authorization: Omit<actors.${actorName}.Authorization, "actorName">,
        options: import("./runtime/index.js").SocketProxyOptions = {},
        dependencies: import("./runtime/index.js").SocketProxyDependencies = {}
    ): Promise<import("./runtime/index.js").SocketGrant> {
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

function isIdentifier(name: string): boolean {
    const scanner = ts.createScanner(ts.ScriptTarget.Latest, false, ts.LanguageVariant.Standard, name)
    return scanner.scan() === ts.SyntaxKind.Identifier && scanner.scan() === ts.SyntaxKind.EndOfFileToken
}

export { backendSource }
