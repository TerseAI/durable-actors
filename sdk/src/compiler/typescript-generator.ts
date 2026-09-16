import { compile } from "json-schema-to-typescript"
import type { JSONSchema } from "json-schema-to-typescript"
import ts from "typescript"

import type { SocketContract } from "../wire/contract.js"
import type { PublicActorContract } from "../wire/public-contract.js"

import { backendFiles } from "./backend-generator.js"
import { parsePublicContract } from "./validate-public-contract.js"

/** Returns generated filenames and TypeScript source without writing files or executing actor code. */
async function generateTypeScript(
    input: readonly SocketContract[] | PublicActorContract
): Promise<ReadonlyMap<string, string>> {
    const document = "actors" in input ? input : undefined
    if (document && document.version !== 1)
        throw new Error(`unsupported public actor contract version ${document.version}`)
    if (document) parsePublicContract(document)
    const contracts = "actors" in input ? input.actors.map(actor => actor.socket) : input
    const artifacts = new Map<string, string>()
    for (const contract of contracts) {
        if (!/^[A-Za-z_$][\w$]*$/u.test(contract.actorType))
            throw new Error(`actor name ${contract.actorType} cannot be emitted as a TypeScript identifier`)
        const declarations = await wireDeclarations(contract)
        for (const [file, contents] of await actorFiles(contract, declarations)) artifacts.set(file, contents)
        for (const [file, contents] of await proxyFiles(contract, declarations)) artifacts.set(file, contents)
    }
    artifacts.set("index.ts", clientIndex(contracts))
    artifacts.set("proxy.ts", proxyIndex(contracts))
    if (document) {
        for (const actor of document.actors)
            if (actor.actorType !== actor.socket.actorType)
                throw new Error("actor and socket contract names must match")
        for (const [file, contents] of await backendFiles(document.actors)) artifacts.set(file, contents)
    }
    return artifacts
}

async function actorFiles(contract: SocketContract, declarations: string): Promise<[string, string][]> {
    const name = contract.actorType
    const fields = contract.emittable.map(field => JSON.stringify(field)).join(" | ") || "never"
    const source = `${declarations}\nexport type Connection = import("little-actors/browser").ActorConnection<Incoming, Outgoing, State, ${fields}>\n\nexport const ${name}: import("little-actors/browser").ActorDescriptor<Incoming, Outgoing, State, ${fields}> = {\n    actorType: ${JSON.stringify(name)},\n    emittable: ${JSON.stringify(contract.emittable)}\n}\n`
    return [[`${name}.actor.ts`, source]]
}

async function proxyFiles(contract: SocketContract, declarations: string): Promise<[string, string][]> {
    const name = contract.actorType
    const source = `${declarations}\nexport interface Authorization {\n    actorType: ${JSON.stringify(name)}\n    actorId: string\n    metadata: Metadata\n    authorizationLifetimeMs?: number\n}\n\nexport const ${name}: import("little-actors/proxy").ProxyActor<Metadata> = {}\n`
    return [[`${name}.proxy.ts`, source]]
}

async function wireDeclarations(contract: SocketContract): Promise<string> {
    const kinds = ["Metadata", "Incoming", "Outgoing", "State"]
    const properties = Object.fromEntries(
        kinds.map(kind => [
            kind.toLowerCase(),
            typeof contract.schema.definitions?.[kind] === "boolean"
                ? contract.schema.definitions[kind]
                : { $ref: `#/definitions/${kind}` }
        ])
    )
    const code = await compile(
        inlinePrimitiveReferences(
            {
                ...contract.schema,
                type: "object",
                additionalProperties: false,
                properties,
                required: Object.keys(properties)
            },
            contract.schema.definitions ?? {}
        ) as JSONSchema,
        "ActorTypes",
        {
            $refOptions: { resolve: { file: false, http: false } },
            bannerComment: "",
            unknownAny: true,
            additionalProperties: false,
            customName: (schema, key) => {
                const name = schema.title || schema.$id || key
                return name === "Connection" || name === "Authorization" ? `${name}Data` : undefined
            }
        }
    )
    const source = ts.createSourceFile("types.ts", code, ts.ScriptTarget.Latest, true)
    const root = source.statements.find(
        statement => ts.isInterfaceDeclaration(statement) && statement.name.text === "ActorTypes"
    ) as ts.InterfaceDeclaration
    const declarations = source.statements.filter(statement => statement !== root)
    const named = new Map(
        declarations
            .filter(
                (statement): statement is ts.InterfaceDeclaration | ts.TypeAliasDeclaration =>
                    ts.isInterfaceDeclaration(statement) || ts.isTypeAliasDeclaration(statement)
            )
            .map(statement => [statement.name.text, statement])
    )
    const main = kinds.map(kind => {
        const declaration = named.get(kind)
        if (declaration) return declaration.getText(source)
        const property = root.members.find(
            member => ts.isPropertySignature(member) && member.name.getText(source) === kind.toLowerCase()
        ) as ts.PropertySignature
        return `export type ${kind} = ${property.type!.getText(source)}`
    })
    const helpers = declarations.filter(statement => !kinds.some(kind => named.get(kind) === statement))
    return [...main, ...helpers.map(statement => statement.getText(source))].join("\n\n") + "\n"
}

function inlinePrimitiveReferences(
    value: unknown,
    definitions: NonNullable<SocketContract["schema"]["definitions"]>
): unknown {
    if (Array.isArray(value)) return value.map(item => inlinePrimitiveReferences(item, definitions))
    if (!value || typeof value !== "object") return value
    const node = value as Record<string, unknown>
    const definition =
        typeof node.$ref === "string" && node.$ref.startsWith("#/definitions/")
            ? definitions[node.$ref.slice("#/definitions/".length)]
            : undefined
    if (
        definition &&
        typeof definition === "object" &&
        typeof definition.type === "string" &&
        ["string", "number", "integer", "boolean", "null"].includes(definition.type)
    ) {
        const { $ref, ...rest } = node
        return { ...definition, ...rest }
    }
    return Object.fromEntries(
        Object.entries(node).map(([key, child]) => [key, inlinePrimitiveReferences(child, definitions)])
    )
}

function clientIndex(contracts: readonly SocketContract[]): string {
    const { imports, actors } = actorImports(contracts, "actor")
    return `import { createClient } from "little-actors/browser"\nimport type { ClientOptions } from "little-actors/browser"\n${imports}\n\nexport interface Client {\n${contracts.map(({ actorType }) => `    ${JSON.stringify(actorType)}: { get(actorId: string): import("./${actorType}.actor.js").Connection }`).join("\n")}\n}\n\nexport function ActorClient(options: ClientOptions = {}): Client {\n    return createClient({ ${actors} }, options)\n}\n`
}

function proxyIndex(contracts: readonly SocketContract[]): string {
    const { imports, actors } = actorImports(contracts, "proxy")
    return `import { SocketProxy } from "little-actors/proxy"
import type { SocketGrant, SocketProxyDependencies, SocketProxyOptions } from "little-actors/proxy"
${imports}

const actors = { ${actors} }
export type ActorAuthorization = ${contracts.map(({ actorType }) => `import("./${actorType}.proxy.js").Authorization`).join(" | ") || "never"}

export class ActorProxy extends SocketProxy<typeof actors> {
    constructor(options: SocketProxyOptions = {}, dependencies: SocketProxyDependencies = {}) {
        super(actors, options, dependencies)
    }

    handle(authorization: ActorAuthorization): Promise<SocketGrant> {
        return super.handle(authorization)
    }

    static handle(authorization: ActorAuthorization, options: SocketProxyOptions = {}): Promise<SocketGrant> {
        return new ActorProxy(options).handle(authorization)
    }
}
`
}

function actorImports(contracts: readonly SocketContract[], suffix: string) {
    const imports = contracts
        .map(({ actorType }, index) => `import { ${actorType} as actor${index} } from "./${actorType}.${suffix}.js"`)
        .join("\n")
    const actors = contracts.map(({ actorType }, index) => `[${JSON.stringify(actorType)}]: actor${index}`).join(", ")
    return { imports, actors }
}

export { generateTypeScript }
export type { SocketContract }
export type { PublicActorContract }
