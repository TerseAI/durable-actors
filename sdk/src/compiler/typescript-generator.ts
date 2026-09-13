import { Ajv } from "ajv"
import standaloneCode from "ajv/dist/standalone/index.js"
import { build } from "esbuild"
import { compile } from "json-schema-to-typescript"
import type { JSONSchema } from "json-schema-to-typescript"
import { fileURLToPath } from "node:url"
import ts from "typescript"

import type { SocketContract } from "../wire/contract.js"

async function generateTypeScript(contracts: readonly SocketContract[]): Promise<ReadonlyMap<string, string>> {
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
    return artifacts
}

async function actorFiles(contract: SocketContract, declarations: string): Promise<[string, string][]> {
    const name = contract.actorType
    const kinds = ["Incoming", "Outgoing", "State"]
    const fields = contract.emittable.map(field => JSON.stringify(field)).join(" | ") || "never"
    const validators = validatorBinding(name, declarations)
    const source = `import * as ${validators} from "./${name}.validators.js"\n\n${declarations}\nexport type Connection = import("little-actors/browser").ActorConnection<Incoming, Outgoing, State, ${fields}>\n\nexport const ${name}: import("little-actors/browser").ActorDescriptor<Incoming, Outgoing, State, ${fields}> = {\n    actorType: ${JSON.stringify(name)},\n    emittable: ${JSON.stringify(contract.emittable)},\n    validators: ${validators}\n}\n`
    return [
        [`${name}.actor.ts`, source],
        [`${name}.validators.js`, await validatorsSource(contract, kinds)],
        [`${name}.validators.d.ts`, validatorDeclarations(kinds)]
    ]
}

async function proxyFiles(contract: SocketContract, declarations: string): Promise<[string, string][]> {
    const name = contract.actorType
    const kinds = ["Metadata"]
    const validators = validatorBinding(name, declarations)
    const source = `import * as ${validators} from "./${name}.proxy-validators.js"\n\n${declarations}\nexport interface Authorization {\n    actorType: ${JSON.stringify(name)}\n    actorId: string\n    metadata: Metadata\n    authorizationLifetimeMs?: number\n}\n\nexport const ${name}: import("little-actors/proxy").ProxyActor<Metadata> = { metadata: ${validators}.metadata }\n`
    return [
        [`${name}.proxy.ts`, source],
        [`${name}.proxy-validators.js`, await validatorsSource(contract, kinds)],
        [`${name}.proxy-validators.d.ts`, validatorDeclarations(kinds)]
    ]
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

function validatorBinding(name: string, declarations: string): string {
    let validators = `${name}Validators`
    while (declarations.includes(validators)) validators = `_${validators}`
    return validators
}

function validatorDeclarations(kinds: readonly string[]): string {
    return (
        kinds.map(kind => `export declare const ${kind.toLowerCase()}: (value: unknown) => boolean`).join("\n") + "\n"
    )
}

async function validatorsSource(contract: SocketContract, kinds: readonly string[]): Promise<string> {
    const ajv = new Ajv({ strict: false, validateFormats: false, code: { source: true, esm: true } })
    ajv.addSchema({ ...contract.schema, $id: "actor-contract" })
    const source = standaloneCode.default(
        ajv,
        Object.fromEntries(kinds.map(kind => [kind.toLowerCase(), `actor-contract#/definitions/${kind}`]))
    )
    const result = await build({
        stdin: { contents: source, resolveDir: fileURLToPath(new URL("../../", import.meta.url)), loader: "js" },
        bundle: true,
        platform: "browser",
        format: "esm",
        write: false,
        minify: true,
        logLevel: "silent"
    })
    return result.outputFiles[0]!.text
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
