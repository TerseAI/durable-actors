import { Command } from "commander"
import { rm } from "node:fs/promises"
import path from "node:path"
import { z } from "zod"

import type { PublicActorContract } from "../wire/public-contract.js"

import { ControlPlaneClient } from "./control-plane.js"

interface GenerateOptions {
    outDir: string
    config?: string
    url?: string | true
    apiKey?: string
    namespace?: string
    revision?: string
}

function registerGenerateCommand(program: Command): void {
    program
        .command("generate [entrypoint]")
        .description("Generate browser clients, authorization proxies, and backend RPC stubs")
        .option("--out-dir <directory>", "generated source directory", "generated")
        .option("--config <file>", "TypeScript configuration file (local source only)")
        .option("--url [origin]", "fetch a published contract (defaults to the configured or local runtime URL)")
        .option("--api-key <key>", "admin API key (or DURABLE_OBJECT_API_KEY)")
        .option("--namespace <id>", "contract namespace (or DURABLE_OBJECT_NAMESPACE_ID)")
        .option("--revision <revision>", "require this active code revision (defaults to the latest deployment)")
        .action(generate)
}

async function generate(entrypoint: string | undefined, options: GenerateOptions): Promise<void> {
    validateOptions(entrypoint, options)
    const { generateClient } = await import("../compiler/client-generator.js")
    const { contract, codeRevision } = options.url
        ? await remoteContract(options)
        : await localContract(entrypoint, options.config)
    const directory = path.resolve(options.outDir)
    await generateClient(contract, directory)
    for (const obsolete of ["contract.json", "contract-source.json"])
        await rm(path.join(directory, obsolete), { force: true })
    console.log(
        `Generated ${contract.actors.length} actor contract(s) in ${directory}${codeRevision ? ` from revision ${codeRevision}` : ""}.`
    )
}

function validateOptions(entrypoint: string | undefined, options: GenerateOptions): void {
    if (options.url && (entrypoint || options.config))
        throw new Error("--url cannot be combined with a source entrypoint or --config.")
    if (!options.url && (options.apiKey || options.namespace || options.revision))
        throw new Error("--api-key, --namespace, and --revision require --url.")
    if (options.revision && !/^[A-Za-z0-9._-]{1,128}$/u.test(options.revision))
        throw new Error("Invalid code revision; use 1–128 letters, digits, dots, underscores, or hyphens.")
}

async function localContract(entrypoint: string | undefined, configFile?: string) {
    const { ActorCompiler } = await import("../compiler/actor-compiler.js")
    return {
        contract: new ActorCompiler().compileContract(entrypoint ?? "src/durable-objects.ts", { configFile }),
        codeRevision: undefined
    }
}

async function remoteContract(options: GenerateOptions) {
    const client = new ControlPlaneClient(options, fetch)
    const query = options.revision ? `?${new URLSearchParams({ revision: options.revision })}` : ""
    const publication = publicationSchema.parse(await client.json("GET", `contract${query}`))
    if (client.connection.namespaceId && publication.namespaceId !== client.connection.namespaceId)
        throw new Error("Contract response namespace does not match the requested namespace.")
    if (options.revision && publication.codeRevision !== options.revision)
        throw new Error("Contract response revision does not match the requested revision.")
    return { contract: publication.contract as PublicActorContract, codeRevision: publication.codeRevision }
}

const publicationSchema = z.strictObject({
    namespaceId: z.string().regex(/^[A-Za-z0-9._-]{1,255}$/u),
    codeRevision: z.string().regex(/^[A-Za-z0-9._-]{1,255}$/u),
    contractHash: z.string().regex(/^sha256:[a-f0-9]{64}$/u),
    contract: z.object({ version: z.number(), actors: z.array(z.unknown()) }).passthrough()
})

export { registerGenerateCommand }
