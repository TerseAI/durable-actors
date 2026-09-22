import { Command, Option } from "commander"
import { rm } from "node:fs/promises"
import path from "node:path"
import { z } from "zod"

import { createControlPlaneClient } from "./control-plane.js"

interface GenerateOptions {
    projectId: string
    outDir: string
    config?: string
    url?: string | true
    apiKey?: string
}

function registerGenerateCommand(program: Command): void {
    program
        .command("generate")
        .argument("[entrypoint]", "actor source file (default: src/durable-objects.ts)")
        .description("Generate TypeScript clients")
        .option("--out-dir <directory>", "generated source directory", "generated")
        .option("--config <file>", "TypeScript configuration file (local source only)")
        .option("--url [origin]", "generate from a server (defaults to the configured or local URL)")
        .addOption(new Option("--project-id <id>", "actor project ID").env("DURABLE_OBJECT_PROJECT_ID"))
        .option("--api-key <key>", "admin API key (or DURABLE_OBJECT_API_KEY)")
        .action(generate)
}

async function generate(entrypoint: string | undefined, options: GenerateOptions): Promise<void> {
    validateOptions(entrypoint, options)
    const { generateClient } = await import("../compiler/generators/client-generator.js")
    const contract = options.url ? await remoteContract(options) : await localContract(entrypoint, options.config)
    const directory = path.resolve(options.outDir)
    await generateClient(contract, directory)
    for (const obsolete of ["contract.json", "contract-source.json"])
        await rm(path.join(directory, obsolete), { force: true })
    console.log(`Generated ${contract.actors.length} actor contract(s) in ${directory}.`)
}

function validateOptions(entrypoint: string | undefined, options: GenerateOptions): void {
    if (options.url && (entrypoint || options.config))
        throw new Error("--url cannot be combined with a source entrypoint or --config.")
    if (!options.url && options.apiKey) throw new Error("--api-key requires --url.")
}

async function localContract(entrypoint: string | undefined, configFile?: string) {
    const { ActorCompiler } = await import("../compiler/actor-compiler.js")
    return new ActorCompiler().compileContract(entrypoint ?? "src/durable-objects.ts", { configFile })
}

async function remoteContract(options: GenerateOptions) {
    const { parsePublicContract } = await import("../compiler/validate-public-contract.js")
    const client = createControlPlaneClient(options, fetch)
    const publication = publicationSchema.parse(await client.getContract())
    return parsePublicContract(publication.contract)
}

const publicationSchema = z.strictObject({
    contractHash: z.string().regex(/^sha256:[a-f0-9]{64}$/u),
    contract: z.unknown()
})

export { registerGenerateCommand }
