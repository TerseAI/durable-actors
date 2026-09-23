import { Command } from "commander"
import { rm } from "node:fs/promises"
import path from "node:path"
import { z } from "zod"

import { connectionHelp } from "./connection.js"
import { createControlPlaneClient } from "./control-plane.js"

interface GenerateOptions {
    outDir: string
    config?: string
    remote?: boolean
}

function registerGenerateCommand(program: Command): void {
    program
        .command("generate")
        .argument("[entrypoint]", "actor source file (default: src/actors.ts)")
        .description("Generate clients to reach durable actors in your own project")
        .option("--out-dir <directory>", "generated source directory", "generated")
        .option("--config <file>", "TypeScript configuration file (local source only)")
        .option("--remote", "generate from the configured server")
        .addHelpText("after", connectionHelp)
        .action(generate)
}

async function generate(entrypoint: string | undefined, options: GenerateOptions): Promise<void> {
    validateOptions(entrypoint, options)
    const { generateClient } = await import("../compiler/generators/client-generator.js")
    const contract = options.remote ? await remoteContract() : await localContract(entrypoint, options.config)
    const directory = path.resolve(options.outDir)
    await generateClient(contract, directory)
    for (const obsolete of ["contract.json", "contract-source.json"])
        await rm(path.join(directory, obsolete), { force: true })
    console.log(`Generated ${contract.actors.length} actor contract(s) in ${directory}.`)
}

function validateOptions(entrypoint: string | undefined, options: GenerateOptions): void {
    if (options.remote && (entrypoint || options.config))
        throw new Error("--remote cannot be combined with a source entrypoint or --config.")
}

async function localContract(entrypoint: string | undefined, configFile?: string) {
    const { ActorCompiler } = await import("../compiler/actor-compiler.js")
    return new ActorCompiler().compileContract(entrypoint ?? "src/actors.ts", { configFile })
}

async function remoteContract() {
    const { parsePublicContract } = await import("../compiler/validate-public-contract.js")
    const client = createControlPlaneClient(process.env, fetch)
    const publication = publicationSchema.parse(await client.getContract())
    return parsePublicContract(publication.contract)
}

const publicationSchema = z.strictObject({
    contractHash: z.string().regex(/^sha256:[a-f0-9]{64}$/u),
    contract: z.unknown()
})

export { registerGenerateCommand }
