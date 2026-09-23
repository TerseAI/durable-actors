import { Command } from "commander"
import { rm } from "node:fs/promises"
import path from "node:path"
import { z } from "zod"

import { connectionHelp } from "./connection.js"
import { createControlPlaneClient } from "./control-plane.js"

interface GenerateOptions {
    outDir: string
    config?: string
    controlPlaneUrl?: string
}

function registerGenerateCommand(program: Command): void {
    program
        .command("generate")
        .argument("[entrypoint]", "actor source file to compile instead of fetching from the server")
        .description("Generate actor clients from the running server or an explicit source file")
        .option("--out-dir <directory>", "generated source directory", "generated")
        .option("--config <file>", "TypeScript configuration file (local source only)")
        .option("--control-plane-url <url>", "control-plane origin (overrides DURABLE_ACTORS_CONTROL_PLANE_URL)")
        .addHelpText("after", connectionHelp)
        .action(generate)
}

async function generate(entrypoint: string | undefined, options: GenerateOptions): Promise<void> {
    validateOptions(entrypoint, options)
    const { generateClient } = await import("../compiler/generators/client-generator.js")
    const contract = entrypoint
        ? await localContract(entrypoint, options.config)
        : await remoteContract(options.controlPlaneUrl)
    const directory = path.resolve(options.outDir)
    await generateClient(contract, directory)
    for (const obsolete of ["contract.json", "contract-source.json"])
        await rm(path.join(directory, obsolete), { force: true })
    console.log(`Generated ${contract.actors.length} actor contract(s) in ${directory}.`)
}

function validateOptions(entrypoint: string | undefined, options: GenerateOptions): void {
    if (entrypoint && options.controlPlaneUrl !== undefined)
        throw new Error("--control-plane-url cannot be combined with a source entrypoint.")
    if (options.config && !entrypoint) throw new Error("--config requires a source entrypoint.")
}

async function localContract(entrypoint: string, configFile?: string) {
    const { ActorCompiler } = await import("../compiler/actor-compiler.js")
    return new ActorCompiler().compileContract(entrypoint, { configFile })
}

async function remoteContract(controlPlaneUrl?: string) {
    const { parsePublicContract } = await import("../compiler/validate-public-contract.js")
    const client = createControlPlaneClient(
        {
            ...process.env,
            DURABLE_ACTORS_CONTROL_PLANE_URL: controlPlaneUrl ?? process.env.DURABLE_ACTORS_CONTROL_PLANE_URL
        },
        fetch
    )
    const publication = publicationSchema.parse(await client.getContract())
    return parsePublicContract(publication.contract)
}

const publicationSchema = z.strictObject({
    contractHash: z.string().regex(/^sha256:[a-f0-9]{64}$/u),
    contract: z.unknown()
})

export { registerGenerateCommand }
