import { mkdir, rm, writeFile } from "node:fs/promises"
import path from "node:path"

import type { PublicActorContract } from "../../wire/public-contract.js"

import { generateClientArtifacts } from "./client-artifacts.js"

async function generateClient(input: PublicActorContract, directory: string): Promise<void> {
    const artifacts = await generateClientArtifacts(input)
    await mkdir(directory, { recursive: true })
    await rm(path.join(directory, "runtime"), { recursive: true, force: true })
    for (const [file, contents] of artifacts) {
        const destination = path.join(directory, file)
        await mkdir(path.dirname(destination), { recursive: true })
        await writeFile(destination, contents)
    }
}

export { generateClient }
