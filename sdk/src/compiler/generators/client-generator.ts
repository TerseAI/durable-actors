import { mkdir, rm, writeFile } from "node:fs/promises"
import path from "node:path"

import type { SocketContract } from "../../wire/contract.js"
import type { PublicActorContract } from "../../wire/public-contract.js"

import { generateTypeScript } from "./typescript-generator.js"

async function generateClient(
    input: readonly SocketContract[] | PublicActorContract,
    directory: string
): Promise<void> {
    const artifacts = await generateTypeScript(input)
    await mkdir(directory, { recursive: true })
    await rm(path.join(directory, "runtime"), { recursive: true, force: true })
    for (const [file, contents] of artifacts) {
        const destination = path.join(directory, file)
        await mkdir(path.dirname(destination), { recursive: true })
        await writeFile(destination, contents)
    }
}

export { generateClient }
