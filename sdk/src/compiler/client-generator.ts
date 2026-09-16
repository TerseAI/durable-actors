import { mkdir, rm, writeFile } from "node:fs/promises"
import path from "node:path"

import type { SocketContract } from "../wire/contract.js"
import type { PublicActorContract } from "../wire/public-contract.js"

import { generateTypeScript } from "./typescript-generator.js"

async function generateClient(
    input: readonly SocketContract[] | PublicActorContract,
    directory: string
): Promise<void> {
    const artifacts = await generateTypeScript(input)
    const contracts = "actors" in input ? input.actors.map(actor => actor.socket) : input
    await mkdir(directory, { recursive: true })
    for (const [file, contents] of artifacts) await writeFile(path.join(directory, file), contents)
    for (const { actorType } of contracts)
        for (const suffix of ["validators.js", "validators.d.ts", "proxy-validators.js", "proxy-validators.d.ts"])
            await rm(path.join(directory, `${actorType}.${suffix}`), { force: true })
}

export { generateClient }
