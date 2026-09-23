import { resolve } from "import-meta-resolve"
import { realpath } from "node:fs/promises"
import path from "node:path"
import { fileURLToPath, pathToFileURL } from "node:url"

import { ActorConfigurationError } from "./errors.js"

export async function projectSdkModule(project: string, entrypoint: "cli" | "dev"): Promise<string> {
    const directory = path.resolve(project)
    try {
        const parent = pathToFileURL(path.join(directory, "package.json")).href
        const module = resolve(`durable-actors/${entrypoint}`, parent)
        return pathToFileURL(await realpath(fileURLToPath(module))).href
    } catch (cause) {
        throw new ActorConfigurationError(
            `Cannot resolve durable-actors/${entrypoint} in ${directory}. Run pnpm install in the actor project and ensure its durable-actors version exports ./${entrypoint}.`,
            { cause }
        )
    }
}
