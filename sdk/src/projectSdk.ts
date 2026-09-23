import { resolve } from "import-meta-resolve"
import { realpath } from "node:fs/promises"
import path from "node:path"
import { fileURLToPath, pathToFileURL } from "node:url"

export async function projectSdkModule(
    project: string,
    entrypoint: string,
    caller: string
): Promise<string | undefined> {
    let target: string
    try {
        const parent = pathToFileURL(path.resolve(project, "package.json")).href
        const host = resolve("durable-actors/host", parent)
        target = await realpath(fileURLToPath(new URL(entrypoint, host)))
    } catch (cause) {
        throw new Error(`Cannot load the durable-actors SDK in ${project}. Run pnpm install in the actor project.`, {
            cause
        })
    }
    return target === (await realpath(fileURLToPath(caller))) ? undefined : pathToFileURL(target).href
}
