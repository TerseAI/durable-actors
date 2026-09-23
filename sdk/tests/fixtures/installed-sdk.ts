import { cp, mkdir, readFile, realpath, symlink, writeFile } from "node:fs/promises"
import path from "node:path"
import { fileURLToPath } from "node:url"

export async function installSdk(project: string, version?: string): Promise<string> {
    const source = fileURLToPath(new URL("../../../", import.meta.url))
    const sdk = path.join(project, "node_modules/durable-actors")
    await mkdir(sdk, { recursive: true })
    await cp(path.join(source, "dist"), path.join(sdk, "dist"), { recursive: true })
    const metadata = JSON.parse(await readFile(path.join(source, "package.json"), "utf8"))
    await writeFile(
        path.join(sdk, "package.json"),
        JSON.stringify({ ...metadata, version: version ?? metadata.version })
    )
    await symlink(path.join(source, "node_modules"), path.join(sdk, "node_modules"), "dir")
    await writeFile(path.join(project, "package.json"), '{"type":"module"}')
    return realpath(sdk)
}
