import { chmod, cp, mkdir, mkdtemp, readFile, symlink, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import { fileURLToPath } from "node:url"

export async function projectSdkFixture() {
    const installed = fileURLToPath(new URL("../../../", import.meta.url))
    const directory = await mkdtemp(path.join(tmpdir(), "actor-project-sdk-"))
    const project = path.join(directory, "actor project")
    const sdk = path.join(project, "node_modules/durable-actors")
    await mkdir(sdk, { recursive: true })
    await cp(path.join(installed, "dist"), path.join(sdk, "dist"), { recursive: true })
    const metadata = JSON.parse(await readFile(path.join(installed, "package.json"), "utf8"))
    await writeFile(path.join(sdk, "package.json"), JSON.stringify({ ...metadata, version: "0.0.1" }))
    await symlink(path.join(installed, "node_modules"), path.join(sdk, "node_modules"), "dir")
    await writeFile(path.join(project, "package.json"), '{"type":"module"}')
    await writeFile(
        path.join(project, "actors.ts"),
        `import { Actor, Persisted } from "durable-actors"
        export class Counter extends Actor {
            @Persisted count = 0
            async increment(): Promise<{ id: string; count: number }> {
                return { id: this.id, count: ++this.count }
            }
        }`
    )
    await writeFile(
        path.join(project, "tsconfig.json"),
        JSON.stringify({ compilerOptions: { target: "ES2022", module: "NodeNext", strict: true, skipLibCheck: true } })
    )
    const binary = path.join(directory, "runtime.mjs")
    await cp(path.join(installed, "tests/fixtures/local-runtime-probe.mjs"), binary)
    await chmod(binary, 0o755)
    return { directory, project, sdk, binary }
}
