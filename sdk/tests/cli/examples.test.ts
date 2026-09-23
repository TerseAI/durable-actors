import { execFile } from "node:child_process"
import { copyFile, mkdtemp, rm, symlink } from "node:fs/promises"
import path from "node:path"
import { test } from "node:test"
import { fileURLToPath } from "node:url"
import { promisify } from "node:util"

const run = promisify(execFile)
const sdk = fileURLToPath(new URL("../../../", import.meta.url))

for (const template of ["chat", "ai-chat", "documents"]) {
    test(
        `the ${template} template builds its app and actor contract from a fresh init`,
        { timeout: 60_000 },
        async t => {
            // Match the examples' directory depth so pnpm's relative executable paths remain valid.
            const directory = await mkdtemp(path.resolve(sdk, "../.durable-actors-example-"))
            t.after(() => rm(directory, { recursive: true, force: true }))
            const project = path.join(directory, template)
            await run(process.execPath, [path.join(sdk, "dist/cli.js"), "init", template, "--template", template], {
                cwd: directory
            })
            await copyFile(path.join(project, ".env.example"), path.join(project, ".env"))
            await symlink(
                path.resolve(sdk, "../examples", template, "node_modules"),
                path.join(project, "node_modules")
            )
            await run("npm", ["run", "build"], { cwd: project })
            await run(process.execPath, [path.join(sdk, "dist/cli.js"), "generate"], { cwd: project })
        }
    )
}
