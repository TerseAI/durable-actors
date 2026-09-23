import { mkdtemp, rm, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import { test } from "node:test"

import { watchActorSources } from "../../src/cli/actor-source-watcher.js"

test("rebuilds for configuration and dependency changes", { timeout: 15_000 }, async t => {
    const root = await mkdtemp(path.join(tmpdir(), "actor-watch-"))
    let changed: (() => void) | undefined
    const watcher = await watchActorSources({ projectDirectory: root }, async () => changed?.())
    t.after(async () => {
        await watcher.close()
        await rm(root, { recursive: true, force: true })
    })
    for (const file of ["tsconfig.json", "package.json", "pnpm-lock.yaml", "helper.js"]) {
        const refreshed = new Promise<void>((resolve, reject) => {
            const deadline = setTimeout(() => reject(new Error(`${file} did not trigger a rebuild`)), 2500)
            changed = () => {
                clearTimeout(deadline)
                resolve()
            }
        })
        await writeFile(path.join(root, file), "{}")
        await refreshed
    }
})
