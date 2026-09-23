import { execFile } from "node:child_process"
import { once } from "node:events"
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises"
import { createServer } from "node:net"
import { tmpdir } from "node:os"
import path from "node:path"
import { test } from "node:test"
import { promisify } from "node:util"

import { watchActorSources } from "../../src/cli/actor-source-watcher.js"

test("reloads nested sources in projects containing sockets and FIFOs", { timeout: 10_000 }, async t => {
    const root = await mkdtemp(path.join(tmpdir(), "aw-"))
    const server = createServer()
    let watcher: Awaited<ReturnType<typeof watchActorSources>> | undefined
    t.after(async () => {
        await watcher?.close()
        if (server.listening)
            await new Promise<void>((resolve, reject) => server.close(error => (error ? reject(error) : resolve())))
        await rm(root, { recursive: true, force: true })
    })
    await mkdir(path.join(root, ".config"))
    server.listen(path.join(root, ".config", "client.sock"))
    await once(server, "listening")
    await promisify(execFile)("mkfifo", [path.join(root, ".config", "events.ts")])
    let refresh!: () => void
    const refreshed = new Promise<void>(resolve => {
        refresh = resolve
    })
    watcher = await watchActorSources({ projectDirectory: root }, async () => refresh())
    await mkdir(path.join(root, "src"))
    await writeFile(path.join(root, "src", "actors.ts"), "export {}")
    await refreshed
})

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
