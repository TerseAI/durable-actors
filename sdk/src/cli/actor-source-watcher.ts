import { watch } from "chokidar"
import path from "node:path"

interface ActorSourceWatcherOptions {
    projectDirectory: string
    dataDirectory?: string
}

interface ActorSourceWatcher {
    close(): Promise<void>
}

async function watchActorSources(
    options: ActorSourceWatcherOptions,
    refresh: () => Promise<void>
): Promise<ActorSourceWatcher> {
    const watcher = watch(options.projectDirectory, {
        ignored: watchedPath => ignoredActorPath(watchedPath, options),
        ignoreInitial: true,
        followSymlinks: false,
        atomic: true,
        awaitWriteFinish: { stabilityThreshold: 100, pollInterval: 20 }
    })
    let timer: ReturnType<typeof setTimeout> | undefined
    let updates = Promise.resolve()
    watcher.on("all", (_event, changedPath) => {
        if (!/\.(?:[cm]?[jt]sx?|json|ya?ml)$/u.test(changedPath) && path.basename(changedPath) !== "bun.lock") return
        clearTimeout(timer)
        timer = setTimeout(() => {
            updates = updates.then(refresh).catch(reportWatchError)
        }, 75)
    })
    await new Promise<void>((resolve, reject) => {
        watcher.once("ready", resolve)
        watcher.once("error", reject)
    }).catch(async error => {
        await watcher.close()
        throw error
    })
    watcher.on("error", reportWatchError)
    return {
        async close() {
            clearTimeout(timer)
            await watcher.close()
            await updates
        }
    }
}

function ignoredActorPath(candidate: string, options: ActorSourceWatcherOptions): boolean {
    const relative = path.relative(options.projectDirectory, candidate)
    if (!relative || relative.startsWith(`..${path.sep}`) || path.isAbsolute(relative)) return false
    const components = relative.split(path.sep)
    if (components.includes(".git") || components.includes("node_modules")) return true
    if ([".durable-actors", "generated"].includes(components[0])) return true
    if (!options.dataDirectory) return false
    const state = path.resolve(options.dataDirectory)
    return candidate === state || candidate.startsWith(`${state}${path.sep}`)
}

function reportWatchError(error: unknown): void {
    console.error(`Actor source update failed: ${error instanceof Error ? error.message : String(error)}`)
}

export { watchActorSources }
export type { ActorSourceWatcher }
