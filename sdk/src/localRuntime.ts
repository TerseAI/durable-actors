import { type ChildProcess, spawn } from "node:child_process"
import path from "node:path"
import type { Readable } from "node:stream"
import { fileURLToPath } from "node:url"
import { z } from "zod"

import { fetchRuntimeExecutablePath } from "./runtimeInstaller.js"

export interface LocalActorOptions {
    apiKey?: string
    entrypoint: string
    project?: string
    dataDir?: string
    port?: number
    startupTimeoutMs?: number
    quiet?: boolean
}

const connectionSchema = z.object({
    controlPlaneUrl: z.string().url(),
    apiKey: z.string().min(1),
    namespaceId: z.string().min(1),
    storageRegion: z.string().min(1),
    pid: z.number().int().positive()
})

export interface LocalActorRuntime {
    connection: z.infer<typeof connectionSchema>
    closed: Promise<void>
    stop(): Promise<void>
}

/** Resolves when the local server is ready; stopping it preserves its data directory. */
export async function startLocalActors(options: LocalActorOptions): Promise<LocalActorRuntime> {
    const child = launch(await fetchRuntimeExecutablePath(), options)
    const { closed, stop } = lifecycle(child)
    try {
        const connection = await waitForConnection(
            child.stdio[3] as Readable,
            closed,
            options.startupTimeoutMs ?? 120_000
        )
        return { connection, closed, stop }
    } catch (error) {
        await stop().catch(() => {})
        throw error
    }
}

function launch(executable: string, options: LocalActorOptions) {
    const project = path.resolve(options.project ?? ".")
    return spawn(
        executable,
        [
            "dev",
            ...(options.apiKey === undefined ? [] : ["--api-key", options.apiKey]),
            "--project",
            project,
            "--entrypoint",
            path.resolve(project, options.entrypoint),
            "--data-dir",
            path.resolve(project, options.dataDir ?? ".little-actors"),
            "--port",
            String(options.port ?? 0),
            "--ready-fd",
            "3",
            "--sdk-host",
            fileURLToPath(new URL("./host.js", import.meta.url))
        ],
        {
            cwd: project,
            env: {
                ...process.env,
                PATH: `${path.dirname(process.execPath)}${path.delimiter}${path.dirname(executable)}${path.delimiter}${process.env.PATH ?? ""}`,
                DURABLE_OBJECT_PROCESS_ROLE: "control_plane",
                DURABLE_OBJECT_PARENT_LIFETIME_STDIN: "1"
            },
            stdio: ["pipe", options.quiet ? "ignore" : "inherit", options.quiet ? "ignore" : "inherit", "pipe"]
        }
    )
}

function lifecycle(child: ChildProcess) {
    let stopping = false
    const closed = new Promise<void>((resolve, reject) => {
        child.once("error", reject)
        child.once("exit", (code, signal) => {
            if (code === 0 || stopping) resolve()
            else reject(new Error(`Local actors exited (${signal ?? code})`))
        })
    })
    void closed.catch(() => {})
    let stopped: Promise<void> | undefined
    const stop = () =>
        (stopped ??= (async () => {
            stopping = true
            child.stdin!.end()
            const timer = setTimeout(() => child.kill("SIGKILL"), 10_000)
            try {
                await closed
            } finally {
                clearTimeout(timer)
            }
        })())
    return { closed, stop }
}

async function waitForConnection(readiness: Readable, closed: Promise<void>, timeoutMs: number) {
    let timer: ReturnType<typeof setTimeout> | undefined
    try {
        const connection = await Promise.race([
            readConnection(readiness),
            closed.then(() => {
                throw new Error("Local actors exited before readiness")
            }),
            new Promise<never>((_, reject) => {
                timer = setTimeout(() => reject(new Error("Local actors startup timed out")), timeoutMs)
            })
        ])
        return connection
    } finally {
        clearTimeout(timer)
        readiness.destroy()
    }
}

async function readConnection(stream: Readable) {
    const chunks: Buffer[] = []
    for await (const chunk of stream) chunks.push(Buffer.from(chunk))
    if (chunks.length === 0) throw new Error("Local actors exited without readiness information")
    return connectionSchema.parse(JSON.parse(Buffer.concat(chunks).toString("utf8")))
}
