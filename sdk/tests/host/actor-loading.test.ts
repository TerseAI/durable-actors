import assert from "node:assert/strict"
import { once } from "node:events"
import { mkdir, mkdtemp, realpath, rm, writeFile } from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { test } from "node:test"
import { fileURLToPath, pathToFileURL } from "node:url"
import { Worker } from "node:worker_threads"

import type { ActorSchema } from "../../src/actor/schema.js"
import { ActorConfigurationError, ActorDefinitionError } from "../../src/errors.js"
import { resolveActorEntrypoint } from "../../src/host/actor-host.js"
import type { ActorWorkerMessage } from "../../src/host/protocol.js"

test("resolves the conventional built actor entrypoint", async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), "durable-object-entrypoint-"))
    const previousDirectory = process.cwd()
    try {
        await mkdir(path.join(root, "dist"))
        const entrypoint = path.join(root, "dist/actors.mjs")
        await writeFile(entrypoint, "export {}\n")
        process.chdir(root)
        assert.equal(await realpath(fileURLToPath(await resolveActorEntrypoint(undefined))), await realpath(entrypoint))
    } finally {
        process.chdir(previousDirectory)
        await rm(root, { recursive: true, force: true })
    }
})

test("rejects a configured actor entrypoint that does not exist", async () => {
    await assert.rejects(resolveActorEntrypoint("./missing-durable-objects.ts"), ActorConfigurationError)
})

test("uses the built entrypoint by default even when TypeScript source is present", async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), "durable-object-compiled-"))
    const previousDirectory = process.cwd()
    try {
        await mkdir(path.join(root, "src"))
        await mkdir(path.join(root, "dist"))
        await writeFile(path.join(root, "src/durable-objects.ts"), "export {}\n")
        await writeFile(path.join(root, "dist/actors.mjs"), "export {}\n")
        process.chdir(root)
        assert.equal(
            await realpath(fileURLToPath(await resolveActorEntrypoint(undefined))),
            await realpath(path.join(root, "dist/actors.mjs"))
        )
    } finally {
        process.chdir(previousDirectory)
        await rm(root, { recursive: true, force: true })
    }
})

test("rejects configured JavaScript actor entrypoints", async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), "durable-object-javascript-entrypoint-"))
    try {
        const entrypoint = path.join(root, "actors.js")
        await writeFile(entrypoint, "export {}\n")
        await assert.rejects(resolveActorEntrypoint(entrypoint), /TypeScript source/)
    } finally {
        await rm(root, { recursive: true, force: true })
    }
})

test("rejects an incompatible built actor artifact", async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), "durable-object-artifact-"))
    try {
        const entrypoint = path.join(root, "actors.mjs")
        await writeFile(entrypoint, "export const version = 999; export const actors = {}; export const schemas = []")
        await assert.rejects(loadActorNames(pathToFileURL(entrypoint).href), /rebuild with durable-actors build/)
    } finally {
        await rm(root, { recursive: true, force: true })
    }
})

test("loads only actors from an entrypoint with mixed exports", async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), "durable-object-mixed-entrypoint-"))
    try {
        const entrypoint = path.join(root, "mixed.mts")
        const sdk = new URL("../../src/index.js", import.meta.url).href
        await writeFile(
            entrypoint,
            `import { Actor } from ${JSON.stringify(sdk)}
            export { Actor }
            export const limit = 10
            export const empty = null
            export const callback = () => 1
            export function helper() { return limit }
            export class Utility { value = 1 }
            export default { limit }
            export class MixedRoom extends Actor {}
            export class MixedCounter extends Actor {}`
        )
        const schemas = ["MixedCounter", "MixedRoom"].map(actorName => ({ actorName, fields: [] }))
        assert.deepEqual(await loadActorNames(pathToFileURL(entrypoint).href, schemas), ["MixedCounter", "MixedRoom"])
    } finally {
        await rm(root, { recursive: true, force: true })
    }
})

test("rejects invalid actor exports while ignoring unrelated exports", async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), "durable-object-invalid-entrypoint-"))
    try {
        const sdk = new URL("../../src/index.js", import.meta.url).href
        for (const [name, declaration, message] of [
            ["default", "export default class Counter extends Actor {}", /named exports/],
            ["alias", "class Counter extends Actor {}; export { Counter as Renamed }", /same class name/],
            ["indirect", "class Base extends Actor {}; export class Counter extends Base {}", /directly extends Actor/],
            ["non-actor", "export class Utility {}", /named actor exports/]
        ] as const) {
            const entrypoint = path.join(root, `${name}.mts`)
            await writeFile(
                entrypoint,
                `import { Actor } from ${JSON.stringify(sdk)}
                export const helper = 1
                ${declaration}`
            )
            await assert.rejects(loadActorNames(pathToFileURL(entrypoint).href, []), error => {
                assert.ok(error instanceof ActorDefinitionError)
                assert.match(error.message, message)
                return true
            })
        }
    } finally {
        await rm(root, { recursive: true, force: true })
    }
})

async function loadActorNames(moduleUrl: string, schemas?: readonly ActorSchema[]): Promise<readonly string[]> {
    const worker = new Worker(new URL("../../src/host/actor-worker.js", import.meta.url), {
        workerData: { moduleUrl, schemas }
    })
    try {
        const [message] = (await once(worker, "message", { signal: AbortSignal.timeout(5_000) })) as [
            ActorWorkerMessage
        ]
        if (message.type === "failed") throw new ActorDefinitionError(message.message)
        assert.equal(message.type, "ready")
        return message.actorNames
    } finally {
        await worker.terminate()
    }
}
