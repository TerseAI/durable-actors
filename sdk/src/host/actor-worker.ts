import { fileURLToPath } from "node:url"
import { parentPort, workerData } from "node:worker_threads"

import { Actor, findActorDefinition, registerActorClass } from "../actor/actor.js"
import type { ActorClass } from "../actor/actor.js"
import { ACTOR_ARTIFACT_VERSION } from "../actor/schema.js"
import type { ActorSchema } from "../actor/schema.js"
import type { SocketConnection, SocketEffect } from "../actor/socketProtocol.js"
import { ActorConfigurationError, ActorDefinitionError, errorMessage } from "../errors.js"

import { ActorRuntime } from "./actor-runtime.js"
import { failedReply } from "./protocol.js"
import type { ActorWorkerData, ActorWorkerMessage, ActorWorkerRequest } from "./protocol.js"

const port = parentPort
if (port === null) throw new Error("actor Worker requires a parent message port")

const data = workerData as ActorWorkerData
let publishing: { resolve: () => void; reject: (error: Error) => void } | undefined
let loadingConnections:
    { resolve: (connections: readonly SocketConnection[]) => void; reject: (error: Error) => void } | undefined
try {
    const actorNames = await loadActorEntrypoint(data.moduleUrl, data.schemas)
    let runtime: ActorRuntime | undefined

    port.on("message", (message: ActorWorkerRequest) => {
        if (message.type === "socket_connections") {
            const pending = loadingConnections
            loadingConnections = undefined
            if (message.error === undefined) pending?.resolve(message.connections)
            else pending?.reject(new Error(message.error))
            return
        }
        if (message.type === "socket_effects_published") {
            const pending = publishing
            publishing = undefined
            if (message.error === undefined) pending?.resolve()
            else pending?.reject(new Error(message.error))
            return
        }
        const definition = findActorDefinition(message.command.actor.actor_name)
        if (definition === undefined) {
            post(
                failedReply(
                    "actor_name_not_found",
                    `actor entrypoint ${data.moduleUrl} does not export ${message.command.actor.actor_name}`
                )
            )
            return
        }
        runtime ??= new ActorRuntime(definition, publish, getConnections)
        void runtime.handle(message.command).then(
            reply => post(reply),
            error => post(failedReply("actor_worker_failed", errorMessage(error)))
        )
    })
    post({ type: "ready", actorNames })
} catch (error) {
    post(failedReply("actor_worker_failed", errorMessage(error)))
}

function post(message: ActorWorkerMessage): void {
    port!.postMessage(message)
}

function publish(effects: readonly SocketEffect[]): Promise<void> {
    return new Promise((resolve, reject) => {
        if (publishing !== undefined) throw new Error("actor socket output is already being published")
        publishing = { resolve, reject }
        post({ type: "socket_effects", effects })
    })
}

function getConnections(): Promise<readonly SocketConnection[]> {
    return new Promise((resolve, reject) => {
        if (loadingConnections !== undefined) throw new Error("actor connections are already being loaded")
        loadingConnections = { resolve, reject }
        post({ type: "get_connections" })
    })
}

async function loadActorEntrypoint(moduleUrl: string, schemas: readonly ActorSchema[] | undefined): Promise<string[]> {
    if (schemas !== undefined) return registerActors(await loadTypeScript(moduleUrl), schemas)
    const artifact = await import(moduleUrl)
    if (artifact.version !== ACTOR_ARTIFACT_VERSION || !Array.isArray(artifact.schemas) || !artifact.actors)
        throw new ActorConfigurationError("invalid actor artifact; rebuild with little-actors build")
    return registerActors(artifact.actors, artifact.schemas)
}

async function loadTypeScript(moduleUrl: string): Promise<Record<string, unknown>> {
    requireTypeScriptSource(fileURLToPath(moduleUrl))
    const unregister = (await import("tsx/esm/api")).register()
    try {
        return (await import(moduleUrl)) as Record<string, unknown>
    } finally {
        await unregister()
    }
}

function registerActors(actorModule: Record<string, unknown>, schemas: readonly ActorSchema[]): string[] {
    const actorNames: string[] = []
    for (const [exportName, value] of Object.entries(actorModule)) {
        if (!isActorClass(value)) continue
        if (exportName === "default") {
            throw new ActorDefinitionError("actor entrypoint must use named exports, not a default export")
        }
        if (Object.getPrototypeOf(value.prototype) !== Actor.prototype) {
            throw new ActorDefinitionError(
                `actor entrypoint export ${exportName} must be a class that directly extends Actor`
            )
        }
        if (value.name !== exportName) {
            throw new ActorDefinitionError(`actor entrypoint export ${exportName} must have the same class name`)
        }
        const schema = schemas.find(schema => schema.actorName === exportName)
        if (schema === undefined)
            throw new ActorDefinitionError(`actor ${exportName} has no validated schema; restart the actor host`)
        actorNames.push(registerActorClass(value, schema).actorName)
    }
    if (actorNames.length === 0) throw new ActorDefinitionError("actor entrypoint has no named actor exports")
    if (actorNames.length !== schemas.length)
        throw new ActorDefinitionError("actor exports do not match validated schemas; restart the actor host")
    actorNames.sort()
    return actorNames
}

function isActorClass(value: unknown): value is ActorClass {
    return typeof value === "function" && value.prototype instanceof Actor
}

function requireTypeScriptSource(filePath: string): void {
    if (!/\.(?:ts|tsx|mts|cts)$/u.test(filePath) || /\.d\.[cm]?ts$/u.test(filePath))
        throw new ActorConfigurationError("actor entrypoint must be a TypeScript source file")
}
