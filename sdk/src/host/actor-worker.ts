import { fileURLToPath } from "node:url"
import { parentPort, workerData } from "node:worker_threads"

import { Actor, findActorDefinition, registerActorClass } from "../actor/actor.js"
import type { ActorClass } from "../actor/actor.js"
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
    const actorTypes = await loadActorEntrypoint(data.moduleUrl, data.schemas)
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
        const definition = findActorDefinition(message.command.actor.actor_type)
        if (definition === undefined) {
            post(
                failedReply(
                    "actor_type_not_found",
                    `actor entrypoint ${data.moduleUrl} does not export ${message.command.actor.actor_type}`
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
    post({ type: "ready", actorTypes })
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

async function loadActorEntrypoint(moduleUrl: string, schemas: readonly ActorSchema[]): Promise<string[]> {
    requireTypeScriptSource(fileURLToPath(moduleUrl))
    const unregister = (await import("tsx/esm/api")).register()
    let actorModule: Record<string, unknown>
    try {
        actorModule = (await import(moduleUrl)) as Record<string, unknown>
    } finally {
        await unregister()
    }
    const actorTypes: string[] = []
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
        const schema = schemas.find(schema => schema.actorType === exportName)
        if (schema === undefined)
            throw new ActorDefinitionError(`actor ${exportName} has no validated schema; restart the actor host`)
        actorTypes.push(registerActorClass(value, schema).actorType)
    }
    if (actorTypes.length === 0)
        throw new ActorDefinitionError(`actor entrypoint ${moduleUrl} has no named actor exports`)
    if (actorTypes.length !== schemas.length)
        throw new ActorDefinitionError("actor exports do not match validated schemas; restart the actor host")
    actorTypes.sort()
    return actorTypes
}

function isActorClass(value: unknown): value is ActorClass {
    return typeof value === "function" && value.prototype instanceof Actor
}

function requireTypeScriptSource(filePath: string): void {
    if (!/\.(?:ts|tsx|mts|cts)$/u.test(filePath) || /\.d\.[cm]?ts$/u.test(filePath))
        throw new ActorConfigurationError("actor entrypoint must be a TypeScript source file")
}
