/** @module little-actors/backend */
import { validateActorComponent } from "./actor/identity.js"
import { actorClient } from "./client/client.js"
import type { ActorClientTransport } from "./client/client.js"
import type { DurableObjectsClientOptions } from "./client/remoteClient.js"
import { ActorDefinitionError, ActorSerializationError } from "./errors.js"

type ActorRpcTransport = Pick<ActorClientTransport, "invoke">

interface ActorRpcMethod {
    readonly name: string
    readonly result: "void" | "value"
}

/** Creates a backend client from generated method definitions. */
function createActorStub<Stub extends object>(
    actorName: string,
    actorId: string,
    methods: readonly ActorRpcMethod[],
    transport?: ActorRpcTransport
): Stub {
    validateActorComponent("actor name", actorName)
    validateActorComponent("actor ID", actorId)
    const stub = Object.create(null)
    for (const method of methods) {
        validateActorComponent("actor method", method.name)
        if (["then", "connect", "broadcast", "onConnect", "onMessage", "onDisconnect"].includes(method.name))
            throw new ActorDefinitionError(`actor method ${actorName}.${method.name} is reserved`)
        if (Object.hasOwn(stub, method.name))
            throw new ActorDefinitionError(`duplicate actor method ${actorName}.${method.name}`)
        Object.defineProperty(stub, method.name, {
            enumerable: true,
            value: async (...args: unknown[]) => {
                const parameters = invocationArguments(args)
                const client = transport ?? (await actorClient())
                const result = await client.invoke(actorName, actorId, method.name, parameters)
                return method.result === "void" ? undefined : result
            }
        })
    }
    return stub as Stub
}

function invocationArguments(args: readonly unknown[]): readonly unknown[] {
    let end = args.length
    while (end > 0 && args[end - 1] === undefined) end--
    const parameters = args.slice(0, end)
    if (parameters.includes(undefined))
        throw new ActorSerializationError("undefined cannot precede a supplied RPC argument in JSON transport")
    return parameters
}

export { createActorStub }
export type { ActorRpcMethod, ActorRpcTransport }

/** Connects backend clients using explicit settings. Keep the API key on the server. */
export function createActorTransport(options: DurableObjectsClientOptions): ActorRpcTransport {
    let client: Promise<ActorRpcTransport> | undefined
    return {
        async invoke(...args) {
            client ??= import("./client/remoteClient.js").then(
                ({ RemoteActorClient }) => new RemoteActorClient(options)
            )
            return (await client).invoke(...args)
        }
    }
}
export type { DurableObjectsClientOptions }
