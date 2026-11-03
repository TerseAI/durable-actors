import { HttpActorClient } from "./client.js"
import type { ActorWarmReply } from "./client.js"
import { ActorDefinitionError, ActorSerializationError } from "./errors.js"
import type { DurableActorsClientOptions } from "./settings.js"
import { validateActorComponent } from "./settings.js"

interface ActorRpcTransport {
    invoke(actorName: string, actorId: string, method: string, args: readonly unknown[]): Promise<unknown>
    warm?(actorName: string, actorId: string): Promise<ActorWarmReply>
}

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
        if (
            [
                "then",
                "connect",
                "broadcast",
                "db",
                "alarm",
                "onConnect",
                "onMessage",
                "onDisconnect",
                "onAlarm"
            ].includes(method.name)
        )
            throw new ActorDefinitionError(`actor method ${actorName}.${method.name} is reserved`)
        if (Object.hasOwn(stub, method.name))
            throw new ActorDefinitionError(`duplicate actor method ${actorName}.${method.name}`)
        Object.defineProperty(stub, method.name, {
            enumerable: true,
            value: async (...args: unknown[]) => {
                const parameters = invocationArguments(args)
                const client = transport ?? defaultTransport()
                const result = await client.invoke(actorName, actorId, method.name, parameters)
                return method.result === "void" ? undefined : result
            }
        })
    }
    return stub as Stub
}

/** Asks the control plane to start the actor's host before calls arrive. */
async function warmActor(actorName: string, actorId: string, transport?: ActorRpcTransport): Promise<ActorWarmReply> {
    validateActorComponent("actor name", actorName)
    validateActorComponent("actor ID", actorId)
    const client = transport ?? defaultTransport()
    if (!client.warm) throw new Error("this actor transport does not support warm")
    return client.warm(actorName, actorId)
}

export { createActorStub, warmActor }
export type { ActorRpcMethod, ActorRpcTransport, ActorWarmReply }

let shared: ActorRpcTransport | undefined
export function createActorTransport(options: DurableActorsClientOptions): ActorRpcTransport {
    return new HttpActorClient(options)
}

function defaultTransport(): ActorRpcTransport {
    return (shared ??= new HttpActorClient())
}
export type { DurableActorsClientOptions }

function invocationArguments(args: readonly unknown[]): readonly unknown[] {
    let end = args.length
    while (end > 0 && args[end - 1] === undefined) end--
    const parameters = args.slice(0, end)
    if (parameters.includes(undefined))
        throw new ActorSerializationError("undefined cannot precede a supplied RPC argument in JSON transport")
    return parameters
}
