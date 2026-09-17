import { validateActorComponent } from "./actor/identity.js"
import { actorClient } from "./client/client.js"
import type { ActorClientTransport } from "./client/client.js"
import { ActorDefinitionError, ActorSerializationError } from "./errors.js"

type ActorRpcTransport = Pick<ActorClientTransport, "invoke">

interface ActorRpcMethod {
    readonly name: string
    readonly result: "void" | "value"
}

function createActorStub<Stub extends object>(
    actorType: string,
    actorId: string,
    methods: readonly ActorRpcMethod[],
    transport?: ActorRpcTransport
): Stub {
    validateActorComponent("actor type", actorType)
    validateActorComponent("actor ID", actorId)
    const stub = Object.create(null)
    for (const method of methods) {
        validateActorComponent("actor method", method.name)
        if (["then", "connect", "broadcast", "onConnect", "onMessage", "onDisconnect"].includes(method.name))
            throw new ActorDefinitionError(`actor method ${actorType}.${method.name} is reserved`)
        if (Object.hasOwn(stub, method.name))
            throw new ActorDefinitionError(`duplicate actor method ${actorType}.${method.name}`)
        Object.defineProperty(stub, method.name, {
            enumerable: true,
            value: async (...args: unknown[]) => {
                const parameters = invocationArguments(args)
                const client = transport ?? (await actorClient())
                const result = await client.invoke(actorType, actorId, method.name, parameters)
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
