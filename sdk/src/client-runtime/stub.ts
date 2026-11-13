import { HttpActorClient } from "./client.js"
import { ActorDefinitionError, ActorSerializationError } from "./errors.js"
import type { DurableActorsClientOptions } from "./settings.js"
import { validateActorComponent } from "./settings.js"

interface ActorRpcTransport {
    invoke(actorName: string, actorId: string, method: string, args: readonly unknown[]): Promise<unknown>
    warm?(actorName: string, actorId: string, warmMs: number, options?: WarmOptions): Promise<WarmStatus>
}

/** Waits until the host is warm by default; `waitForCompletion: false` replies at once or within `timeoutMs`. */
type WarmOptions =
    | { readonly waitForCompletion?: true; readonly timeoutMs?: never }
    | { readonly waitForCompletion: false; readonly timeoutMs?: number }

interface WarmStatus {
    readonly status: "OK" | "accepted"
}

interface ActorWarm {
    /** Keeps this actor's host up for `warmMs` after it is ready, starting it if needed. */
    warm(warmMs: number, options?: WarmOptions): Promise<WarmStatus>
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
                "onAlarm",
                "warm"
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
    Object.defineProperty(stub, "warm", {
        value: (warmMs: number, options?: WarmOptions) =>
            warmActor(actorName, actorId, warmMs, options, transport ?? defaultTransport())
    })
    return stub as Stub
}

async function warmActor(
    actorName: string,
    actorId: string,
    warmMs: number,
    options: WarmOptions | undefined,
    transport: ActorRpcTransport
): Promise<WarmStatus> {
    if (!transport.warm) throw new ActorDefinitionError("actor transport does not support warm")
    return transport.warm(actorName, actorId, warmMs, options)
}

export { createActorStub, warmActor }
export type { ActorRpcMethod, ActorRpcTransport, ActorWarm, WarmOptions, WarmStatus }

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
