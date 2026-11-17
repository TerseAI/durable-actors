import type { SocketProxy as ServerProxy } from "./proxy.js"
import type { createActorSessionTransport as ServerSessionTransport } from "./session.js"
import type { createActorStub as ServerStub, createActorTransport as ServerTransport } from "./stub.js"

const serverOnly =
    "actors must be used on the server. From a browser, open the websocketUrl your backend gets from prepareWebsocket; for local demos only, regenerate with DURABLE_ACTORS_ALLOW_BROWSER=1."

const createActorStub: typeof ServerStub = () => {
    throw new Error(serverOnly)
}

const createActorTransport: typeof ServerTransport = () => {
    throw new Error(serverOnly)
}
const createActorSessionTransport: typeof ServerSessionTransport = () => {
    throw new Error(serverOnly)
}

const SocketProxy = class {
    constructor() {
        throw new Error("ActorProxy must be used on the server")
    }
} as unknown as typeof ServerProxy

export { createActorStub, createActorTransport, createActorSessionTransport, SocketProxy }
export { ActorInvocationError, ActorSessionRejectedError } from "./errors.js"
export type { ActorSession, ActorSessionTransportOptions, ActorSessionTransport } from "./session.js"
