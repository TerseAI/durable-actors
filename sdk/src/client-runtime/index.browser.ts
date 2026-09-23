import type { SocketProxy as ServerProxy } from "./proxy.js"
import type { createActorStub as ServerStub, createActorTransport as ServerTransport } from "./stub.js"

const createActorStub: typeof ServerStub = () => {
    throw new Error("actors must be used on the server")
}

const createActorTransport: typeof ServerTransport = () => {
    throw new Error("actors must be used on the server")
}

const SocketProxy = class {
    constructor() {
        throw new Error("ActorProxy must be used on the server")
    }
} as unknown as typeof ServerProxy

export { createActorStub, createActorTransport, SocketProxy }
export { ActorInvocationError } from "./errors.js"
