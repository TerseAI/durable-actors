import type { SocketProxy as ServerProxy } from "./proxy.js"
import type { createActorSessionTransport as ServerSessionTransport } from "./session.js"
import type {
    createActorStub as ServerStub,
    createActorTransport as ServerTransport,
    warmActor as ServerWarm
} from "./stub.js"

const createActorStub: typeof ServerStub = () => {
    throw new Error("actors must be used on the server")
}

const createActorTransport: typeof ServerTransport = () => {
    throw new Error("actors must be used on the server")
}
const createActorSessionTransport: typeof ServerSessionTransport = () => {
    throw new Error("actors must be used on the server")
}
const warmActor: typeof ServerWarm = () => {
    throw new Error("actors must be used on the server")
}

const SocketProxy = class {
    constructor() {
        throw new Error("ActorProxy must be used on the server")
    }
} as unknown as typeof ServerProxy

export { createActorStub, createActorTransport, createActorSessionTransport, SocketProxy, warmActor }
export { ActorInvocationError, ActorSessionRejectedError } from "./errors.js"
export type { ActorSession, ActorSessionTransportOptions, ActorSessionTransport } from "./session.js"
