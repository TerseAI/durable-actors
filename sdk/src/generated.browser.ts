import type { createActorStub as ServerStub } from "./backend.js"
import type { SocketProxy as ServerProxy } from "./proxy.js"

const createActorStub: typeof ServerStub = () => {
    throw new Error("actors must be used on the server")
}

const SocketProxy = class {
    constructor() {
        throw new Error("ActorProxy must be used on the server")
    }
} as unknown as typeof ServerProxy

export { createActorStub, SocketProxy }
