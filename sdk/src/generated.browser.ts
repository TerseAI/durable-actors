import type { createActorStub as ServerStub } from "./backend.js"
import type { SocketProxy as ServerProxy } from "./proxy.js"

export { createClient } from "./browser.js"

const createActorStub: typeof ServerStub = () => {
    throw new Error("actors must be used on the server; use clients in the browser")
}

const SocketProxy = class {
    constructor() {
        throw new Error("ActorProxy must be used on the server")
    }
} as unknown as typeof ServerProxy

export { createActorStub, SocketProxy }
