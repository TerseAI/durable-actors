import { createActorStub as runtimeStub } from "../backend.js"
import { SocketProxy as RuntimeProxy } from "../proxy.js"

import type {
    ActorRpcMethod,
    ActorRpcTransport,
    ProxyActor,
    SocketAuthorization,
    SocketGrant,
    SocketProxyDependencies,
    SocketProxyOptions
} from "./types.js"

export function createActorStub<Stub extends object>(
    actorName: string,
    actorId: string,
    methods: readonly ActorRpcMethod[],
    transport?: ActorRpcTransport
): Stub {
    return runtimeStub(actorName, actorId, methods, transport)
}

export class SocketProxy<Actors extends Record<string, ProxyActor>> {
    private readonly proxy

    constructor(actors: Actors, options: SocketProxyOptions = {}, dependencies: SocketProxyDependencies = {}) {
        this.proxy = new RuntimeProxy(actors, options, dependencies)
    }

    handle(authorization: SocketAuthorization<Actors>): Promise<SocketGrant> {
        return this.proxy.handle(authorization)
    }
}
