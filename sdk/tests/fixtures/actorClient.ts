import { validateActorComponent } from "../../src/actor/identity.js"
import { socketMessage } from "../../src/actor/socket.js"
import type { ActorConnection, ActorSocketMessage } from "../../src/actor/socket.js"
import type { SocketMessage } from "../../src/actor/socketProtocol.js"
import { runWithActorClient } from "../../src/client/client.js"
import type { ActorClientTransport } from "../../src/client/client.js"
import { ActorProtocolError } from "../../src/errors.js"
import { cloneJson } from "../../src/json.js"
import type { JsonValue } from "../../src/json.js"

function runWithActorClientForTests<T>(options: ActorTestClientOptions, operation: () => T): T {
    return runWithActorClient(new TestActorClient(options), operation)
}

class TestActorClient implements ActorClientTransport {
    private readonly requestId: () => string
    private readonly invokeTest: ActorTestInvoker
    private readonly connectTest: ActorTestConnector
    private readonly broadcastTest: ActorTestBroadcaster

    constructor(options: ActorTestClientOptions) {
        this.requestId = options.requestId ?? (() => globalThis.crypto.randomUUID())
        this.invokeTest = options.invoke
        this.connectTest =
            options.connect ??
            (() => Promise.reject(new ActorProtocolError("socket connections are not configured for this test")))
        this.broadcastTest =
            options.broadcast ??
            (() => Promise.reject(new ActorProtocolError("socket broadcasts are not configured for this test")))
    }

    async invoke(actorType: string, actorId: string, method: string, args: readonly unknown[]): Promise<unknown> {
        const requestId = validateActorComponent("request ID", this.requestId())
        const serializedArgs = cloneJson(args, "actor arguments")
        if (!Array.isArray(serializedArgs)) throw new ActorProtocolError("actor arguments must be a JSON array")
        const request: ActorInvocationRequest = {
            requestId,
            actorType: validateActorComponent("actor type", actorType),
            actorId: validateActorComponent("actor ID", actorId),
            method: validateActorComponent("actor method", method),
            args: serializedArgs
        }
        return this.invokeTest(request)
    }

    async connect(actorType: string, actorId: string, metadata: unknown): Promise<ActorConnection> {
        const request: ActorConnectionRequest = {
            requestId: validateActorComponent("request ID", this.requestId()),
            actorType: validateActorComponent("actor type", actorType),
            actorId: validateActorComponent("actor ID", actorId),
            metadata: cloneJson(metadata, "socket metadata")
        }
        return this.connectTest(request)
    }

    async broadcast(actorType: string, actorId: string, message: ActorSocketMessage): Promise<void> {
        return this.broadcastTest({
            requestId: validateActorComponent("request ID", this.requestId()),
            actorType: validateActorComponent("actor type", actorType),
            actorId: validateActorComponent("actor ID", actorId),
            message: socketMessage(message)
        })
    }
}

interface ActorInvocationRequest {
    readonly requestId: string
    readonly actorType: string
    readonly actorId: string
    readonly method: string
    readonly args: readonly JsonValue[]
}

interface ActorTestClientOptions {
    readonly requestId?: () => string
    readonly invoke: ActorTestInvoker
    readonly connect?: ActorTestConnector
    readonly broadcast?: ActorTestBroadcaster
}

type ActorTestInvoker = (request: ActorInvocationRequest) => Promise<unknown>
type ActorTestConnector = (request: ActorConnectionRequest) => Promise<ActorConnection>
type ActorTestBroadcaster = (request: ActorBroadcastRequest) => Promise<void>

interface ActorConnectionRequest {
    readonly requestId: string
    readonly actorType: string
    readonly actorId: string
    readonly metadata: JsonValue
}

interface ActorBroadcastRequest {
    readonly requestId: string
    readonly actorType: string
    readonly actorId: string
    readonly message: SocketMessage
}

export { runWithActorClientForTests }
