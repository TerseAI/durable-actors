import type { SocketEffect, SocketSource } from "../actor/socketProtocol.js"
import type { JsonValue } from "../json.js"

import type { ActorCallRequest } from "./protocol.js"

interface ActorHostSettings {
    readonly socketPath: string
    readonly actorEntrypoint: string | undefined
    readonly databaseFd: number | undefined
    readonly startupTimeoutMs: number
}

type SocketPublisher = (effects: readonly SocketEffect[]) => void

type ActorCallRequester = (call: ActorCallRequest) => Promise<JsonValue>

export type { ActorCallRequester, ActorHostSettings, SocketPublisher, SocketSource }
