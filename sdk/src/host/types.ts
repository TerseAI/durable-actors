import type { ActorSchema } from "../actor/schema.js"
import type { SocketConnection, SocketEffect } from "../actor/socketProtocol.js"

import type {
    ActorExecutorCommand,
    ActorExecutorReply,
    ActorWorkerData,
    HydrateCommand,
    InvokeCommand,
    WebSocketEventCommand
} from "./protocol.js"
import type { ActorWorkerSupervisor, ResidentActorWorker } from "./worker-supervisor.js"

interface ActorHostSettings {
    readonly socketPath: string
    readonly actorEntrypoint: string | undefined
    readonly startupTimeoutMs: number
    readonly actorIdleTimeoutMs: number
}

type SocketPublisher = (effects: readonly SocketEffect[]) => Promise<void>
type SocketSource = () => Promise<readonly SocketConnection[]>

type ActorCommandHandler = (
    command: ActorExecutorCommand,
    publish?: SocketPublisher,
    connections?: SocketSource
) => Promise<ActorExecutorReply>

type ActorWorkerSupervisorFactory = (
    options: ActorWorkerSupervisorOptions
) => Pick<ActorWorkerSupervisor, "ready" | "handle" | "close">

interface ActorWorkerSupervisorOptions {
    readonly actorEntrypointUrl: string
    readonly actorSchemas: readonly ActorSchema[] | undefined
    readonly actorIdleTimeoutMs?: number
    readonly createWorker?: ActorWorkerFactory
}

interface ResidentActorWorkerOptions {
    readonly moduleUrl: string
    readonly schemas: readonly ActorSchema[] | undefined
    readonly idleTimeoutMs: number
    readonly worker?: ActorWorkerHandle
    readonly createWorker: ActorWorkerFactory
    readonly onIdle: (actor: ResidentActorWorker) => void
}

interface ActorWorkerHandle {
    ready(): Promise<readonly string[]>
    execute(
        command: InvokeCommand | WebSocketEventCommand | HydrateCommand,
        publish?: SocketPublisher,
        connections?: SocketSource
    ): Promise<ActorExecutorReply>
    terminate(reason: string): void
}

type ActorWorkerFactory = (data: ActorWorkerData) => ActorWorkerHandle

export type {
    ActorCommandHandler,
    ActorHostSettings,
    ActorWorkerFactory,
    ActorWorkerHandle,
    ActorWorkerSupervisorFactory,
    ActorWorkerSupervisorOptions,
    ResidentActorWorkerOptions,
    SocketPublisher,
    SocketSource
}
