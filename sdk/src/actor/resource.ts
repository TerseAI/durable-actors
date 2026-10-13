import type { ActorDatabase } from "durable-actors"

export interface ActorResourceContext {
    db: ActorDatabase
}

export interface ActorResource {
    onResume({ db }: ActorResourceContext): Promise<void>
    onGracefulShutdown({ db }: ActorResourceContext): Promise<void>
}