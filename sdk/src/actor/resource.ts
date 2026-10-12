import type { ActorDatabase } from "durable-actors"

interface ActorResourceContext {
    db: ActorDatabase
}

interface ActorResource {
    onResume({ db }: ActorResourceContext): Promise<void>
    onShutdown({ db }: ActorResourceContext): Promise<void>
}

export { ActorResource }
