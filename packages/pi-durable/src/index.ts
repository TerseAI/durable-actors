import type { HarnessOptions, ModelRef } from "@earendil-works/pi-durable"
import { ActorResource } from "durable-actors"
import type { Actor } from "durable-actors"

import { PiSession } from "./session.js"
import { ActorStorage } from "./storage.js"

export interface DurablePiAgentOptions extends HarnessOptions {
    readonly model: ModelRef
}

export interface DurablePiResult {
    readonly text: string
}

export class DurablePiAgent extends ActorResource {
    readonly session: PiSession

    constructor(actor: Pick<Actor, "db">, options: DurablePiAgentOptions) {
        super()
        this.session = new PiSession(() => new ActorStorage(actor.db), options)
    }

    async prompt(text: string): Promise<DurablePiResult> {
        return { text: await this.session.prompt(text) }
    }
}

export { PiSession }
export type { PiSessionScope } from "./session.js"
