import { Harness, HarnessOptions } from "@earendil-works/pi-durable"
import { ActorResource } from "durable-actors"
import type { Actor } from "durable-actors"

import { ActorStorage } from "./storage.js"
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context"

export class DurablePiAgent {
    static async create(actor: Pick<Actor, "db">, options: HarnessOptions): Promise<Harness> {
        return Harness.open(new ActorStorage(actor.db), options, BACKGROUND_CONTEXT)
    }
}
