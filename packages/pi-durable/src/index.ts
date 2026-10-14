import { contentText } from "@earendil-works/pi-ai"
import { AssistantEntry, Harness, type HarnessOptions, type ModelRef } from "@earendil-works/pi-durable"
import type { Actor, ActorResource, ActorResourceContext } from "durable-actors"

import { ActorStorage } from "./storage.js"
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context"

const context = BACKGROUND_CONTEXT;

export type DurablePiAgentOptions = HarnessOptions & { readonly model: ModelRef }

export class DurablePiAgent implements ActorResource {
    private _harness?: Harness
    private readonly options: DurablePiAgentOptions


    get harness(): Harness {
        if (!this._harness) {
            throw new Error("DurablePiAgent has not resumed")
        }
        return this._harness
    }

    constructor(options: DurablePiAgentOptions) {
        this.options = options
    }

    async prompt(text: string): Promise<string> {
         const root = await this.harness.root(context)

         const job = {
            type: "input",
            content: text,
        } as const;
        
        const submission = await root.submit(job, context);
        const settled = await submission.wait(context);

        if (settled.status === "unanswered") 
            throw new Error(`Pi did not answer: ${settled.reason}${typeof settled.detail === "string" ? `: ${settled.detail}` : ""}`)

        if (settled.status !== "done" || settled.type !== "input")
            throw new Error(`Pi did not answer: ${settled.reason}`)

        const answer = await root.commit((tx) => tx.entry(AssistantEntry, settled.answer), context);

        if (answer === undefined)
            throw new Error("Pi did not answer")

        if (answer.model === undefined)
            throw new Error("The Pi model did not provide an answer")

        return contentText(answer.model[0].content)
    }

    async onResume({ db }: ActorResourceContext): Promise<void> {
        this._harness = await Harness.open(new ActorStorage(db), this.options, context)
        const root = await this._harness.root(context)
        await root.configure({ model: this.options.model }, context)
        this._harness.resume();
    }

    async onGracefulShutdown({ db }: ActorResourceContext): Promise<void> {
        if (this.harness === undefined) return
        await this.harness.close(context)
        this._harness = undefined
    }
}
