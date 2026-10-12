import type { Context } from "@earendil-works/chord"
import { BACKGROUND_CONTEXT as context } from "@earendil-works/chord/context"
import { AssistantEntry, Harness } from "@earendil-works/pi-durable"
import type { Conversation, EntryId, Storage } from "@earendil-works/pi-durable"

import type { DurablePiAgentOptions } from "./index.js"

export interface PiSessionScope {
    readonly harness: Harness
    readonly conversation: Conversation
    readonly context: Context
}

export class PiSession {
    constructor(
        private readonly openStorage: () => Storage,
        private readonly options: DurablePiAgentOptions
    ) {}

    async prompt(text: string): Promise<string> {
        return this.run(async ({ conversation }) => {
            const answerId = await this.submit(conversation, text)
            return this.readAnswer(conversation, answerId)
        })
    }

    async run<T>(operation: (session: PiSessionScope) => T | Promise<T>): Promise<T> {
        const harness = await Harness.open(this.openStorage(), this.options, context)
        try {
            const conversation = await harness.root(context, { agent: { model: this.options.model } })
            return await operation({ harness, conversation, context })
        } finally {
            await harness.close(context)
        }
    }

    private async submit(conversation: Conversation, text: string): Promise<EntryId> {
        const submission = await conversation.submit({ type: "input", content: text }, context)
        const result = await submission.wait(context)
        if (result.status === "unanswered") {
            const detail = typeof result.detail === "string" ? `: ${result.detail}` : ""
            throw new Error(`Pi did not answer: ${result.reason}${detail}`)
        }
        if (result.type !== "input") throw new Error("Pi returned an unexpected submission type")
        return result.answer
    }

    private async readAnswer(conversation: Conversation, answerId: EntryId): Promise<string> {
        const entry = await conversation.commit(tx => tx.entry(AssistantEntry, answerId), context)
        const answer = entry?.model?.find(message => message.role === "assistant")
        if (!answer) throw new Error("Pi's answer is missing from the transcript")
        return answer.content
            .filter(part => part.type === "text")
            .map(part => part.text)
            .join("\n")
    }
}
