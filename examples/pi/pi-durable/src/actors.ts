import { DurablePiAgent } from "@durable-actors/pi-durable"
import { createModels } from "@earendil-works/pi-ai"
import { openaiProvider } from "@earendil-works/pi-ai/providers/openai"
import { createRegistry } from "@earendil-works/pi-durable"
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { Actor, Ephemeral } from "durable-actors"

const models = createModels()
models.setProvider(openaiProvider())
const registry = createRegistry()
const context = BACKGROUND_CONTEXT

export class PiAgent extends Actor {
    @Ephemeral private readonly agent = DurablePiAgent.create(this, {
        models,
        registry,
    })

    async prompt(text: string): Promise<string> {
        const conversation = await (await this.agent).root(context, { agent: { model: { provider: "openai", modelId: "gpt-5-mini" } } })

        const submission = await conversation.submit({ type: "input", content: text }, context)
        const result = await submission.wait(context)
    }
}
