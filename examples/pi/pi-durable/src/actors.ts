import { DurablePiAgent } from "@durable-actors/pi-durable"
import { createModels } from "@earendil-works/pi-ai"
import { openaiProvider } from "@earendil-works/pi-ai/providers/openai"
import { createRegistry } from "@earendil-works/pi-durable"
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { Actor, Resource } from "durable-actors"

const models = createModels()
models.setProvider(openaiProvider())
const registry = createRegistry()
const context = BACKGROUND_CONTEXT

export class PiAgent extends Actor {
    @Resource private readonly agent = new DurablePiAgent({
        models,
        registry,
    })

    async prompt(text: string): Promise<string> {
        const response = await this.agent.prompt(text)
        return response
    }
}
