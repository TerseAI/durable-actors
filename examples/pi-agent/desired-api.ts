// API target: the adapter package and actor lifecycle integration are not implemented yet.
import { DurablePiAgent } from "@durable-actors/pi-durable"
import { createModels } from "@earendil-works/pi-ai"
import { openaiProvider } from "@earendil-works/pi-ai/providers/openai"
import { createRegistry } from "@earendil-works/pi-durable"
import { Actor } from "durable-actors"

const models = createModels()
models.setProvider(openaiProvider())
const registry = createRegistry()

export class PiAgent extends Actor {
    private agent = new DurablePiAgent(this, {
        models,
        registry,
        model: { provider: "openai", modelId: "gpt-5-mini" }
    })

    async prompt(text: string): Promise<string> {
        const result = await this.agent.prompt(text)
        return result.text
    }
}
