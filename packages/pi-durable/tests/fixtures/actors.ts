import { DurablePiAgent } from "@durable-actors/pi-durable"
import { createModels } from "@earendil-works/pi-ai"
import { fauxProvider } from "@earendil-works/pi-ai/providers/faux"
import { createRegistry } from "@earendil-works/pi-durable"
import { Actor } from "durable-actors"

export const provider = fauxProvider({ tokensPerSecond: 0 })
const models = createModels()
models.setProvider(provider.provider)
const model = provider.getModel()
const registry = createRegistry()

export class PiAgent extends Actor {
    private agent = new DurablePiAgent(this, {
        models,
        registry,
        model: { provider: model.provider, modelId: model.id }
    })

    async prompt(text: string): Promise<string> {
        const result = await this.agent.prompt(text)
        return result.text
    }
}
