import { createModels } from "@earendil-works/pi-ai"
import { openaiProvider } from "@earendil-works/pi-ai/providers/openai"
import { Actor } from "durable-actors"

import { PiChat } from "./chat.js"

export class PiAgent extends Actor {
    async prompt(text: string): Promise<string> {
        if (!process.env.OPENAI_API_KEY) throw new Error("Set OPENAI_API_KEY in the pi-agent project's .env")
        const models = createModels()
        models.setProvider(openaiProvider())
        return new PiChat(models).prompt(text)
    }
}
