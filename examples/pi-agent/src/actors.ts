import type { AgentMessage } from "@earendil-works/pi-agent-core"
import { createModels } from "@earendil-works/pi-ai"
import { openaiProvider } from "@earendil-works/pi-ai/providers/openai"
import { Actor, Persisted } from "durable-actors"

import { PiChat } from "./chat.js"

export class PiAgent extends Actor {
    @Persisted private messages: AgentMessage[] = []

    async prompt(text: string): Promise<string> {
        if (!process.env.OPENAI_API_KEY) throw new Error("Set OPENAI_API_KEY in the pi-agent project's .env")
        const models = createModels()
        models.setProvider(openaiProvider())
        const result = await new PiChat(models).prompt(text, this.messages)
        this.messages = result.messages
        return result.text
    }
}
