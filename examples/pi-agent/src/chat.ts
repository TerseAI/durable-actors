import { Agent, type AgentMessage } from "@earendil-works/pi-agent-core"
import type { Models } from "@earendil-works/pi-ai"

export class PiChat {
    constructor(private readonly models: Pick<Models, "getModel" | "streamSimple">) {}

    async prompt(text: string, messages: AgentMessage[]): Promise<{ text: string; messages: AgentMessage[] }> {
        if (!text.trim()) throw new Error("Enter a prompt")
        const model = this.models.getModel("openai", "gpt-5-mini")
        if (!model) throw new Error("Model gpt-5-mini is unavailable")
        const agent = new Agent({
            initialState: { model, systemPrompt: "You are a concise, helpful assistant.", messages: structuredClone(messages) },
            streamFn: this.models.streamSimple.bind(this.models)
        })
        await agent.prompt(text)
        if (agent.state.errorMessage) throw new Error(agent.state.errorMessage)
        const reply = agent.state.messages.at(-1)
        if (reply?.role !== "assistant") throw new Error("Pi did not return an assistant response")
        return {
            text: reply.content.flatMap(block => (block.type === "text" ? [block.text] : [])).join(""),
            messages: agent.state.messages
        }
    }
}
