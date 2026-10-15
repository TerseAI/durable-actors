import { DurablePiAgent } from "@durable-actors/pi-durable"
import { contentText, createModels, JsonValue, Message } from "@earendil-works/pi-ai"
import { openaiProvider } from "@earendil-works/pi-ai/providers/openai"
import { createRegistry, watchEvents } from "@earendil-works/pi-durable"
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { Actor, Resource, Interleave, ActorSocket } from "durable-actors"

const models = createModels()
models.setProvider(openaiProvider())
const registry = createRegistry()
const context = BACKGROUND_CONTEXT

export type ChatEvent =
  | { type: "history"; messages: ChatMessage[] }
  | { type: "start"; prompt: string }
  | { type: "delta"; text: string }
  | { type: "done" };

export type ChatMessage =
  | { role: "user"; content: string }
  | { role: "assistant"; content: string };

export class PiAgent extends Actor<null, string, ChatEvent> {
    @Resource private readonly agent = new DurablePiAgent({
        models,
        registry,
        agent: { model: { provider: "openai", modelId: "gpt-6-sol" } },
    })

    @Interleave
    async prompt(text: string): Promise<string> {
        const response = await this.agent.prompt(text)

        const stream = await watchEvents(this.agent.harness,this.agent.root.id, context)

        stream.start(async (events) => {
            for (const event in events) {
                switch (event) {
                    case "delta":
                        this.broadcast({ type: "delta", text: event });
                        break;
                    case "done":
                        this.broadcast({ type: "done" });
                }
            }
        })
        return response
    }

    async reset() {
        this.agent.root.reset(undefined, context)
    }

    async ringGoesInMountDoom() {
        process.exit(1)
    }

    async onConnect(socket: ActorSocket<null, ChatEvent, string>) {
        const { messages } = await this.agent.root.context(context)
        const messages2 = Array.from(messages);
        socket.send({ type: "history", messages: chatMessages(messages2) });
    }
}

function chatMessages(messages: readonly Message[]): ChatMessage[] {
    return messages.flatMap((message) =>
        message.role === "user" || message.role === "assistant"
            ? [{ role: message.role, content: contentText(message.content) }]
            : []
    )
}