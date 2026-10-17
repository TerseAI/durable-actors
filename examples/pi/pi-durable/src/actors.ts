import { DurablePiAgent } from "@durable-actors/pi-durable"
import { contentText, createModels, Message, Type } from "@earendil-works/pi-ai"
import { openaiProvider } from "@earendil-works/pi-ai/providers/openai"
import { createRegistry, watchEvents } from "@earendil-works/pi-durable"
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { Actor, Resource, Interleave, ActorSocket, ActorResource, ActorDatabase } from "durable-actors"
import { CodingTools } from "@earendil-works/pi-durable/tools"
import { ModalSandbox } from "./modal/SandboxResource.js";

export const WORKSPACE = "/workspace"

export class PiAgent extends Actor<null, string, ChatEvent | Preview> {
    @Resource private sandbox = new ModalSandbox(this.db)

    @Resource private readonly agent = new DurablePiAgent(this.db, {
        models,
        registry,
        env: () => this.sandbox.env(),
        agent: { 
            model: { provider: "openai", modelId: "gpt-6-sol" },
            cwd: WORKSPACE,
            instructions: instructions,
        },
    })

    @Interleave
    async prompt(text: string): Promise<string> {
        const stream = await watchEvents(this.agent.harness,this.agent.root.id, context)
        this.broadcast({ type: "start", prompt: text })

        await stream.start(async (events) => {
            for (const event of events) {
                switch (event.type) {
                    case "message_update":
                        for (const change of event.changes) {
                            if (change.type === "text_delta")  {
                                this.broadcast({ type: "delta", text: change.delta });
                            }
                        }
                        break;
                }
            }
        })

        const response = await this.agent.prompt(text)
        this.broadcast({ type: "done" });

        return response
    }

    async reset() {
        await this.agent.root.reset(undefined, context)
        const { messages } = await this.agent.root.context(context)
        const messages2 = Array.from(messages);
        this.broadcast({ type: "history", messages: chatMessages(messages2) });
    }

    async ringGoesInMountDoom() {
        process.exit(1)
    }

    async onConnect(socket: ActorSocket<null, ChatEvent|Preview, string>) {
        const { messages } = await this.agent.root.context(context)
        const messages2 = Array.from(messages);
        socket.send({ type: "history", messages: chatMessages(messages2) });
        socket.send({ status: "ready", url: await this.sandbox.previewUrl() });
    }
}

function chatMessages(messages: readonly Message[]): ChatMessage[] {
    return messages.flatMap((message) =>
        message.role === "user" || message.role === "assistant"
            ? [{ role: message.role, content: contentText(message.content) }]
            : []
    )
}

// MARK: Types
export type ChatEvent =
  | { type: "history"; messages: ChatMessage[] }
  | { type: "start"; prompt: string }
  | { type: "delta"; text: string }
  | { type: "done" };

export type ChatMessage =
  | { role: "user"; content: string }
  | { role: "assistant"; content: string };

export type Preview =
| { status: "starting" }
| { status: "ready"; url: string }
| { status: "stopped" }
| { status: "error"; message: string };

const instructions = `You are editing a Vite app in ${WORKSPACE}. Its dev server is already running and the user sees it live; edits hot-reload.
Add packages with npm install. Do not change vite.config.js or the dev script.`

const models = createModels()
models.setProvider(openaiProvider())
const registry = createRegistry()
registry.install(CodingTools)
const context = BACKGROUND_CONTEXT