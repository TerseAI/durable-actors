import { DurablePiAgent } from "@durable-actors/pi-durable"
import { contentText, createModels, Message, Type } from "@earendil-works/pi-ai"
import { openaiProvider } from "@earendil-works/pi-ai/providers/openai"
import { ConversationView, ConversationWatch, createRegistry, TaskId, watchEvents } from "@earendil-works/pi-durable"
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { Actor, Resource, Interleave, ActorSocket, Ephemeral, Unchecked, unchecked } from "durable-actors"
import { CodingTools } from "@earendil-works/pi-durable/tools"
import { ModalSandbox } from "./modal/SandboxResource.js";
import { Op } from "@earendil-works/chord/delta";

export const WORKSPACE = "/workspace"

export class PiAgent extends Actor<null, string, Outgoing> {
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

    @Ephemeral private readonly watches = new Map<string, ConversationWatch>()

    @Interleave
    async prompt(text: string): Promise<string> {
        const watch = await this.agent.root.watch(context)
        watch.start(async (_view, ops) => this.broadcast({ type: "ops", ops: unchecked(ops) }))
        
        try {
            return await this.agent.prompt(text)
        } finally {
            await watch.stop()
            const latest = await this.agent.root.watch(context)
            this.broadcast({ type: "view", view: unchecked(latest.value) })
            await latest.stop()
        }
    }

    async reset() {
        await this.agent.root.reset(undefined, context)
    }

    async ringGoesInMountDoom() {
        process.exit(1)
    }

    async onConnect(socket: ActorSocket<null, Outgoing, string>) {
        const watch = await this.agent.root.watch(context)
        socket.send({ type: "view", view: unchecked(watch.value) })
        watch.start(async (_view, ops) => socket.send({ type: "ops", ops: unchecked(ops) }))
        this.watches.set(socket.id, watch)
        socket.send({ status: "ready", url: await this.sandbox.previewUrl() })
    }
}

// MARK: Types
type Outgoing =
| { type: "view"; view: Unchecked<ConversationView> }
| { type: "ops"; ops: Unchecked<readonly Op[]> }
| Preview

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

// MARK: Helpers
function taskId(taskId: TaskId | undefined): string {
    return taskId?.toString() ?? ""
}