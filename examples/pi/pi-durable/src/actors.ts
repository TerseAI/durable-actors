import { DurablePiAgent } from "@durable-actors/pi-durable"
import { contentText, createModels, JsonValue, Message } from "@earendil-works/pi-ai"
import { openaiProvider } from "@earendil-works/pi-ai/providers/openai"
import { createRegistry, watchEvents } from "@earendil-works/pi-durable"
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { Actor, Resource, Interleave, ActorSocket, ActorResource, ActorResourceContext, ActorDatabase } from "durable-actors"
import { ModalClient, Sandbox } from "modal";

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
    @Resource private readonly agent = new DurablePiAgent(this.db, {
        models,
        registry,
        agent: { model: { provider: "openai", modelId: "gpt-6-sol" } },
    })

    @Resource private sanbdox = new ModalSandbox(this.db)

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

const APP_NAME = "pi-durable-sandboxes"
const WORKSPACE = "/workspace"
const IMAGE = "node:22"
const PORTS = [3000]

type SandboxRow = { sandbox_id: string | null; workspace_image_id: string | null }


class ModalSandbox implements ActorResource {
    private modal: ModalClient;
    private _sandbox: Sandbox | undefined;

    get sandbox(): Sandbox {
        if (this._sandbox === undefined) {
            throw new Error("Sandbox not created")
        }
        return this._sandbox
    }

    constructor(private readonly db: ActorDatabase) {
        this.modal = new ModalClient({
            tokenId: process.env.MODAL_TOKEN_ID,
            tokenSecret: process.env.MODAL_TOKEN_SECRET,
        });

        this.db.exec("CREATE TABLE IF NOT EXISTS modal_sandbox (id INTEGER PRIMARY KEY CHECK (id = 1), sandbox_id TEXT, workspace_image_id TEXT)")
    }

    private async attach(sandboxId: string): Promise<Sandbox> {
        const sandbox = await this.modal.sandboxes.fromId(sandboxId)
        const isRunning = (await sandbox.poll()) !== null

        if (isRunning) { // great, sandbox is still running
            return sandbox
        } else {
            // sandbox is not running, we need to create a new one
            const sandbox = await this.createNewSandbox()
            return sandbox

        }
    }

    private async createNewSandbox(): Promise<Sandbox> {
        const imageId = this.db.exec<{ image_id: string }>("SELECT image_id FROM workspace_images WHERE image = ?", IMAGE)
        const sandbox = await this.createSandbox()

        this.db.exec("INSERT OR REPLACE INTO modal_sandbox (sandbox_id) VALUES (?)", sandbox.sandboxId)

        return sandbox
    }

    private async createSandbox(imageId?: string | undefined): Promise<Sandbox> {
        const app = await this.modal.apps.fromName(APP_NAME, { createIfMissing: true })
        const sandbox = await this.modal.sandboxes.create(app, this.modal.images.fromRegistry(imageId ?? IMAGE), {
            encryptedPorts: [...PORTS],
            timeoutMs: 10000,
            idleTimeoutMs: 10000,
        })

        return sandbox
    }

    async onResume(): Promise<void> {
        const [row] = this.db.exec<SandboxRow>("SELECT sandbox_id, workspace_image_id FROM modal_sandbox")

        // There is no active sandbox, likely, this is a fresh Actor instance!
        if (row.sandbox_id === null) {
           this._sandbox = await this.createNewSandbox()
           return;
        }

        this._sandbox = await this.attach(row.sandbox_id)
        return;
    }

    async onGracefulShutdown(): Promise<void> {
        const image = await this.sandbox.snapshotFilesystem({ ttlMs: null })
        this.db.exec("UPDATE modal_sandbox SET workspace_image_id = ?", image.imageId)

        return;
    }
}