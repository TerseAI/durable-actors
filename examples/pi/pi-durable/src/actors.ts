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

class ModalSandbox implements ActorResource {
    private modal: ModalClient;
    private _sandbox: Sandbox | undefined;
    private resourceKV: ResourceKV

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

        this.resourceKV = new ResourceKV(this.db, "modal_sandbox")
    }

    private async createNewSandbox(): Promise<Sandbox> {
        const imageId = this.resourceKV.get("image_id")

        // If there is no workspace image, no problem!
        let sandbox: Sandbox
        if (imageId === undefined) {
            sandbox = await this.createSandbox(IMAGE)
        } else {
            sandbox = await this.createSandbox(imageId)
        }

        this.resourceKV.set("sandbox_id", sandbox.sandboxId)

        return sandbox
    }

    private async createSandbox(imageId?: string | undefined): Promise<Sandbox> {
        const app = await this.modal.apps.fromName(APP_NAME, { createIfMissing: true })
        const sandbox = await this.modal.sandboxes.create(app, this.modal.images.fromRegistry(imageId ?? IMAGE), {
            encryptedPorts: [...PORTS],
            timeoutMs: 10000,
            idleTimeoutMs: 10000,
        })

        console.log("created sandbox \n", sandbox.sandboxId)
        if (imageId === undefined) {
            console.log("no image id, using default image", IMAGE)
        } else {
            console.log("using image id", imageId)
        }

        return sandbox
    }

    async onResume(): Promise<void> {
        console.log("Resume hook fired")
        const sandboxId = this.resourceKV.get("sandbox_id")

        // No sandbox session associated with this actor
        if (sandboxId === undefined) {
            this._sandbox = await this.createNewSandbox()
            this.resourceKV.set("sandbox_id", this._sandbox.sandboxId)
            return;
        }

        const sandbox = await this.modal.sandboxes.fromId(sandboxId)
        const isRunning = (await sandbox.poll()) !== null

        if (!isRunning) {
            this._sandbox = await this.createNewSandbox()
        } else {
            this._sandbox = sandbox
        }

        console.log("Attached sandbox", this._sandbox.sandboxId)
        return;
    }

    async onGracefulShutdown(): Promise<void> {
        console.log("Graceful shutdown hook fired")
        const image = await this.sandbox.snapshotFilesystem({ ttlMs: null })

        console.log("Snapshot filesystem", image.imageId)
        this.resourceKV.set("image_id", image.imageId)

        console.log("Updated database with image id", image.imageId)

        return;
    }
}

class ResourceKV {
    constructor(private readonly db: ActorDatabase, private readonly namespace: string) {
        db.exec("CREATE TABLE IF NOT EXISTS resource_kv (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
    }
    get(key: string): string | undefined {
        return this.db.exec<{ value: string }>(
            "SELECT value FROM resource_kv WHERE key = ?", `${this.namespace}:${key}`
        )[0]?.value
    }
    set(key: string, value: string): void {
        this.db.exec(
            "INSERT INTO resource_kv (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            `${this.namespace}:${key}`, value
        )
    }
}