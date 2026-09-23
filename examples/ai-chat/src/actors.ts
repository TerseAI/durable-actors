import { Actor, Persisted } from "durable-actors"

export interface StoredMessage {
    id: string
    role: "system" | "user" | "assistant"
    parts: { type: "text"; text: string }[]
}

export class ChatHistory extends Actor {
    @Persisted private messages: StoredMessage[] = []

    async load() {
        return this.messages
    }

    async append(message: StoredMessage) {
        this.messages.push(message)
        return this.messages
    }
}
