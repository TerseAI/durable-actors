import { Actor, Persisted } from "little-actors"

export class ChatHistory extends Actor {
    @Persisted private messages: ChatMessage[] = []

    async load() {
        return this.messages
    }

    async append(message: ChatMessage) {
        this.messages.push(message)
        return this.messages
    }
}

export type ChatMessage = {
    id: string
    role: "system" | "user" | "assistant"
    parts: { type: "text"; text: string }[]
}
