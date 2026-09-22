import { Actor, Persisted } from "durable-actors"

export class Counter extends Actor {
    @Persisted private value = 0

    async read() {
        return this.value
    }

    async increment() {
        return ++this.value
    }
}
