import { Actor, Emittable, Ephemeral, Persisted } from "durable-actors"

export class BankAccount extends Actor {
    @Persisted @Emittable balance = 0
    @Ephemeral private depositsSinceWakeup = 0

    async getBalance(): Promise<number> {
        return this.balance
    }

    async deposit(amount: number): Promise<number> {
        this.balance += amount
        this.depositsSinceWakeup++
        return this.balance
    }
}
