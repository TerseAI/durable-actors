import assert from "node:assert/strict"
import { test } from "node:test"

import { Actor, registerActorClass } from "../../src/actor/actor.js"
import { Persistence } from "../../src/actor/schema.js"
import type { SocketEffect } from "../../src/actor/socketProtocol.js"
import { ActorRuntime } from "../../src/host/actor-runtime.js"
import { seed } from "../fixtures/litestream.js"

test("commit-bound socket output stays in the successful reply and is discarded on failure", async () => {
    const published: SocketEffect[] = []
    class Receipts extends Actor {
        count = 0

        async record(fail: boolean): Promise<number> {
            this.count++
            const socket = (await this.getConnections())[0]!
            socket.sendAfterCommit({ id: this.count })
            this.broadcastAfterCommit({ count: this.count }, { tags: ["watcher"] })
            this.broadcast("progress")
            assert.equal(published.length, 1)
            if (fail) throw new Error("rejected")
            return this.count
        }
    }
    const runtime = new ActorRuntime(
        registerActorClass(Receipts, {
            actorName: "Receipts",
            fields: [{ name: "count", persistence: Persistence.Persisted }]
        }),
        () => {},
        async effects => {
            published.push(...effects)
        },
        async () => [{ id: "socket", metadata: null, tags: [] }]
    )
    const actor = { project_id: "test", actor_name: "Receipts", actor_id: "one" }
    const command = {
        type: "invoke" as const,
        request_id: "one",
        actor,
        method: "record",
        args: [false],
        sqlite: await seed()
    }
    try {
        const committed = await runtime.handle(command)
        assert.equal(committed.type, "invoked")
        if (committed.type !== "invoked") throw new Error("invocation failed")
        assert.equal(committed.result, 1)
        assert.deepEqual(
            committed.effects?.map(effect => effect.type),
            ["send", "broadcast"]
        )
        published.length = 0
        assert.equal((await runtime.handle({ ...command, args: [true] })).type, "failed")
        assert.deepEqual(
            published.map(effect => effect.type === "broadcast" && effect.message.data),
            ['"progress"']
        )
        published.length = 0
        const recovered = await runtime.handle(command)
        assert.equal(recovered.type, "invoked")
        if (recovered.type === "invoked") assert.equal(recovered.result, 2)
    } finally {
        runtime.close()
    }
})
