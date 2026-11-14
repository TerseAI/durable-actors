import type { ActorSessionPort } from "../../src/host/actor-executor.js"
import type { DatabaseChannel } from "../../src/host/database-channel.js"

import type { HostDatabase } from "./host-database.js"

export function sessionPort(overrides: Partial<ActorSessionPort> = {}): ActorSessionPort {
    return {
        allowNextInvocation() {},
        publish() {},
        async connections() {
            return []
        },
        async callActor() {
            throw new Error("unexpected actor call")
        },
        log() {},
        ...overrides
    }
}

export function databaseChannel(database: HostDatabase, messageIds: (number | undefined)[] = []): DatabaseChannel {
    return {
        call(call, messageId) {
            messageIds.push(messageId)
            return database.call(call)
        }
    }
}
