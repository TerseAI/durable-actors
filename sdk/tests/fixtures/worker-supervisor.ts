import type { ActorExecutorCommand, ActorExecutorReply } from "../../src/host/protocol.js"
import type { SqliteState } from "../../src/host/sqlite.js"
import type { ActorWorker, ActorWorkerSupervisor } from "../../src/host/worker-supervisor.js"

import { commit } from "./litestream.js"

type Arguments = Parameters<ActorWorkerSupervisor["handle"]>
const sqliteState = Symbol("fixture SQLite state")
type SqliteHost = { [sqliteState]?: SqliteState }

export function handle(
    host: ActorWorkerSupervisor,
    command: Arguments[0],
    allowNext: Arguments[1],
    publish?: Arguments[4],
    connections?: Arguments[5]
): Promise<ActorExecutorReply> {
    return host.handle(command, allowNext, capture(host as SqliteHost, command), persist, publish, connections)
}

export function execute(
    worker: ActorWorker,
    command: Parameters<ActorWorker["execute"]>[0],
    allowNext: Arguments[1],
    publish?: Arguments[4],
    connections?: Arguments[5]
): Promise<ActorExecutorReply> {
    return worker.execute(command, allowNext, capture(worker as SqliteHost, command), persist, publish, connections)
}

function capture(host: SqliteHost, command: ActorExecutorCommand): () => Promise<number> {
    if ("sqlite" in command && command.sqlite) host[sqliteState] ??= command.sqlite
    if (command.type === "evict") delete host[sqliteState]
    const state = host[sqliteState]
    return () => {
        if (!state) throw new Error("host has no registered SQLite database")
        return commit(state)
    }
}

async function persist(_commitId: number, txid: number): Promise<{ txid: number }> {
    return { txid }
}
