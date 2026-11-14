import { readSync, writeSync } from "node:fs"

import { ActorProtocolError, ActorSessionError } from "../errors.js"

import type { DatabaseCall, DatabaseResult } from "./database.js"
import type { DatabaseCallMessage } from "./protocol.js"
import { parseDatabaseReply } from "./protocol.js"

/** Sends a database call to the Rust host and blocks until it replies. */
interface DatabaseChannel {
    call(call: DatabaseCall, messageId: number | undefined): DatabaseResult
}

/** Speaks newline-delimited JSON over a blocking socket the Rust host passed to this process. */
class FileDescriptorDatabaseChannel implements DatabaseChannel {
    private nextCallId = 0
    private readonly chunk = Buffer.allocUnsafe(64 * 1024)

    constructor(private readonly fd: number) {}

    call(call: DatabaseCall, messageId: number | undefined): DatabaseResult {
        const callId = ++this.nextCallId
        this.write({ type: "database_call", call_id: callId, message_id: messageId, call })
        const reply = parseDatabaseReply(this.readLine())
        if (reply.call_id !== callId) throw new ActorProtocolError("Rust host replied to another database call")
        if (reply.result === undefined) throw new Error(reply.error)
        return reply.result
    }

    private write(message: DatabaseCallMessage): void {
        const bytes = Buffer.from(`${JSON.stringify(message)}\n`)
        for (let offset = 0; offset < bytes.length;) offset += writeSync(this.fd, bytes, offset)
    }

    // Calls are strictly request/reply, so a reply always ends at the last byte read.
    private readLine(): string {
        const chunks: Buffer[] = []
        for (;;) {
            const read = readSync(this.fd, this.chunk)
            if (read === 0) throw new ActorSessionError("Rust host closed the database channel")
            chunks.push(Buffer.from(this.chunk.subarray(0, read)))
            if (this.chunk[read - 1] !== NEWLINE) continue
            const line = Buffer.concat(chunks)
            return line.toString("utf8", 0, line.length - 1)
        }
    }
}

const NEWLINE = 0x0a

export { FileDescriptorDatabaseChannel }
export type { DatabaseChannel }
