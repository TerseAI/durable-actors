import type { Duplex } from "node:stream"

import type { SqliteValue } from "../../src/actor/database.js"
import type { DatabaseCall, DatabaseResult } from "../../src/host/database.js"
import type { BlockingStream } from "../../src/payload/blocking-stream.js"

type Database = (call: DatabaseCall, invocation?: number) => DatabaseResult

interface Request {
    readonly invocation: number | undefined
    readonly call: DatabaseCall
}

const COLUMNS = 0x10
const ROW = 0x11
const RESULT = 0x12
const FAILED = 0x13

/** The Rust host's end of a database channel, answering on the thread that asks. */
export class DatabasePeer implements BlockingStream {
    readonly requests: Request[] = []
    private asked: Buffer = Buffer.alloc(0)
    private answer: Buffer = Buffer.alloc(0)

    /** `readBytes` is the most one read returns, so that values arrive split across reads. */
    constructor(
        private readonly database: Database,
        private readonly readBytes = 1021
    ) {}

    write(bytes: Uint8Array): void {
        this.asked = Buffer.concat([this.asked, bytes])
    }

    read(into: Uint8Array, offset: number): number {
        if (this.answer.length === 0) this.answer = this.respond()
        const taken = this.answer.subarray(0, Math.min(this.readBytes, into.byteLength - offset))
        into.set(taken, offset)
        this.answer = this.answer.subarray(taken.length)
        return taken.length
    }

    // An incomplete request is never answered, which the asker sees as the end of the stream.
    private respond(): Buffer {
        const taken = takeRequest(this.asked)
        if (taken === undefined) return Buffer.alloc(0)
        this.asked = this.asked.subarray(taken.length)
        this.requests.push(taken.request)
        return answerTo(taken.request, this.database)
    }
}

/** Answers the database calls a child process makes on `stream`. */
export function serveDatabase(stream: Duplex, database: Database): void {
    let asked: Buffer = Buffer.alloc(0)
    stream.on("data", (chunk: Buffer) => {
        asked = Buffer.concat([asked, chunk])
        for (let taken = takeRequest(asked); taken !== undefined; taken = takeRequest(asked)) {
            asked = asked.subarray(taken.length)
            stream.write(answerTo(taken.request, database))
        }
    })
}

export function answerTo({ call, invocation }: Request, database: Database): Buffer {
    try {
        const result = database(call, invocation)
        if (result.type !== "rows") return ending(RESULT, JSON.stringify(result))
        return Buffer.concat([
            Buffer.from([COLUMNS]),
            u32(result.columns.length),
            ...result.columns.map(sized),
            ...result.rows.map(row => Buffer.concat([Buffer.from([ROW]), ...row.map(value)])),
            ending(RESULT, JSON.stringify({ type: "rows" }))
        ])
    } catch (error) {
        return ending(FAILED, (error as Error).message)
    }
}

export function ending(record: number, body: string): Buffer {
    return Buffer.concat([Buffer.from([record]), sized(body)])
}

export function value(value: SqliteValue): Buffer {
    if (value === null) return Buffer.from([0])
    if (typeof value === "string") return Buffer.concat([Buffer.from([3]), sized(value)])
    if (typeof value === "object") return Buffer.concat([Buffer.from([4]), sized(value)])
    const bytes = Buffer.alloc(9)
    if (typeof value === "bigint" || Number.isSafeInteger(value)) {
        bytes[0] = 1
        bytes.writeBigInt64LE(BigInt(value), 1)
    } else {
        bytes[0] = 2
        bytes.writeDoubleLE(value, 1)
    }
    return bytes
}

export function sized(body: string | Uint8Array): Buffer {
    const bytes = Buffer.from(body)
    return Buffer.concat([u32(bytes.length), bytes])
}

function u32(value: number): Buffer {
    const bytes = Buffer.alloc(4)
    bytes.writeUInt32LE(value)
    return bytes
}

function takeRequest(bytes: Buffer): { request: Request; length: number } | undefined {
    const cursor = new Cursor(bytes)
    try {
        const head = JSON.parse(cursor.sized().toString()) as {
            message_id?: number
            call: DatabaseCall
            bindings: number
        }
        const bindings = Array.from({ length: head.bindings }, () => cursor.value())
        const call = head.call.op === "exec" ? { ...head.call, bindings } : head.call
        return { request: { invocation: head.message_id, call }, length: cursor.at }
    } catch (error) {
        if (error instanceof RangeError) return undefined
        throw error
    }
}

// Reading past the end throws a RangeError, which means the rest of the request has not arrived.
class Cursor {
    at = 0

    constructor(private readonly bytes: Buffer) {}

    value(): SqliteValue {
        switch (this.bytes.readUInt8(this.at++)) {
            case 0:
                return null
            case 1:
                return this.bytes.readBigInt64LE(this.advance(8))
            case 2:
                return this.bytes.readDoubleLE(this.advance(8))
            case 3:
                return this.sized().toString()
            default:
                return new Uint8Array(this.sized())
        }
    }

    sized(): Buffer {
        const size = this.bytes.readUInt32LE(this.advance(4))
        const start = this.advance(size)
        if (this.at > this.bytes.length) throw new RangeError("incomplete")
        return this.bytes.subarray(start, this.at)
    }

    private advance(bytes: number): number {
        const start = this.at
        this.at += bytes
        return start
    }
}
