import type { SqliteValue } from "../actor/database.js"
import { ActorProtocolError, errorMessage } from "../errors.js"
import type { DatabaseCall, DatabaseResult } from "../host/database.js"

import type { BlockingStream } from "./blocking-stream.js"
import { StreamReader } from "./stream-reader.js"
import { StreamWriter } from "./stream-writer.js"

const NULL = 0
const INTEGER = 1
const REAL = 2
const TEXT = 3
const BLOB = 4

const COLUMNS = 0x10
const ROW = 0x11
const RESULT = 0x12
const FAILED = 0x13

// SQLite's own limit on the values one statement binds. The Rust host ends the channel beyond it.
const MAX_BINDINGS = 32_766
const MAX_SIZE = 0xffff_ffff

/** The channel can no longer be used: an exchange stopped part way, so the next would be misread. */
class DatabaseChannelError extends ActorProtocolError {}

type Answer = DatabaseResult | { readonly type: "failed"; readonly message: string }

/**
 * The actor's database in the Rust host. A call holds the thread until it is answered, which is what lets
 * `this.db` be synchronous. Bound values and rows cross as bytes and are never held as JSON.
 */
class DatabaseChannel {
    private readonly reader: StreamReader
    private readonly writer: StreamWriter
    private failure: DatabaseChannelError | undefined

    constructor(stream: BlockingStream) {
        this.reader = new StreamReader(stream)
        this.writer = new StreamWriter(stream)
    }

    /** `invocation` names the command whose database the call reaches. Without it, the activation's. */
    call(call: DatabaseCall, invocation?: number): DatabaseResult {
        const answer = this.exchange(call, invocation)
        if (answer.type === "failed") throw new Error(answer.message)
        return answer
    }

    private exchange(call: DatabaseCall, invocation: number | undefined): Answer {
        if (this.failure !== undefined) throw this.failure
        const bindings = bindable(call)
        try {
            this.request(call, invocation, bindings)
            return this.answer()
        } catch (error) {
            this.failure = new DatabaseChannelError(`actor database channel failed: ${errorMessage(error)}`, {
                cause: error
            })
            throw this.failure
        } finally {
            this.reader.release()
        }
    }

    private request(call: DatabaseCall, invocation: number | undefined, bindings: readonly SqliteValue[]): void {
        this.writer.text(
            JSON.stringify({ message_id: invocation, call: withoutBindings(call), bindings: bindings.length })
        )
        for (const value of bindings) this.value(value)
        this.writer.flush()
    }

    private value(value: SqliteValue): void {
        if (value === null) this.writer.u8(NULL)
        else if (typeof value === "string") {
            this.writer.u8(TEXT)
            this.writer.text(value)
        } else if (typeof value === "object") {
            this.writer.u8(BLOB)
            this.writer.bytes(value)
        } else if (typeof value === "bigint" || fitsInteger(value)) {
            this.writer.u8(INTEGER)
            this.writer.i64(BigInt(value))
        } else {
            this.writer.u8(REAL)
            this.writer.f64(value)
        }
    }

    // Rows written before a statement fails are dropped with it.
    private answer(): Answer {
        let columns: string[] = []
        const rows: SqliteValue[][] = []
        for (;;) {
            const record = this.reader.u8()
            switch (record) {
                case COLUMNS:
                    columns = Array.from({ length: this.reader.u32() }, () => this.reader.text())
                    break
                case ROW:
                    rows.push(Array.from(columns, () => this.cell()))
                    break
                case RESULT:
                    return result(this.reader.text(), columns, rows)
                case FAILED:
                    return { type: "failed", message: this.reader.text() }
                default:
                    throw new Error(`unknown record ${record}`)
            }
        }
    }

    private cell(): SqliteValue {
        const type = this.reader.u8()
        switch (type) {
            case NULL:
                return null
            case INTEGER:
                return this.reader.integer()
            case REAL:
                return this.reader.f64()
            case TEXT:
                return this.reader.text()
            case BLOB:
                return this.reader.bytes()
            default:
                throw new Error(`unknown value type ${type}`)
        }
    }
}

// What cannot be sent is refused before anything is written, so a refusal leaves the channel usable.
function bindable(call: DatabaseCall): readonly SqliteValue[] {
    if (call.op !== "exec") return []
    if (call.bindings.length > MAX_BINDINGS)
        throw new RangeError(`a statement binds at most ${MAX_BINDINGS} values, not ${call.bindings.length}`)
    call.bindings.forEach(assertBindable)
    return call.bindings
}

function assertBindable(value: unknown, index: number): void {
    if (value === null || typeof value === "string" || typeof value === "number") return
    if (typeof value === "bigint") {
        if (BigInt.asIntN(64, value) !== value)
            throw new RangeError(`binding ${index + 1} does not fit a 64-bit SQLite integer`)
    } else if (value instanceof Uint8Array) {
        if (value.byteLength > MAX_SIZE) throw new RangeError(`binding ${index + 1} is larger than 4 GiB`)
    } else
        throw new TypeError(
            `binding ${index + 1} is ${value === undefined ? "undefined" : typeof value}; SQLite binds null, numbers, bigints, strings and Uint8Arrays`
        )
}

function withoutBindings(call: DatabaseCall): object {
    if (call.op !== "exec") return call
    const { bindings: _, ...statement } = call
    return statement
}

// A whole number is bound as a SQLite integer when 64 bits hold it, and as a real otherwise.
function fitsInteger(value: number): boolean {
    return Number.isInteger(value) && value >= -(2 ** 63) && value < 2 ** 63
}

function result(document: string, columns: string[], rows: SqliteValue[][]): DatabaseResult {
    const parsed = JSON.parse(document) as DatabaseResult
    switch (parsed.type) {
        case "rows":
            return { type: "rows", columns, rows }
        case "fields":
        case "alarm":
        case "done":
            return parsed
        default:
            throw new Error(`unknown result ${document.slice(0, 80)}`)
    }
}

export { DatabaseChannel, DatabaseChannelError }
