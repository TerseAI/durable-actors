import type { ActorAlarmState, ActorAlarmStorage } from "../actor/alarm.js"
import { actorDatabaseMode } from "../actor/database.js"
import type { ActorDatabase, SqliteValue } from "../actor/database.js"
import { errorMessage } from "../errors.js"
import type { JsonObject, JsonValue } from "../json.js"

type DatabaseValue =
    | { readonly type: "null" }
    | { readonly type: "integer"; readonly value: string }
    | { readonly type: "real"; readonly value: number }
    | { readonly type: "text"; readonly value: string }
    | { readonly type: "blob"; readonly value: string }

type DatabaseCall =
    | { readonly op: "exec"; readonly sql: string; readonly bindings: DatabaseValue[]; readonly immediate: boolean }
    | { readonly op: "fields" }
    | { readonly op: "initialize_fields"; readonly fields: JsonObject }
    | { readonly op: "persist_field"; readonly name: string; readonly value: JsonValue; readonly immediate: boolean }
    | { readonly op: "delete_field"; readonly name: string; readonly immediate: boolean }
    | { readonly op: "replace_fields"; readonly fields: JsonObject }
    | { readonly op: "alarm" }
    | { readonly op: "set_alarm"; readonly timestamp_ms: number; readonly immediate: boolean }
    | { readonly op: "delete_alarm"; readonly immediate: boolean }
    | { readonly op: "commit" }
    | { readonly op: "rollback" }
    | { readonly op: "savepoint" }
    | { readonly op: "release_savepoint"; readonly immediate: boolean }
    | { readonly op: "rollback_to_savepoint" }

type DatabaseResult =
    | { readonly type: "rows"; readonly columns: string[]; readonly rows: DatabaseValue[][] }
    | { readonly type: "fields"; readonly fields: JsonObject }
    | { readonly type: "alarm"; readonly timestamp_ms: number | null }
    | { readonly type: "done" }

type DatabaseTransport = (call: DatabaseCall) => DatabaseResult

class DatabaseCommitError extends Error {}

type FieldRestore = (value: JsonValue | undefined) => void

class DatabaseProxy implements ActorDatabase, ActorAlarmStorage {
    private atomicInvocations = 0
    private transactionsUnavailable: string | undefined
    private readonly fieldRestores = new Map<string, FieldRestore>()

    constructor(private readonly transport: DatabaseTransport) {}

    async atomically<T>(operation: () => Promise<T>): Promise<T> {
        this.atomicInvocations++
        try {
            return await operation()
        } finally {
            this.atomicInvocations--
        }
    }

    exec<Row extends object>(sql: string, ...bindings: SqliteValue[]): Row[] {
        this.assertForegroundAccess()
        const result = this.transport({ op: "exec", sql, bindings: bindings.map(encode), immediate: immediate() })
        if (result.type !== "rows") throw unexpected(result)
        return result.rows.map(
            row => Object.fromEntries(row.map((value, index) => [result.columns[index], decode(value)])) as Row
        )
    }

    async transaction<T>(operation: () => Promise<T>): Promise<T> {
        if (this.transactionsUnavailable !== undefined) throw new Error(this.transactionsUnavailable)
        this.assertForegroundAccess()
        this.transport({ op: "savepoint" })
        let result: T
        try {
            result = await operation()
        } catch (error) {
            this.transport({ op: "rollback_to_savepoint" })
            this.restoreFields()
            throw error
        }
        this.transport({ op: "release_savepoint", immediate: immediate() })
        return result
    }

    rejectTransactions(reason: string): void {
        this.transactionsUnavailable = reason
    }

    onFieldRestore(name: string, restore: FieldRestore): void {
        this.fieldRestores.set(name, restore)
    }

    fields(): JsonObject {
        const result = this.transport({ op: "fields" })
        if (result.type !== "fields") throw unexpected(result)
        return result.fields
    }

    getAlarmState(): ActorAlarmState {
        const result = this.transport({ op: "alarm" })
        if (result.type !== "alarm") throw unexpected(result)
        return { timestampMs: result.timestamp_ms }
    }

    setAlarm(timestampMs: number): void {
        this.transport({ op: "set_alarm", timestamp_ms: timestampMs, immediate: immediate() })
    }

    deleteAlarm(): void {
        this.transport({ op: "delete_alarm", immediate: immediate() })
    }

    initializeFields(fields: JsonObject): void {
        this.transport({ op: "initialize_fields", fields })
    }

    persistField(name: string, value: JsonValue | undefined): void {
        this.transport(
            value === undefined
                ? { op: "delete_field", name, immediate: immediate() }
                : { op: "persist_field", name, value, immediate: immediate() }
        )
    }

    commit(): void {
        try {
            this.transport({ op: "commit" })
        } catch (cause) {
            throw new DatabaseCommitError(`failed to replicate actor SQLite commit: ${errorMessage(cause)}`, { cause })
        }
    }

    rollback(): void {
        this.transport({ op: "rollback" })
    }

    private assertForegroundAccess(): void {
        // Background SQL would join, and then commit, the open @Atomic transaction.
        if (this.atomicInvocations > 0 && actorDatabaseMode() === "background")
            throw new Error("actor database is unavailable to background work while an @Atomic method runs")
    }

    private restoreFields(): void {
        const fields = this.fields()
        for (const [name, restore] of this.fieldRestores) restore(fields[name])
    }
}

function immediate(): boolean {
    const mode = actorDatabaseMode()
    return mode === "immediate" || mode === "background"
}

function encode(value: SqliteValue): DatabaseValue {
    if (value === null) return { type: "null" }
    if (typeof value === "bigint") return { type: "integer", value: value.toString() }
    if (typeof value === "number")
        return Number.isInteger(value) ? { type: "integer", value: value.toString() } : { type: "real", value }
    if (typeof value === "string") return { type: "text", value }
    return { type: "blob", value: Buffer.from(value.buffer, value.byteOffset, value.byteLength).toString("base64") }
}

function decode(value: DatabaseValue): SqliteValue {
    switch (value.type) {
        case "null":
            return null
        case "integer": {
            const integer = BigInt(value.value)
            return Number.isSafeInteger(Number(integer)) ? Number(integer) : integer
        }
        case "real":
        case "text":
            return value.value
        case "blob":
            return new Uint8Array(Buffer.from(value.value, "base64"))
    }
}

function unexpected(result: DatabaseResult): Error {
    return new Error(`unexpected actor database result ${result.type}`)
}

export { DatabaseCommitError, DatabaseProxy }
export type { DatabaseCall, DatabaseResult, DatabaseTransport, DatabaseValue }
