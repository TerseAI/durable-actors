import { AsyncLocalStorage } from "node:async_hooks"
import { createRequire } from "node:module"

import type { Alarm, AlarmStorage } from "../actor/alarm.js"
import type { ActorDatabase, SqliteResult, SqliteValue } from "../actor/database.js"
import type { JsonObject, JsonValue } from "../json.js"

import { syncLitestream } from "./litestream.js"
import type { LitestreamDatabase } from "./litestream.js"

interface SqliteState {
    readonly alarm?: Alarm
    readonly txid: number
    readonly path?: string
    readonly socket?: string
}

class SqliteCaptureError extends Error {}

interface ActorDatabaseStorage extends ActorDatabase, AlarmStorage {
    fields(): JsonObject
    persistFields(fields: JsonObject): void
    restore(state: SqliteState | undefined): void
    snapshot(): Promise<SqliteState>
    rollback(): void
    close(): void
}

class SqliteActorDatabase implements ActorDatabaseStorage {
    private connection: SqliteConnection | undefined
    private seed: LitestreamDatabase | undefined
    private readonly transaction = new AsyncLocalStorage<{ active: boolean }>()
    private savepoint = 0
    private txid = 0
    private version = ""
    private failure: SqliteCaptureError | undefined

    constructor(
        private readonly connect: (path: string) => SqliteConnection = openSqlite,
        private readonly sync: (database: LitestreamDatabase) => Promise<number> = syncLitestream
    ) {}

    exec<Row extends object>(sql: string, ...bindings: SqliteValue[]): Row[] {
        validateStatement(sql)
        const database = this.open()
        validateSingleStatement(database, sql)
        const statement = database.prepare(sql)
        if (!database.isTransaction) database.exec("BEGIN")
        return statement.all(...bindings) as Row[]
    }

    execute<Row extends object>(sql: string, ...bindings: SqliteValue[]): SqliteResult<Row> {
        return this.transactionSync(() => {
            const database = this.open()
            let rest = sql
            for (;;) {
                if (!withoutComments(rest)) throw new Error("SQL code did not contain a statement")
                validateStatement(rest)
                const [source, tail] = firstStatement(database, rest)
                const statement = database.prepare(source)
                if (withoutComments(tail)) {
                    if (hasBindings(source)) throw new Error("only the final SQL statement accepts bindings")
                    statement.all()
                    rest = tail
                    continue
                }
                const before = totalChanges(database)
                const rows = statement.all(...bindings) as Row[]
                return { rows, rowsWritten: totalChanges(database) - before }
            }
        })
    }

    transactionSync<T>(operation: () => T): T {
        const database = this.open()
        if (!database.isTransaction) database.exec("BEGIN")
        const savepoint = `terse_savepoint_${++this.savepoint}`
        database.exec(`SAVEPOINT ${savepoint}`)
        const scope = { active: true }
        try {
            const result = this.transaction.run(scope, operation)
            if (
                result !== null &&
                (typeof result === "object" || typeof result === "function") &&
                typeof Reflect.get(result, "then") === "function"
            ) {
                void Promise.resolve(result).catch(() => {})
                throw new Error("SQLite transaction callbacks must be synchronous")
            }
            database.exec(`RELEASE ${savepoint}`)
            return result
        } catch (error) {
            database.exec(`ROLLBACK TO ${savepoint}`)
            database.exec(`RELEASE ${savepoint}`)
            throw error
        } finally {
            scope.active = false
        }
    }

    alarm(): Alarm | undefined {
        const row = this.alarmDatabase().prepare("SELECT generation, deadline FROM __terse_alarm WHERE id = 1").get()
        return row == null ? undefined : (row as unknown as Alarm)
    }

    setAlarm(alarm: Alarm | undefined): void {
        const database = this.alarmDatabase()
        if (alarm === undefined) database.exec("DELETE FROM __terse_alarm")
        else
            database
                .prepare(
                    "INSERT INTO __terse_alarm VALUES (1, ?, ?) ON CONFLICT(id) DO UPDATE SET generation = excluded.generation, deadline = excluded.deadline"
                )
                .all(alarm.generation, alarm.deadline)
    }

    private alarmDatabase(): SqliteConnection {
        const database = this.open()
        if (!database.isTransaction) database.exec("BEGIN")
        database.exec(
            "CREATE TABLE IF NOT EXISTS __terse_alarm (id INTEGER PRIMARY KEY CHECK(id = 1), generation TEXT NOT NULL, deadline INTEGER NOT NULL)"
        )
        return database
    }

    fields(): JsonObject {
        const rows = this.fieldDatabase().prepare("SELECT name, value FROM __terse_fields").all() as {
            name: string
            value: string
        }[]
        return Object.fromEntries(rows.map(row => [row.name, JSON.parse(row.value) as JsonValue]))
    }

    persistFields(fields: JsonObject): void {
        const database = this.fieldDatabase()
        const put = database.prepare(
            "INSERT INTO __terse_fields (name, value) VALUES (?, ?) ON CONFLICT(name) DO UPDATE SET value = excluded.value WHERE value <> excluded.value"
        )
        for (const [name, value] of Object.entries(fields)) put.all(name, JSON.stringify(value))
        database
            .prepare("DELETE FROM __terse_fields WHERE name NOT IN (SELECT value FROM json_each(?))")
            .all(JSON.stringify(Object.keys(fields)))
    }

    restore(state: SqliteState | undefined): void {
        this.close()
        if (state === undefined || !Number.isSafeInteger(state.txid) || state.txid < 1 || !state.path || !state.socket)
            throw new Error("invalid actor SQLite recovery state")
        this.seed = { path: state.path, socket: state.socket }
        this.txid = state.txid
        this.version = this.changeToken()
    }

    async snapshot(): Promise<SqliteState> {
        try {
            const database = this.open()
            const alarm = this.alarm()
            const version = this.changeToken()
            if (database.isTransaction) database.exec("COMMIT")
            if (version !== this.version) {
                const txid = await this.sync(this.seed!)
                if (!Number.isSafeInteger(txid) || txid < this.txid) throw new Error("invalid Litestream transaction")
                this.txid = txid
                this.version = version
            }
            return { txid: this.txid, ...(alarm === undefined ? {} : { alarm }) }
        } catch (cause) {
            this.failure = new SqliteCaptureError("failed to replicate actor SQLite commit", { cause })
            throw this.failure
        }
    }

    rollback(): void {
        if (this.connection?.isTransaction) this.connection.exec("ROLLBACK")
        if (this.connection !== undefined) this.version = this.changeToken()
    }

    close(): void {
        try {
            this.connection?.close()
        } finally {
            this.connection = undefined
            this.seed = undefined
            this.failure = undefined
        }
    }

    private changeToken(): string {
        const database = this.open()
        return JSON.stringify([
            database.prepare("SELECT total_changes() AS changes").get(),
            database.prepare("PRAGMA schema_version").get(),
            database.prepare("PRAGMA user_version").get()
        ])
    }

    private fieldDatabase(): SqliteConnection {
        const database = this.open()
        if (!database.isTransaction) database.exec("BEGIN")
        database.exec(
            "CREATE TABLE IF NOT EXISTS __terse_fields (name TEXT PRIMARY KEY, value TEXT NOT NULL CHECK(json_valid(value)))"
        )
        return database
    }

    private open(): SqliteConnection {
        if (this.transaction.getStore()?.active === false)
            throw new Error("SQLite transaction callbacks must be synchronous")
        if (this.failure !== undefined) throw this.failure
        if (this.connection !== undefined) return this.connection
        if (this.seed === undefined) throw new Error("actor SQLite database has not been restored")
        const database = (this.connection = this.connect(this.seed.path))
        database.exec(
            "PRAGMA foreign_keys = ON; PRAGMA journal_mode = WAL; PRAGMA wal_autocheckpoint = 0; PRAGMA synchronous = FULL"
        )
        if (database.prepare("PRAGMA quick_check").get()!.quick_check !== "ok")
            throw new Error("invalid actor SQLite database")
        return database
    }
}

interface SqliteConnection {
    readonly isTransaction: boolean
    exec(sql: string): void
    prepare(sql: string): {
        all(...bindings: SqliteValue[]): object[]
        get(): Record<string, SqliteValue> | undefined
    }
    close(): void
}

function openSqlite(path: string): SqliteConnection {
    const require = createRequire(import.meta.url)
    if (!process.versions.bun) {
        const { DatabaseSync } = require("node:sqlite") as typeof import("node:sqlite")
        return new DatabaseSync(path)
    }
    const { Database } = require("bun:sqlite") as {
        Database: new (
            path: string,
            options: { strict: boolean }
        ) => Omit<SqliteConnection, "isTransaction"> & {
            readonly inTransaction: boolean
        }
    }
    const database = new Database(path, { strict: true })
    return {
        get isTransaction() {
            return database.inTransaction
        },
        exec: sql => database.exec(sql),
        prepare: sql => database.prepare(sql),
        close: () => database.close()
    }
}

function validateStatement(sql: string): void {
    const statement = withoutComments(sql)
    if (/(?:__terse_|_litestream_)/i.test(statement)) throw new Error("SQLite runtime table names are reserved")
    if (/^(?:BEGIN|COMMIT|END|ROLLBACK|SAVEPOINT|RELEASE|ATTACH|DETACH|VACUUM)\b/i.test(statement))
        throw new Error("actor invocations own SQLite transactions and database files")
    if (
        /^PRAGMA\b/i.test(statement) &&
        !/^PRAGMA\s+(?:main\.)?(?:table_info|table_xinfo|index_info|index_xinfo|index_list|foreign_key_list|foreign_key_check|integrity_check|quick_check|user_version)\b/i.test(
            statement
        )
    )
        throw new Error("this SQLite pragma is managed by the actor runtime")
}

function validateSingleStatement(database: SqliteConnection, sql: string): void {
    const [, tail] = firstStatement(database, sql)
    if (withoutComments(tail)) throw new Error("actor database exec accepts one SQL statement")
}

function firstStatement(database: SqliteConnection, sql: string): [string, string] {
    for (const token of sql.matchAll(sqlTokens)) {
        if (token[0] !== ";") continue
        const end = token.index + 1
        if (!withoutComments(sql.slice(0, end))) continue
        try {
            // SQLite distinguishes a statement boundary from semicolons inside a trigger body.
            database.prepare(sql.slice(0, end))
            return [sql.slice(0, end), sql.slice(end)]
        } catch (error) {
            if (!(error instanceof Error) || !error.message.includes("incomplete input")) throw error
        }
    }
    return [sql, ""]
}

const sqlTokens =
    /--[^\n]*(?:\n|$)|\/\*[\s\S]*?\*\/|'(?:''|[^'])*'|"(?:""|[^"])*"|`(?:``|[^`])*`|\[[^\]]*\]|;|\?|[$:@][a-zA-Z0-9_]+/g

function hasBindings(sql: string): boolean {
    return [...sql.matchAll(sqlTokens)].some(token => /^[?$:@]/.test(token[0]))
}

function totalChanges(database: SqliteConnection): number {
    return Number(database.prepare("SELECT total_changes() AS count").get()!.count)
}

function withoutComments(sql: string): string {
    return sql.replace(/^(?:\s|;|--[^\n]*(?:\n|$)|\/\*[\s\S]*?\*\/)*/, "").trim()
}

export { SqliteActorDatabase, SqliteCaptureError }
export type { ActorDatabaseStorage, SqliteState }
