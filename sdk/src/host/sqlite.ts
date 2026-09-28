import { copyFileSync, existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs"
import { createRequire } from "node:module"
import { tmpdir } from "node:os"
import { join } from "node:path"

import type { ActorDatabase, SqliteValue } from "../actor/database.js"

interface SqliteState {
    readonly txid: number
    readonly path?: string
    readonly wal?: { readonly base_txid: number; readonly data: string }
}

class SqliteCaptureError extends Error {}

interface ActorDatabaseStorage extends ActorDatabase {
    restore(state: SqliteState | undefined): void
    checkpoint(durableTxid: number | undefined): void
    snapshot(): SqliteState | undefined
    rollback(): void
    close(): void
}

class SqliteActorDatabase implements ActorDatabaseStorage {
    private connection: SqliteConnection | undefined
    private directory: string | undefined
    private seed: SqliteState | undefined
    private txid = 0
    private baseTxid = 0
    private offset = 0

    constructor(private readonly connect: (path: string) => SqliteConnection = openSqlite) {}

    exec<Row extends object>(sql: string, ...bindings: SqliteValue[]): Row[] {
        validateStatement(sql)
        const database = this.open()
        const statement = database.prepare(sql)
        if (!database.isTransaction) database.exec("BEGIN")
        return statement.all(...bindings) as Row[]
    }

    restore(state: SqliteState | undefined): void {
        this.close()
        if (state !== undefined && (!Number.isSafeInteger(state.txid) || state.txid < 1 || !state.path))
            throw new Error("invalid actor SQLite recovery state")
        this.seed = state
        this.txid = this.baseTxid = state?.txid ?? 0
        this.offset = 0
    }

    checkpoint(durableTxid: number | undefined): void {
        const database = this.connection
        if (durableTxid !== this.txid || database === undefined || database.isTransaction) return
        const result = database.prepare("PRAGMA wal_checkpoint(TRUNCATE)").get()
        if (result?.busy !== 0) return
        this.baseTxid = this.txid
        this.offset = 0
    }

    snapshot(): SqliteState | undefined {
        const database = this.connection
        if (database === undefined || !database.isTransaction) return this.position()
        try {
            database.exec("COMMIT")
            const path = join(this.directory!, "actor.sqlite-wal")
            if (!existsSync(path)) return this.position()
            const wal = readFileSync(path)
            const commit = walCommit(wal)
            if (commit <= this.offset) return this.position()
            this.offset = commit
            this.txid += 1
            return {
                txid: this.txid,
                wal: { base_txid: this.baseTxid, data: wal.subarray(0, commit).toString("base64") }
            }
        } catch (cause) {
            throw new SqliteCaptureError("failed to capture actor SQLite WAL", { cause })
        }
    }

    rollback(): void {
        if (this.connection?.isTransaction) this.connection.exec("ROLLBACK")
    }

    close(): void {
        try {
            this.connection?.close()
        } finally {
            this.connection = undefined
            if (this.directory !== undefined) rmSync(this.directory, { recursive: true, force: true })
            this.directory = undefined
        }
    }

    private position(): SqliteState | undefined {
        return this.txid === 0 ? undefined : { txid: this.txid }
    }

    private open(): SqliteConnection {
        if (this.connection !== undefined) return this.connection
        this.directory = mkdtempSync(join(tmpdir(), "durable-actor-sqlite-"))
        const path = join(this.directory, "actor.sqlite")
        if (this.seed?.path !== undefined) copyFileSync(this.seed.path, path)
        const database = (this.connection = this.connect(path))
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

function withoutComments(sql: string): string {
    return sql.replace(/^(?:\s|;|--[^\n]*(?:\n|$)|\/\*[\s\S]*?\*\/)*/, "").trim()
}

function walCommit(wal: Buffer): number {
    if (wal.length < 32) return 0
    const pageSize = wal.readUInt32BE(8)
    if (pageSize < 512 || pageSize > 65536 || (pageSize & (pageSize - 1)) !== 0)
        throw new Error("invalid SQLite WAL page size")
    let commit = 0
    for (let offset = 32; offset + 24 + pageSize <= wal.length; offset += 24 + pageSize) {
        if (!wal.subarray(offset + 8, offset + 16).equals(wal.subarray(16, 24))) break
        if (wal.readUInt32BE(offset + 4) !== 0) commit = offset + 24 + pageSize
    }
    return commit
}

export { SqliteActorDatabase, SqliteCaptureError }
export type { ActorDatabaseStorage, SqliteState }
