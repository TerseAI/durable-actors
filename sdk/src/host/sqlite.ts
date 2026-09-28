import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs"
import { createRequire } from "node:module"
import { tmpdir } from "node:os"
import { join } from "node:path"

import type { ActorDatabase, SqliteValue } from "../actor/database.js"

interface ActorDatabaseStorage extends ActorDatabase {
    restore(image: string | undefined): void
    snapshot(): string | undefined
    rollback(): void
    close(): void
}

class SqliteActorDatabase implements ActorDatabaseStorage {
    private connection: SqliteConnection | undefined
    private directory: string | undefined
    private image: string | undefined

    constructor(private readonly connect: (path: string) => SqliteConnection = openSqlite) {}

    exec<Row extends object>(sql: string, ...bindings: SqliteValue[]): Row[] {
        validateStatement(sql)
        const database = this.open()
        const statement = database.prepare(sql)
        if (!database.isTransaction) database.exec("BEGIN")
        return statement.all(...bindings) as Row[]
    }

    restore(image: string | undefined): void {
        this.close()
        if (image !== undefined) {
            const bytes = Buffer.from(image, "base64")
            if (bytes.toString("base64") !== image || bytes.subarray(0, 16).toString() !== "SQLite format 3\0")
                throw new Error("invalid actor SQLite snapshot")
        }
        this.image = image
    }

    snapshot(): string | undefined {
        const database = this.connection
        if (database === undefined || !database.isTransaction) return this.image
        database.exec("COMMIT")
        const bytes = readFileSync(join(this.directory!, "actor.sqlite"))
        this.image = bytes.length === 0 ? undefined : bytes.toString("base64")
        return this.image
    }

    rollback(): void {
        // Restore the captured image even if COMMIT succeeded but reading the file failed.
        this.close()
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

    private open(): SqliteConnection {
        if (this.connection !== undefined) return this.connection
        this.directory = mkdtempSync(join(tmpdir(), "durable-actor-sqlite-"))
        const path = join(this.directory, "actor.sqlite")
        if (this.image !== undefined) writeFileSync(path, Buffer.from(this.image, "base64"))
        const database = (this.connection = this.connect(path))
        database.exec("PRAGMA foreign_keys = ON; PRAGMA journal_mode = DELETE")
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

export { SqliteActorDatabase }
export type { ActorDatabaseStorage }
