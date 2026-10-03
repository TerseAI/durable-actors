import { AsyncLocalStorage } from "node:async_hooks"

import { ActorDefinitionError } from "../errors.js"

type SqliteValue = string | number | bigint | Uint8Array | null

interface SqliteResult<Row extends object = Record<string, SqliteValue>> {
    rows: Row[]
    /** Rows written by the final statement, including triggers and foreign-key actions. */
    rowsWritten: number
}

interface ActorDatabase {
    /** Executes one SQL statement with positional bindings and returns its rows. */
    exec<Row extends object = Record<string, SqliteValue>>(sql: string, ...bindings: SqliteValue[]): Row[]
    /** Executes a SQL script atomically; only the final statement accepts bindings and returns results. */
    execute<Row extends object = Record<string, SqliteValue>>(
        sql: string,
        ...bindings: SqliteValue[]
    ): SqliteResult<Row>
    /** Runs synchronous SQL in a nested savepoint; actor fields are outside this savepoint. */
    transactionSync<T>(operation: () => T): T
}

const databases = new WeakMap<object, ActorDatabase>()
const invocation = new AsyncLocalStorage<{ actor: object; active: boolean }>()

function actorDatabase(actor: object): ActorDatabase {
    const database = databases.get(actor)
    if (database === undefined) throw new ActorDefinitionError("actor database is unavailable during construction")
    return database
}

function bindActorDatabase(actor: object, database: ActorDatabase): void {
    function check() {
        const context = invocation.getStore()
        if (context?.actor !== actor || !context.active)
            throw new ActorDefinitionError("actor database is unavailable outside its invocation")
    }
    databases.set(actor, {
        exec<Row extends object>(sql: string, ...bindings: SqliteValue[]): Row[] {
            check()
            return database.exec<Row>(sql, ...bindings)
        },
        execute<Row extends object>(sql: string, ...bindings: SqliteValue[]): SqliteResult<Row> {
            check()
            return database.execute<Row>(sql, ...bindings)
        },
        transactionSync<T>(operation: () => T): T {
            check()
            return database.transactionSync(operation)
        }
    })
}

async function runWithActorDatabase<T>(actor: object, operation: () => Promise<T>): Promise<T> {
    const context = { actor, active: true }
    try {
        return await invocation.run(context, operation)
    } finally {
        context.active = false
    }
}

export { actorDatabase, bindActorDatabase, runWithActorDatabase }
export type { ActorDatabase, SqliteResult, SqliteValue }
