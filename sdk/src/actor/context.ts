import { ActorDefinitionError } from "../errors.js"

import { isActorInvocation } from "./database.js"
import type { ActorDatabase, SqliteValue } from "./database.js"
import { validateActorComponent } from "./identity.js"

declare const actorContextBrand: unique symbol

interface ActorContext {
    readonly [actorContextBrand]: never
}

class RuntimeActorContext implements ActorContext {
    declare readonly [actorContextBrand]: never
    readonly actorId: string
    readonly persistedFields = new Map<string, PropertyDescriptor>()
    readonly #database: ActorDatabase | undefined
    #actor: object | undefined
    #constructing = false

    private constructor(actorId: string, database?: ActorDatabase) {
        this.actorId = validateActorComponent("actor ID", actorId)
        this.#database = database && this.guard(database)
    }

    static runtime(actorId: string, database: ActorDatabase): RuntimeActorContext {
        return new RuntimeActorContext(actorId, database)
    }

    static reference(actorId: string): RuntimeActorContext {
        return new RuntimeActorContext(actorId)
    }

    static bind(context: unknown, actor: object): RuntimeActorContext {
        if (typeof context !== "object" || context === null || !(#actor in context) || context.#actor !== undefined)
            throw new ActorDefinitionError("actors are constructed by the actor runtime")
        context.#actor = actor
        return context
    }

    construct(actorClass: Function): object {
        this.#constructing = true
        try {
            const actor = Reflect.construct(actorClass, [this])
            if (this.#actor !== actor)
                throw new ActorDefinitionError("actor constructors must call super with their ActorContext")
            return actor
        } finally {
            this.#constructing = false
        }
    }

    get database(): ActorDatabase {
        if (this.#database === undefined)
            throw new ActorDefinitionError("actor database is unavailable outside the actor runtime")
        return this.#database
    }

    assertAccess(): void {
        if (this.#constructing || (this.#actor !== undefined && isActorInvocation(this.#actor))) return
        throw new ActorDefinitionError("actor database is unavailable outside its invocation")
    }

    private guard(database: ActorDatabase): ActorDatabase {
        return {
            exec: <Row extends object>(sql: string, ...bindings: SqliteValue[]): Row[] => {
                this.assertAccess()
                return database.exec<Row>(sql, ...bindings)
            }
        }
    }
}

export { RuntimeActorContext }
export type { ActorContext }
