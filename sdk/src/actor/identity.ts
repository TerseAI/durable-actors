import { z } from "zod"

import { ActorValidationError } from "../errors.js"

function validateActorComponent(name: string, value: string): string {
    const result = actorComponentSchema.safeParse(value)
    if (!result.success)
        throw new ActorValidationError(`${name} may contain only ASCII letters, digits, '.', '-', and '_'`)
    return result.data
}

function actorKey(actor: ActorIdentity): string {
    return `${actor.actor_type}\u001f${actor.actor_id}`
}

const actorComponentSchema = z.string().regex(/^[A-Za-z0-9._-]+$/u)
const actorIdentitySchema = z.strictObject({
    actor_type: actorComponentSchema,
    actor_id: actorComponentSchema
})

type ActorIdentity = z.infer<typeof actorIdentitySchema>

export { actorComponentSchema, actorIdentitySchema, actorKey, validateActorComponent }
export type { ActorIdentity }
