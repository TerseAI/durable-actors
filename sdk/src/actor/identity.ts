import { z } from "zod"

import { ActorValidationError } from "../errors.js"

function validateActorComponent(name: string, value: string): string {
    const result = actorComponentSchema.safeParse(value)
    if (!result.success)
        throw new ActorValidationError(`${name} may contain only ASCII letters, digits, '.', '-', and '_'`)
    return result.data
}

function actorKey(actor: ActorIdentity): string {
    return `${actor.project_id}\u001f${actor.actor_name}\u001f${actor.actor_id}`
}

const actorComponentSchema = z.string().regex(/^[A-Za-z0-9._-]+$/u)
export const projectIdSchema = actorComponentSchema.max(64).refine(value => value !== "." && value !== "..")
export function validateProjectId(value: unknown): string {
    const result = projectIdSchema.safeParse(value)
    if (!result.success) throw new ActorValidationError("A valid actor project ID is required")
    return result.data
}
const actorIdentitySchema = z.strictObject({
    project_id: projectIdSchema,
    actor_name: actorComponentSchema,
    actor_id: actorComponentSchema
})

type ActorIdentity = z.infer<typeof actorIdentitySchema>

export { actorComponentSchema, actorIdentitySchema, actorKey, validateActorComponent }
export type { ActorIdentity }

export function projectActorPath(projectId: string, actorName: string, actorId: string): string {
    return `/v1/projects/${encodeURIComponent(validateProjectId(projectId))}/actors/${encodeURIComponent(actorName)}/${encodeURIComponent(actorId)}`
}
