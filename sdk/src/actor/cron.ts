import { z } from "zod"

import { ActorDefinitionError } from "../errors.js"

/** A UTC schedule occurrence. Retries retain the original expression and Unix time in milliseconds. */
interface CronEvent {
    readonly cron: string
    readonly scheduledTime: number
}

const cronOptionsSchema = z.strictObject({ retries: z.number().int().min(0).max(2147483647).optional() })
type CronOptions = z.infer<typeof cronOptionsSchema>

interface CronSchedule extends CronOptions {
    readonly method: string
    readonly expression: string
}

/** Runs on a five-field UTC schedule. Handler retries default to zero; delivery recovery is automatic. */
function Cron(expression: string, options: CronOptions = {}) {
    if (typeof expression !== "string" || expression.trim().split(/\s+/u).length !== 5)
        throw new ActorDefinitionError("@Cron requires a five-field cron expression")
    const parsed = cronOptionsSchema.safeParse(options)
    if (!parsed.success) throw new ActorDefinitionError(`Invalid @Cron options: ${parsed.error.message}`)
    return function (_value: (event: CronEvent) => Promise<void>, context: ClassMethodDecoratorContext): void {
        if (context.kind !== "method" || context.static || context.private || typeof context.name !== "string")
            throw new ActorDefinitionError("@Cron requires a public instance async method")
    }
}

export { Cron, cronOptionsSchema }
export type { CronEvent, CronOptions, CronSchedule }
