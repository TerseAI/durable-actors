import { ActorValidationError } from "./errors.js"

export interface ActorInvocationOptions {
    /** Use createActorInvocationKey() to persist a logical operation's key; generated automatically when omitted. Reuse only for identical input and caller/actor. */
    readonly idempotencyKey?: string
}

/**
 * Persist this key before invoking when recovery must survive a caller restart.
 * Keys expire after five minutes; bounded server receipts can retire them sooner.
 * Never replace an expired or unknown operation's key automatically.
 */
export function createActorInvocationKey(): string {
    return `${Math.floor(performance.timeOrigin + performance.now())}.${globalThis.crypto.randomUUID()}`
}

export function validateInvocationKey(key: string): string {
    if (typeof key !== "string" || key.length > 255 || !/^(0|[1-9][0-9]*)\.[A-Za-z0-9_-]+$/u.test(key))
        throw new ActorValidationError("idempotency key must be <UnixMilliseconds>.<nonce>")
    return key
}
