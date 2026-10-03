import { AsyncLocalStorage } from "node:async_hooks"
import { randomUUID } from "node:crypto"

import { ActorDefinitionError } from "../errors.js"

interface Alarm {
    readonly generation: string
    readonly deadline: number
}
interface AlarmStorage {
    alarm(): Alarm | undefined
    setAlarm(alarm: Alarm | undefined): void
}
const stores = new WeakMap<object, AlarmStorage>()
const invocation = new AsyncLocalStorage<{ actor: object; active: boolean }>()

function bindActorAlarm(actor: object, storage: AlarmStorage): void {
    stores.set(actor, storage)
}
function alarmStore(actor: object): AlarmStorage {
    const scope = invocation.getStore()
    if (scope?.actor !== actor || !scope.active)
        throw new ActorDefinitionError("alarms are unavailable outside an actor invocation")
    return stores.get(actor)!
}
function getAlarm(actor: object): number | null {
    return alarmStore(actor).alarm()?.deadline ?? null
}
function setAlarm(actor: object, deadline: number): void {
    if (!Number.isSafeInteger(deadline) || deadline < 0)
        throw new TypeError("alarm deadline must be nonnegative Unix milliseconds")
    if (typeof Reflect.get(actor, "onAlarm") !== "function")
        throw new ActorDefinitionError("setAlarm requires an onAlarm handler")
    alarmStore(actor).setAlarm({ generation: randomUUID(), deadline })
}
function deleteAlarm(actor: object): void {
    alarmStore(actor).setAlarm(undefined)
}
async function runWithActorAlarm<T>(actor: object, operation: () => Promise<T>): Promise<T> {
    const scope = { actor, active: true }
    try {
        return await invocation.run(scope, operation)
    } finally {
        scope.active = false
    }
}
async function deliverAlarm(actor: object, generation: unknown): Promise<void> {
    const store = alarmStore(actor)
    const current = store.alarm()
    if (current === undefined || current.generation !== generation) return
    if (current.deadline > Date.now()) throw new Error("alarm delivery arrived before its deadline")
    const handler: unknown = Reflect.get(actor, "onAlarm")
    if (typeof handler !== "function") throw new ActorDefinitionError("alarm delivery requires an onAlarm handler")
    await Reflect.apply(handler, actor, [])
    if (store.alarm()?.generation === generation) store.setAlarm(undefined)
}
export { bindActorAlarm, runWithActorAlarm, getAlarm, setAlarm, deleteAlarm, deliverAlarm }
export type { Alarm, AlarmStorage }
