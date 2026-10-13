interface ActorAlarm {
    get(): number | null
    set(timestampMs: number): void
    delete(): void
}

interface ActorAlarmStorage {
    getAlarm(): number | null
    setAlarm(timestampMs: number): void
    deleteAlarm(): void
}
export type { ActorAlarm, ActorAlarmStorage }
