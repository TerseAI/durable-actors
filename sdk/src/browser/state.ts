import { SocketError } from "./types.js"

class StateCache<State extends object> {
    private value: Record<string, unknown> | undefined
    private readonly versions = new Map<string, number>()
    private baseline = -1

    get current(): Readonly<State> | undefined {
        return this.value === undefined ? undefined : (structuredClone(this.value) as Readonly<State>)
    }

    snapshot(state: Record<string, unknown>, version: number): void {
        this.value = state
        this.baseline = version
        this.versions.clear()
    }

    update(changes: Record<string, unknown>, removed: readonly string[], version: number): string[] {
        if (!this.value) throw new SocketError("invalid_protocol", "State update arrived before the initial snapshot")
        if (removed.some(field => Object.hasOwn(changes, field)))
            throw new SocketError("invalid_protocol", "State update both changes and removes a field")
        const fields = [...new Set([...Object.keys(changes), ...removed])].filter(
            field => version > (this.versions.get(field) ?? this.baseline)
        )
        const next = { ...this.value }
        for (const field of fields) {
            if (Object.hasOwn(changes, field))
                Object.defineProperty(next, field, {
                    value: changes[field],
                    writable: true,
                    configurable: true,
                    enumerable: true
                })
            else delete next[field]
        }
        this.value = next
        for (const field of fields) this.versions.set(field, version)
        return fields
    }

    field<Key extends keyof State>(key: Key): State[Key] {
        return structuredClone(this.value?.[key as string]) as State[Key]
    }
}

export { StateCache }
