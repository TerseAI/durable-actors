export type Session = { id: string; date: number }
const key = "courtside.sessions"

export function currentSession(): Session {
    const id = new URL(location.href).searchParams.get("session")
    const saved = recentSessions()
    const valid = id && /^[a-zA-Z0-9-]{1,128}$/.test(id)
    const session = valid ? (saved.find(session => session.id === id) ?? { id, date: Date.now() }) : (saved[0] ?? newSession())
    remember(session)
    return session
}

export function newSession(): Session {
    return { id: crypto.randomUUID(), date: Date.now() }
}

export function recentSessions(): Session[] {
    try {
        const saved: unknown = JSON.parse(localStorage.getItem(key) ?? "[]")
        return Array.isArray(saved) ? saved.filter((item): item is Session => typeof item?.id === "string" && /^[a-zA-Z0-9-]{1,128}$/.test(item.id) && Number.isFinite(item.date)) : []
    } catch {
        return []
    }
}

export function remember(session: Session): void {
    try {
        localStorage.setItem(key, JSON.stringify([session, ...recentSessions().filter(item => item.id !== session.id)].slice(0, 20)))
    } catch {}
    const url = new URL(location.href)
    url.searchParams.set("session", session.id)
    history.replaceState(null, "", url)
}
