import type { EventKind, PracticeSummary } from "./types.js"

type Fetch = (url: string, options?: RequestInit) => Promise<Response>

export class PracticeApi {
    constructor(private readonly fetch: Fetch) {}

    load(session: string, signal?: AbortSignal): Promise<PracticeSummary> {
        return this.request(session, "", { signal })
    }

    record(session: string, kind: EventKind, made: boolean): Promise<PracticeSummary> {
        return this.request(session, "/events", { method: "POST", body: JSON.stringify({ id: crypto.randomUUID(), kind, made }) })
    }

    undo(session: string, id: string): Promise<PracticeSummary> {
        return this.request(session, "/undo", { method: "POST", body: JSON.stringify({ id }) })
    }

    private async request(session: string, suffix: string, options: RequestInit): Promise<PracticeSummary> {
        const response = await this.fetch(`/api/sessions/${encodeURIComponent(session)}${suffix}`, { ...options, headers: { "content-type": "application/json" } })
        const body = await response.json()
        if (!response.ok) throw new Error(body.error ?? "Could not save your stat. Refresh to check your session.")
        return body
    }
}
