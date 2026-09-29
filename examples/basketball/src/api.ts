import { Router } from "express"
import type { ErrorRequestHandler } from "express"
import { z } from "zod"

import type { EventKind, PracticeSummary } from "./types.js"

interface PracticeClient {
    summary(): Promise<PracticeSummary>
    record(id: string, kind: EventKind, made: boolean): Promise<PracticeSummary>
    undo(id: string): Promise<PracticeSummary>
}

const id = z.string().regex(/^[a-zA-Z0-9-]{1,128}$/)
const event = z.object({ id, kind: z.enum(["two", "three", "free", "rebound", "assist", "steal", "turnover"]), made: z.boolean() })

export function practiceApi(session: (id: string) => PracticeClient): Router {
    const router = Router()
    router.use((_request, response, next) => {
        response.set("Cache-Control", "no-store")
        next()
    })
    router.get("/sessions/:id", async (request, response) => {
        response.json(await session(id.parse(request.params.id)).summary())
    })
    router.post("/sessions/:id/events", async (request, response) => {
        const input = event.parse(request.body)
        response.json(await session(id.parse(request.params.id)).record(input.id, input.kind, input.made))
    })
    router.post("/sessions/:id/undo", async (request, response) => {
        response.json(await session(id.parse(request.params.id)).undo(z.object({ id }).parse(request.body).id))
    })
    const errors: ErrorRequestHandler = (error, _request, response, _next) => {
        const invalid = error instanceof z.ZodError || error instanceof SyntaxError
        if (!invalid) console.error(error)
        response
            .status(invalid ? 400 : 503)
            .json({ error: invalid ? "That stat could not be recorded. Check the input." : "Could not reach your session. Refresh to check whether the last stat was saved." })
    }
    router.use(errors)
    return router
}
