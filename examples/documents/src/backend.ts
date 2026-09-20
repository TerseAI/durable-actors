import express from "express"
import { createServer } from "vite"

import { actors } from "../generated/index.js"

const app = express()

app.post("/api/socket/:actorName/:actorId", async (request, response) => {
    const { actorName, actorId } = request.params
    if (actorName !== "Workspace" && actorName !== "Document") return response.sendStatus(404)
    response.set("Cache-Control", "no-store").json(await actors[actorName].prepareWebsocket({ actorId, metadata: null }))
})

const vite = await createServer({
    server: { middlewareMode: true, fs: { deny: [".env", ".env.*", "**/.little-actors/**", "**/.git/**"] } }
})
app.use(vite.middlewares)
app.listen(3000, "127.0.0.1", () => console.log("Documents: http://127.0.0.1:3000"))
