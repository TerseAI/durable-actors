import express from "express"
import { createServer as createHttpServer } from "node:http"
import { createServer } from "vite"

import { actors } from "../generated/index.js"

const app = express()
const server = createHttpServer(app)
const port = Number(process.env.PORT ?? 3000)

app.post("/api/socket/:actorName/:actorId", async (request, response) => {
    const { actorName, actorId } = request.params
    if (actorName !== "Workspace" && actorName !== "Document") return response.sendStatus(404)
    response.set("Cache-Control", "no-store").json(await actors[actorName].prepareWebsocket({ actorId, metadata: null }))
})

const vite = await createServer({
    server: { middlewareMode: true, hmr: { server }, fs: { deny: [".env", ".env.*", "**/.durable-actors/**", "**/.git/**"] } }
})
app.use(vite.middlewares)
server.listen(port, "127.0.0.1", () => console.log(`Documents: http://127.0.0.1:${port}`))
