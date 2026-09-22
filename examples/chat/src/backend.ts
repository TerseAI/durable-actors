import express from "express"
import { createServer } from "vite"

import { actors } from "../generated/index.js"

const app = express()

app.post("/api/socket/ChatRoom/:actorId", async (request, response) => {
    const grant = await actors.ChatRoom.prepareWebsocket({
        actorId: request.params.actorId,
        metadata: { name: "Guest" }
    })
    response.set("Cache-Control", "no-store").json(grant)
})

const vite = await createServer({
    server: { middlewareMode: true, fs: { deny: [".env", ".env.*", "**/.durable-actors/**", "**/.little-actors/**", "**/.git/**"] } }
})
app.use(vite.middlewares)
app.listen(3000, "127.0.0.1", () => console.log("Chat: http://127.0.0.1:3000"))
