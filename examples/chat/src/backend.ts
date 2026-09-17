import express from "express"
import { createServer } from "vite"

import { ActorProxy } from "../generated/index.js"

const app = express()

app.post("/api/socket/ChatRoom/:actorId", async (request, response) => {
    const grant = await ActorProxy.handle({
        actorType: "ChatRoom",
        actorId: request.params.actorId,
        metadata: { name: "Guest" }
    })
    response.json(grant)
})

const vite = await createServer({
    server: { middlewareMode: true, fs: { deny: [".env", ".env.*", "**/.little-actors/**", "**/.git/**"] } }
})
app.use(vite.middlewares)
app.listen(3000, "127.0.0.1", () => console.log("Chat: http://127.0.0.1:3000"))
