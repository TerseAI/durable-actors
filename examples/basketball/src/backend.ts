import express from "express"
import { createServer as createHttpServer } from "node:http"
import { createServer } from "vite"

import { actors } from "../generated/index.js"

import { practiceApi } from "./api.js"

const app = express()
const server = createHttpServer(app)
const port = Number(process.env.PORT ?? 3003)

app.use(
    "/api",
    express.json({ limit: "4kb" }),
    practiceApi(id => actors.Practice.get(id))
)

const vite = await createServer({
    server: { middlewareMode: true, hmr: { server }, fs: { deny: [".env", ".env.*", "**/.durable-actors/**", "**/.git/**"] } }
})
app.use(vite.middlewares)
server.listen(port, "127.0.0.1", () => console.log(`Courtside: http://127.0.0.1:${port}`))
