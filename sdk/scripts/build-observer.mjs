import { cp, rm } from "node:fs/promises"

const source = new URL("./", import.meta.resolve("@little-actors/observer/standalone/index.html"))
const destination = new URL("../dist/observer/", import.meta.url)
await rm(destination, { recursive: true, force: true })
await cp(source, destination, { recursive: true })
