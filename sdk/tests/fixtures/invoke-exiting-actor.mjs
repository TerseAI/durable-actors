import { pathToFileURL } from "node:url"

import { ResidentActor } from "../../dist/host/resident-actor.js"

import { HostDatabase } from "./host-database.ts"

const database = new HostDatabase()
const actor = new ResidentActor({ actorEntrypointUrl: pathToFileURL(process.argv[2]).href, database: database.call })
await actor.ready()
actor.assign({ project_id: "local", actor_name: "Exiting", actor_id: "one" }, async () => {
    throw new Error("unexpected actor call")
})
const reply = await actor.handle({ type: "invoke", request_id: "exit", method: "stop", args: [] }, { id: 1, allowNext() {}, publish() {}, connections: async () => [] })
actor.close()
console.log(JSON.stringify(reply))
