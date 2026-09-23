import { pathToFileURL } from "node:url"

import { ActorWorkerSupervisor } from "./worker-supervisor.js"

const supervisor = new ActorWorkerSupervisor({ actorEntrypointUrl: pathToFileURL(process.argv[2]!).href })
try {
    await supervisor.ready()
} finally {
    supervisor.close()
}
