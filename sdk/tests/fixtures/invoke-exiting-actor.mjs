import { pathToFileURL } from "node:url"

import { seed } from "../../.test-dist/tests/fixtures/litestream.js"
import { ActorWorkerSupervisor } from "../../dist/host/worker-supervisor.js"

const supervisor = new ActorWorkerSupervisor({ actorEntrypointUrl: pathToFileURL(process.argv[2]).href })
try {
    const reply = await supervisor.handle(
        {
            type: "invoke",
            request_id: "exit",
            actor: { project_id: "local", actor_name: "Exiting", actor_id: "one" },
            method: "stop",
            args: [],
            sqlite: await seed(null)
        },
        () => {}
    )
    console.log(JSON.stringify(reply))
} finally {
    supervisor.close()
}
