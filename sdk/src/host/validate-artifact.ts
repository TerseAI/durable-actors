import { pathToFileURL } from "node:url"

import { loadActorEntrypoint } from "./actor-loading.js"

await loadActorEntrypoint(pathToFileURL(process.argv[2]!).href)
