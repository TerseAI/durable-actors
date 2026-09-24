import { stateDiff } from "./state-diff.js"

self.onmessage = (event: MessageEvent<{ before: object; after: object }>) => {
    try {
        self.postMessage({ html: stateDiff(event.data.before, event.data.after) })
    } catch {
        self.postMessage({ error: "Could not compare these versions." })
    }
}
