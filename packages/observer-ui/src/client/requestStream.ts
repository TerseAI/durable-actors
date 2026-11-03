import type { RequestTracePage } from "../types/Requests.js"

export function subscribeToRequests(onPage: (page: RequestTracePage) => void): () => void {
    const source = new EventSource("/api/observe/requests/events")
    source.addEventListener("requests", event => onPage(JSON.parse(event.data)))
    return () => source.close()
}
