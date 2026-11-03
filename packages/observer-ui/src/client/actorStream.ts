import type { ActorInventory } from "../types/Actors.js"

export function subscribeToActors(onInventory: (inventory: ActorInventory) => void): () => void {
    const source = new EventSource("/api/observe/events")
    source.addEventListener("inventory", event => onInventory(JSON.parse(event.data)))
    return () => source.close()
}
