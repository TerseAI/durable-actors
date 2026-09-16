import { fromBase64, toBase64 } from "lib0/buffer"
import type { ConnectionStatus } from "little-actors/browser"
import * as Y from "yjs"

import { ActorClient } from "../generated/frontend.js"

export function openDocument(id: string, onStatus: (status: ConnectionStatus) => void) {
    const room = ActorClient().Document.get(id)
    const document = new Y.Doc()
    const unsubscribe = room.subscribe("content", content => {
        if (content) Y.applyUpdate(document, fromBase64(content), room)
    })
    const stopStatus = room.on("status", onStatus)
    const stopOpen = room.on("open", () => room.send(toBase64(Y.encodeStateAsUpdate(document))))
    const send = (update: Uint8Array, origin: unknown) => {
        if (origin !== room && room.status === "open") room.send(toBase64(update))
    }
    document.on("update", send)
    room.connect().catch(error => {
        if (room.status !== "closed") console.error(error)
    })
    return {
        document,
        close() {
            document.off("update", send)
            unsubscribe()
            stopStatus()
            stopOpen()
            room.close()
            document.destroy()
        }
    }
}
