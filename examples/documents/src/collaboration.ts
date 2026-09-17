import { fromBase64, toBase64 } from "lib0/buffer"
import * as Y from "yjs"

export type ConnectionStatus = "connecting" | "open" | "closed" | "error"

export function openDocument(id: string, onStatus: (status: ConnectionStatus) => void) {
    let socket: WebSocket | undefined
    let disposed = false
    const document = new Y.Doc()
    const send = (update: Uint8Array, origin: unknown) => {
        if (origin !== socket && socket?.readyState === WebSocket.OPEN) socket.send(JSON.stringify(toBase64(update)))
    }
    document.on("update", send)
    async function open() {
        const response = await fetch(`/api/socket/Document/${encodeURIComponent(id)}`, { method: "POST" })
        if (!response.ok) throw new Error(`Connection denied (${response.status})`)
        const { websocketUrl } = await response.json()
        if (disposed) return
        socket = new WebSocket(websocketUrl)
        socket.onmessage = event => {
            if (disposed) return
            Y.applyUpdate(document, fromBase64(JSON.parse(event.data)), socket)
            onStatus("open")
        }
        socket.onclose = () => {
            if (!disposed) onStatus("closed")
        }
        socket.onerror = () => {
            if (!disposed) onStatus("error")
        }
    }
    void open().catch(error => {
        if (disposed) return
        onStatus("error")
        console.error(error)
    })
    return {
        document,
        close() {
            document.off("update", send)
            disposed = true
            socket?.close()
            document.destroy()
        }
    }
}
