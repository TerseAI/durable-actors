import { useEffect, useRef, useState } from "react"
import { createRoot } from "react-dom/client"

function Chat() {
    const room = useRef<WebSocket | undefined>(undefined)
    const [history, setHistory] = useState<{ name: string; text: string }[]>([])
    const [connected, setConnected] = useState(false)

    useEffect(() => {
        let disposed = false
        async function open() {
            const response = await fetch("/api/socket/ChatRoom/lobby", { method: "POST" })
            if (!response.ok) throw new Error(`Connection denied (${response.status})`)
            const { websocketUrl } = await response.json()
            if (disposed) return
            const socket = new WebSocket(websocketUrl)
            room.current = socket
            socket.onopen = () => {
                if (!disposed) setConnected(true)
            }
            socket.onmessage = event => {
                if (!disposed) setHistory(JSON.parse(event.data))
            }
            socket.onclose = () => {
                if (!disposed) setConnected(false)
            }
            socket.onerror = () => {
                if (!disposed) setConnected(false)
            }
        }
        void open().catch(console.error)
        return () => {
            disposed = true
            room.current?.close()
        }
    }, [])

    function send(form: FormData) {
        if (room.current?.readyState === WebSocket.OPEN) room.current.send(JSON.stringify(String(form.get("message"))))
    }

    return (
        <main>
            <h1>The lobby</h1>
            <ul role="log" aria-label="Messages">
                {history.map((item, index) => (
                    <li key={index}>
                        <strong>{item.name}:</strong> {item.text}
                    </li>
                ))}
            </ul>
            <form action={send}>
                <label>
                    Message <input name="message" required disabled={!connected} />
                </label>
                <button disabled={!connected}>Send</button>
            </form>
            {!connected && <p>Connecting or disconnected. Reload to retry.</p>}
        </main>
    )
}

createRoot(document.getElementById("root")!).render(<Chat />)
