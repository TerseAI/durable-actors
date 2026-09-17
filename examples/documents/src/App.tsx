import { useEffect, useRef, useState } from "react"
import { createRoot } from "react-dom/client"

import { DocumentEditor } from "./Editor.js"
import "./style.css"

function App() {
    const workspace = useRef<WebSocket | undefined>(undefined)
    const [documents, setDocuments] = useState<{ id: string; title: string }[]>([])
    const [selected, select] = useState("welcome")
    const [status, setStatus] = useState("connecting")

    useEffect(() => {
        let disposed = false
        async function open() {
            const response = await fetch("/api/socket/Workspace/demo", { method: "POST" })
            if (!response.ok) throw new Error(`Connection denied (${response.status})`)
            const { websocketUrl } = await response.json()
            if (disposed) return
            const socket = new WebSocket(websocketUrl)
            workspace.current = socket
            socket.onopen = () => {
                if (!disposed) setStatus("open")
            }
            socket.onmessage = event => {
                if (!disposed) setDocuments(JSON.parse(event.data))
            }
            socket.onclose = () => {
                if (!disposed) setStatus("closed")
            }
            socket.onerror = () => {
                if (!disposed) setStatus("error")
            }
        }
        void open().catch(error => {
            if (disposed) return
            setStatus("error")
            console.error(error)
        })
        return () => {
            disposed = true
            workspace.current?.close()
        }
    }, [])

    function create(form: FormData) {
        const title = String(form.get("title")).trim()
        if (!title || workspace.current?.readyState !== WebSocket.OPEN) return
        const id = crypto.randomUUID()
        workspace.current.send(JSON.stringify({ id, title }))
        select(id)
    }

    return (
        <div className="app">
            <header>
                <h1>Documents</h1>
                <p>Open another tab to write together.</p>
            </header>
            <main>
                <aside aria-label="Document list">
                    <form action={create}>
                        <input name="title" aria-label="New document title" placeholder="Document title" required disabled={status !== "open"} />
                        <button disabled={status !== "open"}>Add document</button>
                    </form>
                    <nav aria-label="Documents">
                        {documents.map(document => (
                            <button key={document.id} aria-current={selected === document.id ? "page" : undefined} onClick={() => select(document.id)}>
                                {document.title}
                            </button>
                        ))}
                    </nav>
                    {(status === "error" || status === "closed") && <p role="alert">Disconnected. Reload to retry.</p>}
                </aside>
                <DocumentEditor key={selected} id={selected} title={documents.find(document => document.id === selected)?.title ?? "New document"} />
            </main>
        </div>
    )
}

createRoot(document.getElementById("root")!).render(<App />)
