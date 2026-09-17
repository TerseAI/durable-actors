import { useEffect, useState } from "react"
import { createRoot } from "react-dom/client"

import { type actors, clients } from "../generated/index.js"

import { DocumentEditor } from "./Editor.js"
import "./style.css"

const workspace = clients.Workspace.get("demo")

function App() {
    const [documents, setDocuments] = useState<actors.Workspace.State["documents"]>([])
    const [selected, select] = useState("welcome")
    const [status, setStatus] = useState(workspace.status)

    useEffect(() => {
        const unsubscribe = workspace.subscribe("documents", setDocuments)
        const stopStatus = workspace.on("status", setStatus)
        workspace.connect().catch(error => {
            if (workspace.status !== "closed") console.error(error)
        })
        return () => {
            unsubscribe()
            stopStatus()
            workspace.close()
        }
    }, [])

    function create(form: FormData) {
        const title = String(form.get("title")).trim()
        if (!title) return
        const id = crypto.randomUUID()
        workspace.send({ id, title })
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
                    {status === "error" && <p role="alert">Could not connect. Reload to retry.</p>}
                </aside>
                <DocumentEditor key={selected} id={selected} title={documents.find(document => document.id === selected)?.title ?? "New document"} />
            </main>
        </div>
    )
}

createRoot(document.getElementById("root")!).render(<App />)
