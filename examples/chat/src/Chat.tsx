import { useEffect, useState } from "react"
import { createRoot } from "react-dom/client"

import type { State } from "../generated/ChatRoom.frontend.js"
import { ActorClient } from "../generated/frontend.js"

const room = ActorClient().ChatRoom.get("lobby")

function Chat() {
    const [history, setHistory] = useState<State["history"]>([])

    useEffect(() => {
        const unsubscribe = room.subscribe("history", setHistory)
        room.connect().catch(console.error)
        return () => {
            unsubscribe()
            room.close()
        }
    }, [])

    async function send(form: FormData) {
        await room.connect()
        room.send(String(form.get("message")))
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
                    Message <input name="message" required />
                </label>
                <button>Send</button>
            </form>
        </main>
    )
}

createRoot(document.getElementById("root")!).render(<Chat />)
