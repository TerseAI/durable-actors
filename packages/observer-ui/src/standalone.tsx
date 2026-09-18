import { useState } from "react"
import { createRoot } from "react-dom/client"

import { Activity, Box, ChevronRight, SunMoon } from "lucide-react"

import { ActorObserver } from "./ActorObserver.js"
import { RequestObserver } from "./RequestObserver.js"
import { HttpObserverClient } from "./client.js"
import "./standalone.css"
import "./styles.css"
import "./theme.css"

const client = new HttpObserverClient()
const darkMode = window.matchMedia("(prefers-color-scheme: dark)")
let manualTheme = false
const updateTheme = () => {
    if (!manualTheme) document.documentElement.classList.toggle("dark", darkMode.matches)
}
updateTheme()
darkMode.addEventListener("change", updateTheme)

function Standalone() {
    const [view, setView] = useState<"actors" | "requests">("actors")
    function toggleTheme() {
        manualTheme = true
        const next = !document.documentElement.classList.contains("dark")
        document.documentElement.classList.toggle("dark", next)
    }
    return (
        <>
            <a className="skip-link" href="#main">
                Skip to content
            </a>
            <header className="console-header">
                <div className="console-header-inner">
                    <div className="console-brand">
                        <Box aria-hidden="true" />
                        <strong>little-actors</strong>
                        <span className="console-divider">/</span>
                        <span>Observability</span>
                    </div>
                    <button className="theme-toggle" type="button" onClick={toggleTheme} aria-label="Toggle color theme">
                        <SunMoon aria-hidden="true" />
                    </button>
                </div>
            </header>
            <div className="console-subnav">
                <div>
                    <nav aria-label="Observability">
                        <button type="button" aria-current={view === "actors" ? "page" : undefined} onClick={() => setView("actors")}>
                            <Box aria-hidden="true" />
                            Actors
                        </button>
                        <button type="button" aria-current={view === "requests" ? "page" : undefined} onClick={() => setView("requests")}>
                            <Activity aria-hidden="true" />
                            Requests
                        </button>
                    </nav>
                    <span className="console-readonly">Read-only</span>
                </div>
            </div>
            <main id="main" tabIndex={-1}>
                {view === "actors" ? <ActorObserver client={client} /> : <RequestObserver client={client} />}
            </main>
            <footer className="console-footer">
                <span>
                    little-actors <ChevronRight aria-hidden="true" /> Observability
                </span>
                <span>Actor runtime inspector</span>
            </footer>
        </>
    )
}
createRoot(document.getElementById("root")!).render(<Standalone />)
