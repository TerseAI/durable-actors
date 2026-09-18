import { createRoot } from "react-dom/client"

import { Box, ChevronRight, SunMoon } from "lucide-react"

import { ActorObserver } from "./ActorObserver.js"
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
                    <span aria-current="page">
                        <Box aria-hidden="true" />
                        Actors
                    </span>
                    <span className="console-readonly">Read-only</span>
                </div>
            </div>
            <main id="main" tabIndex={-1}>
                <ActorObserver client={client} />
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
