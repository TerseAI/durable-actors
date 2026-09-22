import { useState } from "react"

import { Activity, ArrowRight, Box, Cable, Hash, LayoutGrid, SunMoon } from "lucide-react"

import { ActorObserver } from "./ActorObserver.js"
import { Overview } from "./Overview.js"
import { RequestObserver } from "./RequestObserver.js"
import { WebSocketObserver } from "./WebSocketObserver.js"
import type { ObserverClient } from "./client.js"
import { defaultTimeRange } from "./time-range.js"
import type { TimeRange } from "./time-range.js"

const views = [
    { id: "overview", label: "little-actors", icon: LayoutGrid },
    { id: "actors", label: "Actors", icon: Box },
    { id: "requests", label: "Requests", icon: Activity },
    { id: "websockets", label: "WebSockets", icon: Cable }
] as const

export function ConsoleApp({ client, toggleTheme }: { client: ObserverClient; toggleTheme: () => void }) {
    const [view, setView] = useState<(typeof views)[number]["id"]>("overview")
    const [actor, setActor] = useState<string>()
    const [timeRange, setTimeRange] = useState<TimeRange>(defaultTimeRange)
    const range = { timeRange, onTimeRangeChange: setTimeRange }
    function selectActor(actorName: string) {
        setActor(actorName)
        setView("actors")
    }
    return (
        <div className="console-shell">
            <a className="skip-link" href="#main">
                Skip to content
            </a>
            <aside className="console-sidebar">
                <div className="console-brand">
                    <Hash aria-hidden="true" />
                    <strong>Terse</strong>
                </div>
                <nav aria-label="Observability">
                    {views.map(item => (
                        <button
                            key={item.id}
                            type="button"
                            aria-current={view === item.id ? "page" : undefined}
                            onClick={() => {
                                setActor(undefined)
                                setView(item.id)
                            }}
                        >
                            <item.icon aria-hidden="true" />
                            {item.label}
                        </button>
                    ))}
                </nav>
                <div className="console-sidebar-foot">
                    <Box aria-hidden="true" />
                    <span>
                        Runtime inspector<small>Read-only observability</small>
                    </span>
                </div>
            </aside>
            <div className="console-workspace">
                <header className="console-topbar">
                    <button className="theme-toggle" type="button" onClick={toggleTheme} aria-label="Toggle color theme">
                        <SunMoon aria-hidden="true" />
                    </button>
                    <a href="https://github.com/TerseAI/little-actors#readme" target="_blank" rel="noopener noreferrer">
                        Docs
                        <ArrowRight aria-hidden="true" />
                    </a>
                </header>
                <main id="main" tabIndex={-1}>
                    {view === "overview" && <Overview client={client} onSelectActor={selectActor} {...range} />}
                    {view === "actors" && <ActorObserver client={client} navigation={{ actorName: actor, onSelectActor: setActor }} {...range} />}
                    {view === "requests" && <RequestObserver client={client} {...range} />}
                    {view === "websockets" && <WebSocketObserver client={client} onSelectActor={selectActor} {...range} />}
                </main>
            </div>
        </div>
    )
}
