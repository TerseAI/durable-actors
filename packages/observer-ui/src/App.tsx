import type { MouseEvent } from "react"

import { Link, Route, Router, Switch } from "wouter"
import { useHashLocation } from "wouter/use-hash-location"

import { DataProvider } from "./client/DatatContext.js"
import { ActorTypePage } from "./components/actorType/actorTypePage.js"
import { Overview } from "./components/overview/overview.js"
import type { ObserverLinkProps } from "./types/Navigation.js"

export function App() {
    return (
        <main className="min-h-screen px-8 pt-7 pb-8 font-satoshi antialiased">
            <DataProvider>
                <Router hook={useHashLocation}>
                    <Switch>
                        <Route path="/actors/:actorName">{params => <ActorTypePage actorName={params.actorName} Link={StandaloneLink} />}</Route>
                        <Route>
                            <Overview Link={StandaloneLink} />
                        </Route>
                    </Switch>
                </Router>
            </DataProvider>
        </main>
    )
}

function StandaloneLink({ to, className, children }: ObserverLinkProps) {
    if (to.page === "overview")
        return (
            <Link href="/" onClick={returnToOverview} className={className}>
                {children}
            </Link>
        )
    return (
        <Link href={`/actors/${encodeURIComponent(to.actorName)}`} state={fromOverview} className={className}>
            {children}
        </Link>
    )
}

const fromOverview = { fromOverview: true }

// Pushing "/" would leave this page as the next back-button stop, so pop the history entry instead.
function returnToOverview(event: MouseEvent) {
    if (history.state?.fromOverview !== true) return
    event.preventDefault()
    history.back()
}
