import { useState } from "react"

import { useData } from "../../client/DatatContext.js"
import { useActorTypesPage } from "../../hooks/useInstancePage.js"
import { useWebSockets } from "../../hooks/useWebSockets.js"
import { currentSessions } from "../../lib/views/sockets.js"

import { ActiveActorsCard } from "./activeActorsCard.js"
import { ActorTypesCard } from "./actorTypesCard.js"
import { RequestCountCard } from "./requestCountCard.js"
import { SocketsCard } from "./socketsCard.js"

const pageSize = 25

export function Overview() {
    const { baseUrl, events, nowMs } = useData()
    const [page, setPage] = useState(1)
    const { data, error, isValidating, retry } = useActorTypesPage((page - 1) * pageSize, pageSize)
    const [summary, setSummary] = useState(data)
    if (data && data !== summary) setSummary(data)
    const pageCount = Math.max(1, Math.ceil((data?.total ?? 0) / pageSize))
    if (data && page > pageCount) setPage(pageCount)
    const failed = !data && Boolean(error) && !isValidating
    const { sessions, gateways, isError: isSocketsError } = useWebSockets(baseUrl, events)
    return (
        <section aria-labelledby="overview-heading" className="flex flex-col gap-5">
            <header className="flex flex-wrap items-baseline justify-between gap-2">
                <h1 id="overview-heading" className="text-2xl font-bold tracking-tight">
                    Overview
                </h1>
            </header>
            <div className="flex flex-col gap-4">
                <div className="grid gap-4 lg:grid-cols-3">
                    <ActiveActorsCard metrics={summary} isError={!summary && failed} />
                    <RequestCountCard />
                    <SocketsCard
                        open={summary?.openSockets}
                        sessions={sessions && currentSessions(sessions, summary?.gateways ?? {}, nowMs, gateways)}
                        failed={(!summary && failed) || isSocketsError}
                        now={nowMs}
                    />
                </div>
                <ActorTypesCard data={data} failed={failed} retry={retry} page={page} pageCount={pageCount} pageSize={pageSize} onPage={setPage} />
            </div>
        </section>
    )
}
