import { type ReactNode, createContext, useContext } from "react"

import { useActors } from "../hooks/useActors.js"
import { useEventStream } from "../hooks/useLiveView.js"
import { useNow } from "../hooks/useNow.js"
import { useRequests } from "../hooks/useRequests.js"
import { useWebSockets } from "../hooks/useWebSockets.js"
import { currentInventory } from "../lib/views/actors.js"
import { currentSessions } from "../lib/views/sockets.js"
import type { ActorInventory } from "../types/Actors.js"
import type { RequestTrace } from "../types/Requests.js"
import type { SocketSession } from "../types/Sockets.js"

import { useTimeWindow } from "./TimeRangeContext.js"
import type { EventBus } from "./events.js"
import { defaultBaseUrl } from "./observerClient.js"

// Statuses that change without an event (unknown, lost) are worked out on this tick.
const statusTickMs = 5_000

const projectEvents = new URLSearchParams({ signals: "requests,platform" })

type DataContextType = {
    baseUrl: string
    events: EventBus | null
    actorTypes: ActorInventory | null
    isError: boolean
    requests: RequestTrace[] | null
    isRequestsError: boolean
    sessions: SocketSession[] | null
    isSocketsError: boolean
}

const DataContext = createContext<DataContextType | undefined>(undefined)

export const DataProvider = ({ baseUrl = defaultBaseUrl, children }: { baseUrl?: string; children: ReactNode }) => {
    const events = useEventStream(baseUrl, projectEvents)
    const now = useNow(statusTickMs)
    const { inventory, isError } = useActors(baseUrl, events)
    const { requests, isError: isRequestsError } = useRequests(baseUrl, useTimeWindow(), events)
    const { sessions, isError: isSocketsError } = useWebSockets(baseUrl, events)

    return (
        <DataContext.Provider
            value={{
                baseUrl,
                events,
                actorTypes: inventory && currentInventory(inventory, now),
                isError,
                requests,
                isRequestsError,
                sessions: sessions && currentSessions(sessions, inventory?.gateways ?? {}, now),
                isSocketsError
            }}
        >
            {children}
        </DataContext.Provider>
    )
}

export const useData = (): DataContextType => {
    const context = useContext(DataContext)
    if (!context) {
        throw new Error("useData must be used within a DataProvider")
    }
    return context
}
