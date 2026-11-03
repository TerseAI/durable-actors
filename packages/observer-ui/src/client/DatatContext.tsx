import type { ActorInventory } from "../types/Actors.js";
import type { RequestTrace } from "../types/Requests.js";
import type { SocketSession } from "../types/Sockets.js";
import { createContext, useContext, type ReactNode } from "react";
import { useActors } from "../hooks/useActors.js";
import { useRequests } from "../hooks/useRequests.js";
import { useWebSockets } from "../hooks/useWebSockets.js";
import { useTimeWindow } from "./TimeRangeContext.js";


type DataContextType = {
    actorTypes: ActorInventory | null,
    isError: boolean,
    requests: RequestTrace[] | null,
    isRequestsError: boolean,
    sessions: SocketSession[] | null,
    isSocketsError: boolean,
}

const DataContext = createContext<DataContextType | undefined>(undefined)

export const DataProvider = ({ children }: { children: ReactNode }) => {
    const { actorTypes, isError } = useActors()
    const { requests, isError: isRequestsError } = useRequests(useTimeWindow())
    const { sessions, isError: isSocketsError } = useWebSockets()

    return (
        <DataContext.Provider value={{ actorTypes, isError, requests, isRequestsError, sessions, isSocketsError }}>
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
