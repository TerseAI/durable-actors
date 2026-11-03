import type { ActorInventory } from "../types/Actors.js";
import type { RequestTrace } from "../types/Requests.js";
import { createContext, useContext, type ReactNode } from "react";
import { useActors } from "../hooks/useActors.js";
import { useRequests } from "../hooks/useRequests.js";


type DataContextType = {
    actorTypes: ActorInventory | null,
    isError: boolean,
    requests: RequestTrace[] | null,
    isRequestsError: boolean,
}

const DataContext = createContext<DataContextType | undefined>(undefined)

export const DataProvider = ({ children }: { children: ReactNode }) => {
    const { actorTypes, isError } = useActors()
    const { requests, isError: isRequestsError } = useRequests()

    return <DataContext.Provider value={{ actorTypes, isError, requests, isRequestsError }}>{children}</DataContext.Provider>
}


export const useData = (): DataContextType => {
    const context = useContext(DataContext)
    if (!context) {
        throw new Error("useData must be used within a DataProvider")
    }
    return context
}
