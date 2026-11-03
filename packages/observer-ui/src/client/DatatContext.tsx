import type { ActorInventory } from "../types/Actors.js";
import { createContext, useContext, type ReactNode } from "react";
import { useActors } from "../hooks/useActors.js";


type DataContextType = {
    actorTypes: ActorInventory | null,
    isError: boolean,
}

const DataContext = createContext<DataContextType | undefined>(undefined)

export const DataProvider = ({ children }: { children: ReactNode }) => {
    const { actorTypes, isError } = useActors()

    return <DataContext.Provider value={{ actorTypes, isError }}>{children}</DataContext.Provider>
}


export const useData = (): DataContextType => {
    const context = useContext(DataContext)
    if (!context) {
        throw new Error("useData must be used within a DataProvider")
    }
    return context
}
