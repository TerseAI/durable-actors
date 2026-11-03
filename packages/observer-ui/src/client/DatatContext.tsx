import type { ActorInventory } from "../types/Actors.js";
import { createContext, useContext } from "react";
import { useActors } from "../hooks/useActors.js";


type DataContextType = {
    actorTypes: ActorInventory,
}

const DataContext = createContext<DataContextType | undefined>(undefined)

export const DataProvider = ({ children }: { children: React.ReactNode }) => {
    const { actorTypes } = useActors()


    return <DataContext.Provider value={{ actorTypes }}>{children}</DataContext.Provider>
}


export const useData = (): DataContextType => {
    const context = useContext(DataContext)
    if (!context) {
        throw new Error("useData must be used within a DataProvider")
    }
    return context
}