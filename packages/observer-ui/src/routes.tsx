import { Navigate, Route, Routes } from "react-router"

import { ActorTypePage } from "./components/actorType/actorTypePage.js"
import { Overview } from "./components/overview/overview.js"

export function ObserverRoutes() {
    return (
        <Routes>
            <Route index element={<Overview />} />
            <Route path="actors/:actorName" element={<ActorTypePage />} />
            <Route path="*" element={<Navigate to=".." replace />} />
        </Routes>
    )
}
