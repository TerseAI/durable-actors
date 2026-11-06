import { Navigate, Route, Routes, useParams } from "react-router"

import { DrawerProvider } from "./client/DrawerContext.js"
import { TimeRangeProvider } from "./client/TimeRangeContext.js"
import { ActorTypePage } from "./components/actorType/actorTypePage.js"
import { InstancePage } from "./components/instance/instancePage.js"
import { Overview } from "./components/overview/overview.js"
import { pinnedLastHour } from "./lib/timeRange.js"

export function ObserverRoutes() {
    return (
        <DrawerProvider>
            <Routes>
                <Route index element={<Overview />} />
                <Route path="actors/:actorName" element={<ActorTypePage />} />
                <Route
                    path="actors/:actorName/instances/:actorId"
                    element={
                        <TimeRangeProvider initialRange={pinnedLastHour}>
                            <InstanceRoute />
                        </TimeRangeProvider>
                    }
                />
                <Route path="*" element={<Navigate to=".." replace />} />
            </Routes>
        </DrawerProvider>
    )
}

// Filters and selections belong to one instance, so each instance gets a fresh page.
function InstanceRoute() {
    const { actorName, actorId } = useParams()
    return <InstancePage key={`${actorName}/${actorId}`} />
}
