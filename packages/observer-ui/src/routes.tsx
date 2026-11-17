import { useEffect, useRef } from "react"
import { Navigate, Route, Routes, useLocation } from "react-router"

import { useSWRConfig } from "swr"

import { DrawerProvider } from "./client/DrawerContext.js"
import { TimeRangeProvider } from "./client/TimeRangeContext.js"
import { ActorTypePage } from "./components/actorType/actorTypePage.js"
import { InstancePage } from "./components/instance/instancePage.js"
import { Overview } from "./components/overview/overview.js"
import { pinnedLastHour } from "./lib/timeRange.js"

export function ObserverRoutes() {
    return (
        <DrawerProvider>
            <RefreshOnNavigate />
            <Routes>
                <Route index element={<Overview />} />
                <Route path="actors/:actorName" element={<ActorTypePage />} />
                <Route
                    path="actors/:actorName/instances/:actorId"
                    element={
                        <TimeRangeProvider initialRange={pinnedLastHour}>
                            <InstancePage />
                        </TimeRangeProvider>
                    }
                />
                <Route path="*" element={<Navigate to=".." replace />} />
            </Routes>
        </DrawerProvider>
    )
}

// Views on a page read the store when they mount; the shared ones re-read on each navigation.
const sharedViews = ["actors", "websockets", "requests"]

function RefreshOnNavigate() {
    const { pathname } = useLocation()
    const { mutate } = useSWRConfig()
    const first = useRef(true)
    useEffect(() => {
        if (first.current) {
            first.current = false
            return
        }
        void mutate(key => Array.isArray(key) && sharedViews.includes(key[0]))
    }, [pathname, mutate])
    return null
}
