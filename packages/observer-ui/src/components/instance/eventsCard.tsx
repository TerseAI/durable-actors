import { useState } from "react"

import { formatCount } from "../../format.js"
import { useEventHistory } from "../../hooks/useEventHistory.js"
import { instanceEvents } from "../../lib/instanceEvents.js"
import { cn } from "../../lib/utils.js"
import type { RecordedEvent } from "../../types/Events.js"
import type { ResidencyPeriod } from "../../types/Residency.js"
import type { TimeWindow } from "../../types/TimeRange.js"
import { SectionCard } from "../ui/sectionCard.js"
import { SegmentedControl } from "../ui/segmentedControl.js"

import { EventList, EventRowSkeletons } from "./eventList.js"
import { levelFill } from "./logChart.js"

type Shown = "all" | "warnings"

const recorded: RecordedEvent["type"][] = ["kill"]

export type EventsCardProps = {
    actorName: string
    actorId: string
    periods: ResidencyPeriod[] | null
    failed: boolean
    timeWindow: TimeWindow
}

export function EventsCard({ actorName, actorId, periods, failed, timeWindow }: EventsCardProps) {
    const [shown, setShown] = useState<Shown>("all")
    const [expanded, setExpanded] = useState(false)
    const { history, isError } = useEventHistory(recorded, timeWindow, actorName, actorId)
    const kills = history?.events.map(event => event.item)
    const events = periods && kills && instanceEvents(periods, kills, timeWindow)
    const warnings = events?.filter(event => event.kill)
    const listed = shown === "all" ? events : warnings
    const unreachable = failed || isError
    return (
        <SectionCard
            title="Events"
            description="Residency changes and sandbox kills."
            aside={<ShownControl value={shown} onChange={setShown} total={events?.length} warnings={warnings?.length} />}
            busy={!listed && !unreachable}
            className="pb-3"
        >
            {unreachable ? (
                <p className="py-4 text-[13px] text-muted-foreground">Couldn't reach the observer API.</p>
            ) : listed ? (
                <EventList events={listed} noun={shown === "all" ? "event" : "warning"} expanded={expanded} onToggle={() => setExpanded(!expanded)} />
            ) : (
                <EventRowSkeletons />
            )}
        </SectionCard>
    )
}

type ShownControlProps = { value: Shown; onChange: (value: Shown) => void; total: number | undefined; warnings: number | undefined }

function ShownControl({ value, onChange, total, warnings }: ShownControlProps) {
    const count = (events: number | undefined) => (events === undefined ? undefined : formatCount(events))
    return (
        <SegmentedControl
            label="Events shown"
            value={value}
            onChange={onChange}
            options={[
                { value: "all", label: "All", badge: count(total) },
                { value: "warnings", label: "Warnings", icon: <i className={cn("size-1.5 rounded-full", levelFill.warn)} />, badge: count(warnings) }
            ]}
        />
    )
}
