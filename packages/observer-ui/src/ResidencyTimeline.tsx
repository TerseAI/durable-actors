import { useState } from "react"

import { timelineTicks } from "./SocketTimeline.js"
import type { ActorInstance } from "./client.js"
import type { ResidencyPeriod } from "./residency.js"
import { formatDuration } from "./socket-sessions.js"

export interface ResidencyTimelineProps {
    instances: ActorInstance[]
    periods: ResidencyPeriod[]
    start: number
    end: number
    endLabel?: string
    onSelect: (actorId: string) => void
}

export function ResidencyTimeline({ instances, periods, start, end, endLabel = "now", onSelect }: ResidencyTimelineProps) {
    const [hover, setHover] = useState<ResidencyPeriod>()
    const span = Math.max(1, end - start)
    const ticks = timelineTicks(start, end)
    const position = (time: number) => `${((Math.min(end, Math.max(start, time)) - start) / span) * 100}%`
    const byInstance = new Map<string, ResidencyPeriod[]>()
    for (const period of periods) {
        if (period.toMs <= start || period.fromMs >= end) continue
        const row = byInstance.get(period.actorId) ?? []
        row.push(period)
        byInstance.set(period.actorId, row)
    }
    return (
        <div className="residency-history">
            <div className="residency-heading">
                <h3>Residency over time</h3>
                <div className="residency-legend" aria-label="Residency legend">
                    <span>
                        <i className="residency-swatch-live" />
                        In memory
                    </span>
                    <span>
                        <i className="residency-swatch-dormant" />
                        Dormant
                    </span>
                    <span>
                        <i className="residency-swatch-unknown" />
                        Unknown
                    </span>
                </div>
            </div>
            <div className="socket-timeline residency-timeline" role="group" aria-label="Instance residency timeline">
                <div className="socket-timeline-axis" aria-hidden="true">
                    <span>Instance</span>
                    <div>
                        {ticks.map(tick => (
                            <span key={tick} style={{ left: position(tick) }}>
                                {tickTime(tick, span)}
                            </span>
                        ))}
                        <span className="socket-timeline-now">{endLabel}</span>
                    </div>
                </div>
                <div className="socket-timeline-body">
                    <div className="socket-timeline-grid" aria-hidden="true">
                        {ticks.map(tick => (
                            <i key={tick} style={{ left: position(tick) }} />
                        ))}
                    </div>
                    {instances.map(instance => (
                        <div className="socket-timeline-row" key={instance.actorId}>
                            <button
                                className="socket-timeline-label residency-instance"
                                type="button"
                                aria-label={`Inspect instance ${instance.actorId}`}
                                onClick={() => onSelect(instance.actorId)}
                                title={instance.actorId}
                            >
                                <strong>{instance.actorId}</strong>
                            </button>
                            <div className="socket-timeline-lanes residency-lane">
                                {byInstance.get(instance.actorId)?.length ? (
                                    byInstance
                                        .get(instance.actorId)!
                                        .map(period => (
                                            <button
                                                key={`${period.fromMs}-${period.status}`}
                                                type="button"
                                                className={`residency-bar residency-swatch-${period.status}`}
                                                style={{ left: position(period.fromMs), width: `${((Math.min(end, period.toMs) - Math.max(start, period.fromMs)) / span) * 100}%` }}
                                                aria-label={`${instance.actorId}: ${periodLabel(period)}`}
                                                onClick={() => onSelect(instance.actorId)}
                                                onMouseEnter={() => setHover(period)}
                                                onMouseLeave={() => setHover(undefined)}
                                                onFocus={() => setHover(period)}
                                                onBlur={() => setHover(undefined)}
                                            />
                                        ))
                                ) : (
                                    <span className="residency-no-history">No retained residency</span>
                                )}
                            </div>
                        </div>
                    ))}
                </div>
            </div>
            <p className="residency-caption" role={hover ? "tooltip" : undefined}>
                {hover ? `${hover.actorId} · ${periodLabel(hover)}` : "Select a segment to inspect an instance. Gaps before retained history are left blank; missing reports are unknown."}
            </p>
        </div>
    )
}

function periodLabel(period: ResidencyPeriod): string {
    const status = period.status === "live" ? "In memory" : period.status === "dormant" ? "Dormant" : "Unknown"
    return `${status} · ${new Date(period.fromMs).toLocaleString()} – ${new Date(period.toMs).toLocaleString()} · ${formatDuration(period.toMs - period.fromMs)}`
}

function tickTime(time: number, span: number): string {
    const date = new Date(time)
    return span > 2 * 86_400_000
        ? date.toLocaleDateString([], { month: "short", day: "numeric" })
        : date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: span <= 270_000 ? "2-digit" : undefined, hour12: false })
}
