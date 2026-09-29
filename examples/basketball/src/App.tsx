import { useState } from "react"

import { Activity, Check, CheckCheck, ChevronDown, CircleDot, Hand, Layers2, Plus, RotateCcw, Target, Undo2, X, Zap } from "lucide-react"

import type { PracticeApi } from "./client.js"
import { currentSession, newSession, recentSessions, remember } from "./sessions.js"
import type { Session } from "./sessions.js"
import type { CounterKind, EventKind, PracticeEvent, PracticeSummary, ShotKind } from "./types.js"
import { usePractice } from "./use-practice.js"

const shots: { kind: ShotKind; name: string; label: string; value: string }[] = [
    { kind: "two", name: "2-pointer", label: "Inside the arc", value: "02" },
    { kind: "three", name: "3-pointer", label: "Beyond the arc", value: "03" },
    { kind: "free", name: "Free throw", label: "At the line", value: "01" }
]
const counters = [
    { kind: "rebound", label: "Rebounds", icon: Layers2 },
    { kind: "assist", label: "Assists", icon: Hand },
    { kind: "steal", label: "Steals", icon: Zap },
    { kind: "turnover", label: "Turnovers", icon: RotateCcw }
] as const

export function App({ api }: { api: PracticeApi }) {
    const [session, setSession] = useState(currentSession)
    const practice = usePractice(session.id, api)
    const disabled = practice.busy || !!practice.error || !practice.data
    const choose = (session: Session) => {
        remember(session)
        setSession(session)
    }

    return (
        <div className="app-shell">
            <Header session={session} busy={practice.busy} choose={choose} />
            <main>
                <div className="page-heading">
                    <div>
                        <div className="eyebrow">
                            <span className="section-dash" /> SINGLE PLAYER / PRACTICE
                        </div>
                        <h1>Put in the reps.</h1>
                        <p>Your shots. Your stats. Every session saved.</p>
                    </div>
                    <div className="save-status" role="status">
                        {practice.error ? (
                            <>
                                <X size={16} /> Connection interrupted
                            </>
                        ) : practice.busy ? (
                            <>
                                <span className="spinner" /> {practice.data ? "Saving stat…" : "Opening practice…"}
                            </>
                        ) : (
                            <>
                                <CheckCheck size={17} /> Session saved
                            </>
                        )}
                    </div>
                </div>
                {practice.error && (
                    <div className="error-banner" role="alert">
                        <span>{practice.error}</span>
                        <button onClick={practice.refresh}>Refresh session</button>
                    </div>
                )}
                <div className="workspace">
                    <div className="tracking-column">
                        <Scoreboard data={practice.data} />
                        <section className="panel shot-panel" aria-labelledby="shot-title">
                            <div className="panel-heading">
                                <div>
                                    <div className="eyebrow">THE WORK</div>
                                    <h2 id="shot-title">Log a shot</h2>
                                </div>
                                <span className="subtle-label">Make it count.</span>
                            </div>
                            <div className="shot-rows">
                                {shots.map(shot => (
                                    <div className="shot-row" key={shot.kind}>
                                        <span className={`shot-number ${shot.kind}`}>
                                            {shot.value}
                                            <span>PTS</span>
                                        </span>
                                        <div className="shot-label">
                                            <h3>{shot.name}</h3>
                                            <span>{shot.label}</span>
                                        </div>
                                        <button className="shot-button made" disabled={disabled} onClick={() => void practice.record(shot.kind, true)} aria-label={`${shot.name} made`}>
                                            <Plus size={18} /> Made
                                        </button>
                                        <button className="shot-button missed" disabled={disabled} onClick={() => void practice.record(shot.kind, false)} aria-label={`${shot.name} missed`}>
                                            <X size={17} /> Missed
                                        </button>
                                    </div>
                                ))}
                            </div>
                            <div className="undo-bar">
                                <span>Miscounted? Take the last one back.</span>
                                <button className="text-button" disabled={disabled || !practice.data?.recent.length} onClick={() => void practice.undo()}>
                                    <Undo2 size={16} /> Undo last
                                </button>
                            </div>
                        </section>
                        <section className="extra-section" aria-labelledby="extra-title">
                            <div className="compact-heading">
                                <h2 id="extra-title">Beyond the bucket</h2>
                                <span className="subtle-label">The little things add up.</span>
                            </div>
                            <div className="counter-grid">
                                {counters.map(counter => (
                                    <button className="counter" key={counter.kind} disabled={disabled} onClick={() => void practice.record(counter.kind, false)} aria-label={`Add ${counter.kind}`}>
                                        <span className="counter-top">
                                            <counter.icon size={18} />
                                            <Plus size={16} />
                                        </span>
                                        <strong>{practice.data?.counters[counter.kind] ?? 0}</strong>
                                        <span>{counter.label}</span>
                                    </button>
                                ))}
                            </div>
                        </section>
                    </div>
                    <aside className="analytics-column" aria-label="Practice analytics">
                        <Analytics data={practice.data} />
                        <ShotRhythm data={practice.data} />
                    </aside>
                </div>
                <RecentActivity data={practice.data} />
                <footer>
                    <span>
                        <BasketballIcon size={15} /> COURTSIDE
                    </span>
                    <p>Built for the hours nobody sees.</p>
                    <span>{practice.data?.eventCount ?? 0} events this session</span>
                </footer>
            </main>
        </div>
    )
}

function Header({ session, busy, choose }: { session: Session; busy: boolean; choose: (session: Session) => void }) {
    const sessions = recentSessions()
    return (
        <header className="topbar">
            <a className="brand" href={location.href}>
                <span className="brand-mark">
                    <BasketballIcon size={25} strokeWidth={1.7} />
                </span>
                COURTSIDE
                <span className="brand-divider" />
                <span className="brand-subtitle">Practice tracker</span>
            </a>
            <div className="header-actions">
                <label className="session-picker">
                    <span className="sr-only">Practice session</span>
                    <select
                        aria-label="Practice session"
                        value={session.id}
                        disabled={busy}
                        onChange={event => {
                            const selected = sessions.find(session => session.id === event.target.value)
                            if (selected) choose(selected)
                        }}
                    >
                        {sessions.map(item => (
                            <option key={item.id} value={item.id}>
                                {new Date(item.date).toLocaleDateString(undefined, { month: "short", day: "numeric" })} ·{" "}
                                {new Date(item.date).toLocaleTimeString(undefined, { hour: "numeric", minute: "2-digit" })}
                            </option>
                        ))}
                    </select>
                    <ChevronDown size={14} />
                </label>
                <button className="new-button" disabled={busy} onClick={() => choose(newSession())}>
                    <Plus size={17} /> New practice
                </button>
            </div>
        </header>
    )
}

function Scoreboard({ data }: { data: PracticeSummary | null }) {
    const attempts = data?.shots.reduce((total, shot) => total + shot.attempts, 0) ?? 0
    const made = data?.shots.reduce((total, shot) => total + shot.made, 0) ?? 0
    return (
        <section className="scoreboard" aria-label="Session score">
            <div className="scoreboard-top">
                <span>
                    <span className="score-dot" /> SESSION TOTALS
                </span>
                <BasketballIcon size={20} />
            </div>
            <div className="scoreboard-values">
                <div className="points">
                    <strong data-testid="points">{data?.points ?? 0}</strong>
                    <span>POINTS</span>
                </div>
                <div className="score-secondary">
                    <strong>{formatPercentage(data?.fieldGoalPercentage)}</strong>
                    <span>FIELD GOAL %</span>
                    <small>
                        {data?.fieldGoalsMade ?? 0} / {data?.fieldGoalsAttempted ?? 0} from the field
                    </small>
                </div>
                <div className="score-secondary">
                    <strong>{attempts.toString().padStart(2, "0")}</strong>
                    <span>SHOTS TAKEN</span>
                    <small>
                        {made} made · {attempts - made} missed
                    </small>
                </div>
            </div>
            <div className="scoreboard-bottom">
                <Activity size={14} />
                <span>{attempts ? "One rep at a time. Keep going." : "A fresh session. Find your rhythm."}</span>
                <span className="court-lines" aria-hidden="true">
                    ///
                </span>
            </div>
        </section>
    )
}

function Analytics({ data }: { data: PracticeSummary | null }) {
    const total = data?.shots.reduce((sum, shot) => sum + shot.attempts, 0) ?? 0
    const made = data?.shots.reduce((sum, shot) => sum + shot.made, 0) ?? 0
    const rate = total ? (made / total) * 100 : 0
    return (
        <section className="panel analytics-panel" aria-labelledby="analytics-title">
            <div className="panel-heading">
                <div>
                    <div className="eyebrow">THE NUMBERS</div>
                    <h2 id="analytics-title">Shooting breakdown</h2>
                </div>
                <Target size={21} className="muted-icon" />
            </div>
            <div className="accuracy-overview">
                <div className="accuracy-ring">
                    <svg viewBox="0 0 120 120" aria-hidden="true">
                        <circle className="ring-track" cx="60" cy="60" r="50" />
                        <circle className="ring-value" cx="60" cy="60" r="50" pathLength="100" strokeDasharray={`${rate} 100`} />
                    </svg>
                    <div>
                        <strong>
                            {total ? Math.round(rate) : "—"}
                            <span>{total ? "%" : ""}</span>
                        </strong>
                        <small>ALL SHOTS</small>
                    </div>
                </div>
                <div className="accuracy-description">
                    <strong>
                        {made} <span>of {total}</span>
                    </strong>
                    <p>shots made</p>
                    <span className="small-note">Includes free throws</span>
                </div>
            </div>
            <div className="shooting-bars">
                {shots.map(shot => {
                    const stats = data?.shots.find(item => item.kind === shot.kind)
                    return (
                        <div className={`shooting-bar ${shot.kind}`} key={shot.kind}>
                            <div>
                                <strong>{shot.name}</strong>
                                <span>
                                    {stats?.made ?? 0}
                                    <span className="bar-denominator"> / {stats?.attempts ?? 0}</span>
                                    <b>{formatPercentage(stats?.percentage)}</b>
                                </span>
                            </div>
                            <div className="bar-track">
                                <div style={{ width: `${stats?.percentage ?? 0}%` }} />
                            </div>
                        </div>
                    )
                })}
            </div>
            <div className="effective-stat">
                <div>
                    <span>Effective FG%</span>
                    <p>Accounts for the extra value of 3s.</p>
                </div>
                <strong>{formatPercentage(data?.effectiveFieldGoalPercentage)}</strong>
            </div>
        </section>
    )
}

function ShotRhythm({ data }: { data: PracticeSummary | null }) {
    const recent = data?.lastTen ?? []
    const made = recent.filter(shot => shot.made).length
    return (
        <section className="panel rhythm-panel" aria-labelledby="rhythm-title">
            <div className="compact-heading">
                <h2 id="rhythm-title">Last 10 shots</h2>
                <span className="rhythm-score">
                    {made}
                    <span> / {recent.length}</span>
                </span>
            </div>
            <div className="shot-dots" aria-label={`${made} made out of the last ${recent.length} shots`}>
                {Array.from({ length: 10 }, (_, index) => {
                    const shot = recent[index]
                    return (
                        <span className={`shot-dot ${shot ? (shot.made ? "hit" : "miss") : "empty"}`} key={shot?.id ?? index} title={shot ? eventLabel(shot) : "No shot yet"}>
                            {shot ? shot.made ? <Check size={15} /> : <X size={15} /> : "·"}
                        </span>
                    )
                })}
            </div>
            <div className="rhythm-legend">
                <span>
                    <i className="legend-hit" /> Made
                </span>
                <span>
                    <i className="legend-miss" /> Missed
                </span>
                <small>Oldest to newest</small>
            </div>
        </section>
    )
}

function RecentActivity({ data }: { data: PracticeSummary | null }) {
    return (
        <section className="panel activity-panel" aria-labelledby="activity-title">
            <div className="panel-heading">
                <div className="activity-title">
                    <h2 id="activity-title">Session activity</h2>
                    <span className="count-pill">{data?.eventCount ?? 0}</span>
                </div>
                <span className="subtle-label">Most recent first</span>
            </div>
            {!data?.recent.length ? (
                <div className="empty-activity">
                    <CircleDot size={24} />
                    <div>
                        <strong>The floor is yours.</strong>
                        <p>Log your first shot above to start your session.</p>
                    </div>
                </div>
            ) : (
                <div className="event-list">
                    {data.recent.map(event => (
                        <div className="event-row" key={event.id}>
                            <span className={`event-icon ${event.made ? "hit" : "neutral"}`}>{event.made ? <Check size={16} /> : isShot(event.kind) ? <X size={16} /> : <Plus size={16} />}</span>
                            <span className="event-label">{eventLabel(event)}</span>
                            <span className="event-points">{event.made ? `+${event.kind === "three" ? 3 : event.kind === "two" ? 2 : 1} pts` : "—"}</span>
                            <time dateTime={new Date(event.createdAt).toISOString()}>
                                {new Date(event.createdAt).toLocaleTimeString(undefined, { hour: "numeric", minute: "2-digit", second: "2-digit" })}
                            </time>
                        </div>
                    ))}
                </div>
            )}
        </section>
    )
}

function formatPercentage(value: number | null | undefined): string {
    return value == null ? "—" : `${Number.isInteger(value) ? value : value.toFixed(1)}%`
}

function isShot(kind: EventKind): boolean {
    return kind === "two" || kind === "three" || kind === "free"
}

function eventLabel(event: PracticeEvent): string {
    const shot = shots.find(shot => shot.kind === event.kind)
    return shot
        ? `${shot.name} ${event.made ? "made" : "missed"}`
        : `${({ rebound: "Rebound", assist: "Assist", steal: "Steal", turnover: "Turnover" } as Record<CounterKind, string>)[event.kind as CounterKind]} recorded`
}

function BasketballIcon({ size = 24, strokeWidth = 1.7 }: { size?: number; strokeWidth?: number }) {
    return (
        <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={strokeWidth} aria-hidden="true">
            <circle cx="12" cy="12" r="9" />
            <path d="M3 12h18M12 3v18M5.6 5.6c8.5 4 12.8 8.3 12.8 12.8M18.4 5.6C9.9 9.6 5.6 13.9 5.6 18.4" />
        </svg>
    )
}
