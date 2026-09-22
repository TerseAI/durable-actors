import { useState } from "react"
import type { DateRange } from "react-day-picker"

import { Calendar as CalendarIcon, ChevronDown } from "lucide-react"

import { Button } from "./components/ui/button.js"
import { Calendar } from "./components/ui/calendar.js"
import { Input } from "./components/ui/input.js"
import { Popover, PopoverContent, PopoverTrigger } from "./components/ui/popover.js"
import { rangeLabel, relativePresets, resolveRange, sameRange } from "./time-range.js"
import type { TimeRange } from "./time-range.js"

interface TimeRangePickerProps {
    value: TimeRange
    onChange: (range: TimeRange) => void
    allowAll?: boolean
    className?: string
    align?: "start" | "end"
}

interface Draft {
    dates: DateRange | undefined
    fromTime: string
    toTime: string
}

export function TimeRangePicker({ value, onChange, allowAll = true, className, align = "end" }: TimeRangePickerProps) {
    const [open, setOpen] = useState(false)
    const [draft, setDraft] = useState<Draft>(() => draftFor(value))
    const [invalid, setInvalid] = useState(false)
    const choose = (range: TimeRange) => {
        onChange(range)
        setOpen(false)
    }
    return (
        <Popover
            open={open}
            onOpenChange={next => {
                if (next) {
                    setDraft(draftFor(value))
                    setInvalid(false)
                }
                setOpen(next)
            }}
        >
            <PopoverTrigger asChild>
                <Button type="button" variant="outline" className={className} aria-label={`Time range: ${rangeLabel(value)}`}>
                    <CalendarIcon aria-hidden="true" />
                    <span className="la:truncate la:tabular-nums">{rangeLabel(value)}</span>
                    <ChevronDown aria-hidden="true" className="la:text-muted-foreground" />
                </Button>
            </PopoverTrigger>
            <PopoverContent align={align} aria-label="Time range" className="la:flex la:w-auto la:p-0 la:text-sm">
                <div className="la:flex la:flex-col la:gap-0.5 la:border-r la:p-2" role="group" aria-label="Preset ranges">
                    {relativePresets.map(preset => (
                        <Preset
                            key={preset.minutes}
                            label={preset.label}
                            selected={sameRange(value, { kind: "relative", minutes: preset.minutes })}
                            onSelect={() => choose({ kind: "relative", minutes: preset.minutes })}
                        />
                    ))}
                    {allowAll && <Preset label="All retained" selected={value.kind === "all"} onSelect={() => choose({ kind: "all" })} />}
                </div>
                <form
                    className="la:flex la:w-[22rem] la:flex-col"
                    aria-label="Custom range"
                    onSubmit={event => {
                        event.preventDefault()
                        const range = absoluteRange(draft)
                        if (!range) {
                            setInvalid(true)
                            return
                        }
                        setInvalid(false)
                        choose(range)
                    }}
                >
                    <Calendar
                        mode="range"
                        numberOfMonths={1}
                        defaultMonth={draft.dates?.to ?? draft.dates?.from}
                        selected={draft.dates}
                        onSelect={dates => setDraft(current => ({ ...current, dates }))}
                        disabled={{ after: new Date() }}
                    />
                    <div className="la:flex la:items-end la:gap-2 la:border-t la:px-3 la:py-3">
                        <label className="la:grid la:gap-1 la:text-xs la:text-muted-foreground">
                            From
                            <Input
                                type="time"
                                name="from"
                                required
                                className="la:h-8 la:w-24 la:tabular-nums"
                                value={draft.fromTime}
                                onChange={event => setDraft({ ...draft, fromTime: event.target.value })}
                                aria-invalid={invalid || undefined}
                            />
                        </label>
                        <label className="la:grid la:gap-1 la:text-xs la:text-muted-foreground">
                            To
                            <Input
                                type="time"
                                name="to"
                                required
                                className="la:h-8 la:w-24 la:tabular-nums"
                                value={draft.toTime}
                                onChange={event => setDraft({ ...draft, toTime: event.target.value })}
                                aria-invalid={invalid || undefined}
                            />
                        </label>
                        <Button type="submit" variant="outline" size="sm" className="la:ml-auto" disabled={!draft.dates?.from}>
                            Apply range
                        </Button>
                    </div>
                    {invalid && (
                        <p className="la:px-3 la:pb-3 la:text-xs la:text-danger" role="alert">
                            From must be before To.
                        </p>
                    )}
                </form>
            </PopoverContent>
        </Popover>
    )
}

function Preset({ label, selected, onSelect }: { label: string; selected: boolean; onSelect: () => void }) {
    return (
        <Button type="button" variant={selected ? "secondary" : "ghost"} size="sm" className="la:justify-start la:font-normal la:aria-pressed:font-semibold" aria-pressed={selected} onClick={onSelect}>
            {label}
        </Button>
    )
}

function draftFor(value: TimeRange): Draft {
    const now = Date.now()
    const resolved = resolveRange(value, now)
    const from = new Date(resolved.fromMs ?? now - 3_600_000)
    const to = new Date(resolved.toMs ?? now)
    return { dates: { from: startOfDay(from), to: startOfDay(to) }, fromTime: clock(from), toTime: clock(to) }
}

function absoluteRange(draft: Draft): TimeRange | null {
    const from = draft.dates?.from
    const to = draft.dates?.to ?? from
    if (!from || !to) return null
    const fromMs = withClock(from, draft.fromTime)
    const toMs = withClock(to, draft.toTime)
    return Number.isFinite(fromMs) && Number.isFinite(toMs) && fromMs < toMs ? { kind: "absolute", fromMs, toMs } : null
}

function startOfDay(date: Date) {
    return new Date(date.getFullYear(), date.getMonth(), date.getDate())
}

function clock(date: Date) {
    return `${String(date.getHours()).padStart(2, "0")}:${String(date.getMinutes()).padStart(2, "0")}`
}

function withClock(day: Date, time: string) {
    const [hours, minutes] = time.split(":").map(Number)
    return new Date(day.getFullYear(), day.getMonth(), day.getDate(), hours ?? 0, minutes ?? 0).getTime()
}
