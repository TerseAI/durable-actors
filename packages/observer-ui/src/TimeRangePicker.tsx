import { useEffect, useId, useRef, useState } from "react"

import { Calendar, Check, ChevronDown } from "lucide-react"

import { Button } from "./components/ui/button.js"
import { Input } from "./components/ui/input.js"
import { rangeLabel, relativePresets, resolveRange, sameRange, toLocalInput } from "./time-range.js"
import type { TimeRange } from "./time-range.js"

interface TimeRangePickerProps {
    value: TimeRange
    onChange: (range: TimeRange) => void
    allowAll?: boolean
    className?: string
}

export function TimeRangePicker({ value, onChange, allowAll = true, className = "" }: TimeRangePickerProps) {
    const [open, setOpen] = useState(false)
    const [invalid, setInvalid] = useState(false)
    const root = useRef<HTMLDivElement>(null)
    const trigger = useRef<HTMLButtonElement>(null)
    const panelId = useId()
    useEffect(() => {
        if (!open) return
        const close = (event: MouseEvent) => {
            if (!root.current?.contains(event.target as Node)) setOpen(false)
        }
        document.addEventListener("mousedown", close)
        root.current?.querySelector<HTMLElement>("[aria-pressed='true']")?.focus()
        return () => document.removeEventListener("mousedown", close)
    }, [open])
    const choose = (range: TimeRange) => {
        onChange(range)
        setOpen(false)
        setInvalid(false)
        trigger.current?.focus()
    }
    const resolved = resolveRange(value, Date.now())
    const custom = { from: toLocalInput(resolved.fromMs ?? Date.now() - 3_600_000), to: toLocalInput(resolved.toMs ?? Date.now()) }
    return (
        <div ref={root} className={`la-range ${className}`.trim()}>
            <Button
                ref={trigger}
                type="button"
                variant="outline"
                className="la-range-trigger"
                aria-haspopup="dialog"
                aria-expanded={open}
                aria-controls={open ? panelId : undefined}
                onClick={() => setOpen(current => !current)}
            >
                <Calendar aria-hidden="true" />
                <span>{rangeLabel(value)}</span>
                <ChevronDown aria-hidden="true" className="la-range-chevron" />
            </Button>
            {open && (
                <div
                    id={panelId}
                    role="dialog"
                    aria-label="Time range"
                    className="la-range-panel"
                    onKeyDown={event => {
                        if (event.key === "Escape") {
                            event.stopPropagation()
                            setOpen(false)
                            trigger.current?.focus()
                        }
                    }}
                >
                    <div className="la-range-presets" role="group" aria-label="Preset ranges">
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
                        className="la-range-custom"
                        onSubmit={event => {
                            event.preventDefault()
                            const data = new FormData(event.currentTarget)
                            const fromMs = new Date(String(data.get("from"))).getTime()
                            const toMs = new Date(String(data.get("to"))).getTime()
                            if (!Number.isFinite(fromMs) || !Number.isFinite(toMs) || fromMs >= toMs) {
                                setInvalid(true)
                                return
                            }
                            choose({ kind: "absolute", fromMs, toMs })
                        }}
                    >
                        <strong>Custom range</strong>
                        <label>
                            From
                            <Input type="datetime-local" name="from" step={60} required defaultValue={custom.from} aria-invalid={invalid || undefined} />
                        </label>
                        <label>
                            To
                            <Input type="datetime-local" name="to" step={60} required defaultValue={custom.to} aria-invalid={invalid || undefined} />
                        </label>
                        {invalid && (
                            <p className="la-range-error" role="alert">
                                From must be before To.
                            </p>
                        )}
                        <Button type="submit" variant="outline" size="sm">
                            Apply range
                        </Button>
                    </form>
                </div>
            )}
        </div>
    )
}

function Preset({ label, selected, onSelect }: { label: string; selected: boolean; onSelect: () => void }) {
    return (
        <button type="button" className="la-range-preset" aria-pressed={selected} onClick={onSelect}>
            <Check aria-hidden="true" />
            {label}
        </button>
    )
}
