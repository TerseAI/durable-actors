import { useId, useState } from "react"

import { Search } from "lucide-react"

import { Input } from "./components/ui/input.js"

export interface FilterSuggestion {
    group: string
    value: string
    hint?: string
}

interface FilterComboboxProps {
    label: string
    placeholder: string
    value: string
    onChange: (value: string) => void
    suggestions: FilterSuggestion[]
    limit?: number
}

export function FilterCombobox({ label, placeholder, value, onChange, suggestions, limit = 8 }: FilterComboboxProps) {
    const [open, setOpen] = useState(false)
    const [active, setActive] = useState(0)
    const listId = useId()
    const matches = matchSuggestions(suggestions, value, limit)
    const expanded = open && matches.length > 0
    const choose = (suggestion: FilterSuggestion) => {
        onChange(suggestion.value)
        setOpen(false)
    }
    return (
        <div className="la-combobox">
            <div className="la-observer-search la-combobox-field">
                <Search aria-hidden="true" />
                <Input
                    type="search"
                    role="combobox"
                    aria-label={label}
                    aria-autocomplete="list"
                    aria-expanded={expanded}
                    aria-controls={expanded ? listId : undefined}
                    aria-activedescendant={expanded ? `${listId}-${active}` : undefined}
                    placeholder={placeholder}
                    value={value}
                    onInput={event => {
                        onChange(event.currentTarget.value)
                        setOpen(true)
                        setActive(0)
                    }}
                    onFocus={() => setOpen(true)}
                    onBlur={() => setOpen(false)}
                    onKeyDown={event => {
                        if (!expanded) {
                            if (event.key === "ArrowDown") setOpen(true)
                            return
                        }
                        if (event.key === "ArrowDown") {
                            event.preventDefault()
                            setActive(index => (index + 1) % matches.length)
                        } else if (event.key === "ArrowUp") {
                            event.preventDefault()
                            setActive(index => (index - 1 + matches.length) % matches.length)
                        } else if (event.key === "Enter") {
                            event.preventDefault()
                            choose(matches[active]!)
                        } else if (event.key === "Escape") {
                            event.preventDefault()
                            setOpen(false)
                        }
                    }}
                />
            </div>
            {expanded && (
                <ul id={listId} role="listbox" aria-label={`${label} suggestions`} className="la-combobox-list">
                    {matches.map((suggestion, index) => (
                        <li
                            key={`${suggestion.group}:${suggestion.value}`}
                            id={`${listId}-${index}`}
                            role="option"
                            aria-selected={index === active}
                            className="la-combobox-option"
                            onMouseDown={event => event.preventDefault()}
                            onMouseEnter={() => setActive(index)}
                            onClick={() => choose(suggestion)}
                        >
                            <span className="la-combobox-group">{suggestion.group}</span>
                            <span className="la-combobox-value">{suggestion.value}</span>
                            {suggestion.hint && <span className="la-combobox-hint">{suggestion.hint}</span>}
                        </li>
                    ))}
                </ul>
            )}
        </div>
    )
}

export function matchSuggestions(suggestions: FilterSuggestion[], value: string, limit: number): FilterSuggestion[] {
    const needle = value.trim().toLocaleLowerCase()
    const seen = new Set<string>()
    const matches: FilterSuggestion[] = []
    for (const suggestion of suggestions) {
        const key = `${suggestion.group}:${suggestion.value}`
        const text = suggestion.value.toLocaleLowerCase()
        if (seen.has(key) || text === needle || (needle && !text.includes(needle))) continue
        seen.add(key)
        matches.push(suggestion)
    }
    // Prefix matches read as the most likely intent, so they lead.
    return matches.sort((a, b) => Number(b.value.toLocaleLowerCase().startsWith(needle)) - Number(a.value.toLocaleLowerCase().startsWith(needle))).slice(0, limit)
}
