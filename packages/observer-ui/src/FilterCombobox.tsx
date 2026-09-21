import { useState } from "react"

import { Command, CommandGroup, CommandInput, CommandItem, CommandList } from "./components/ui/command.js"

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

// A free-text filter whose suggestions come from what is on screen; cmdk owns the listbox keyboard model.
export function FilterCombobox({ label, placeholder, value, onChange, suggestions, limit = 8 }: FilterComboboxProps) {
    const [open, setOpen] = useState(false)
    const matches = matchSuggestions(suggestions, value, limit)
    const groups = matches.reduce((grouped, suggestion) => grouped.set(suggestion.group, [...(grouped.get(suggestion.group) ?? []), suggestion]), new Map<string, FilterSuggestion[]>())
    const expanded = open && matches.length > 0
    return (
        <Command
            label={label}
            shouldFilter={false}
            loop
            className="la-combobox la:relative la:overflow-visible la:bg-transparent"
            onKeyDown={event => {
                if (event.key === "Escape") setOpen(false)
                if (event.key === "ArrowDown") setOpen(true)
            }}
        >
            <CommandInput
                placeholder={placeholder}
                value={value}
                onValueChange={next => {
                    onChange(next)
                    setOpen(true)
                }}
                onFocus={() => setOpen(true)}
                onBlur={() => setOpen(false)}
            />
            {expanded && (
                <CommandList
                    label={`${label} suggestions`}
                    className="la:absolute la:top-full la:right-0 la:left-0 la:z-20 la:mt-1.5 la:rounded-md la:border la:bg-popover la:text-popover-foreground la:shadow-md"
                    onMouseDown={event => event.preventDefault()}
                >
                    {[...groups].map(([group, items]) => (
                        <CommandGroup key={group} heading={group}>
                            {items.map(suggestion => (
                                <CommandItem
                                    key={suggestion.value}
                                    value={`${group}:${suggestion.value}`}
                                    onSelect={() => {
                                        onChange(suggestion.value)
                                        setOpen(false)
                                    }}
                                >
                                    <span className="la:truncate la:font-mono la:text-xs">{suggestion.value}</span>
                                    {suggestion.hint && <span className="la:ml-auto la:text-xs la:text-muted-foreground">{suggestion.hint}</span>}
                                </CommandItem>
                            ))}
                        </CommandGroup>
                    ))}
                </CommandList>
            )}
        </Command>
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
