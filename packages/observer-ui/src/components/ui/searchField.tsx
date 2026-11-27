import { Search, X } from "lucide-react"

export type SearchFieldProps = {
    value: string
    onChange: (value: string) => void
    placeholder: string
    label: string
}

export function SearchField({ value, onChange, placeholder, label }: SearchFieldProps) {
    return (
        <label className="flex h-8 w-64 items-center gap-2 rounded-lg border bg-background/40 px-2.5 text-[12.5px] focus-within:border-ring focus-within:ring-2 focus-within:ring-ring/40">
            <Search aria-hidden className="size-3.5 shrink-0 text-muted-foreground" />
            <input
                value={value}
                maxLength={256}
                placeholder={placeholder}
                aria-label={label}
                onChange={event => onChange(event.target.value)}
                onKeyDown={event => {
                    if (event.key === "Escape") onChange("")
                }}
                className="min-w-0 flex-1 bg-transparent text-foreground outline-none placeholder:text-muted-foreground"
            />
            {value && (
                <button
                    type="button"
                    aria-label="Clear search"
                    onClick={() => onChange("")}
                    className="grid size-4.5 shrink-0 place-items-center rounded-full bg-muted text-muted-foreground hover:text-foreground"
                >
                    <X className="size-2.5" />
                </button>
            )}
        </label>
    )
}
