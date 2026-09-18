import type * as React from "react"

import { cn } from "../../lib/utils.js"

function Input({ className, type, ...props }: React.ComponentProps<"input">) {
    return (
        <input
            type={type}
            data-slot="input"
            className={cn(
                "la:flex la:h-9 la:w-full la:min-w-0 la:rounded-md la:border la:border-input la:bg-card la:px-3 la:py-1 la:text-sm la:text-foreground la:outline-none la:transition-colors la:placeholder:text-muted-foreground la:focus-visible:border-ring la:focus-visible:ring-2 la:focus-visible:ring-ring/20 la:disabled:cursor-not-allowed la:disabled:opacity-50 la:max-md:min-h-11",
                className
            )}
            {...props}
        />
    )
}
export { Input }
