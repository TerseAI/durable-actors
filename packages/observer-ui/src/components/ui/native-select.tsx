import * as React from "react"

import { ChevronDownIcon } from "lucide-react"

import { cn } from "../../lib/utils.js"

function NativeSelect({ className, size = "default", ...props }: Omit<React.ComponentProps<"select">, "size"> & { size?: "sm" | "default" }) {
    return (
        <div className="la:group/native-select la:relative la:w-fit la:has-[select:disabled]:opacity-50" data-slot="native-select-wrapper">
            <select
                data-slot="native-select"
                data-size={size}
                className={cn(
                    "la:h-9 la:w-full la:min-w-0 la:appearance-none la:rounded-md la:border la:border-input la:bg-transparent la:px-3 la:py-2 la:pr-9 la:text-sm la:shadow-xs la:outline-none la:transition-[color,box-shadow] la:selection:bg-primary la:selection:text-primary-foreground la:disabled:pointer-events-none la:disabled:cursor-not-allowed la:data-[size=sm]:h-8 la:data-[size=sm]:py-1 la:focus-visible:border-ring la:focus-visible:ring-[3px] la:focus-visible:ring-ring/50 la:aria-invalid:border-destructive la:aria-invalid:ring-destructive/20 la:dark:bg-input/30 la:dark:hover:bg-input/50 la:dark:aria-invalid:ring-destructive/40",
                    className
                )}
                {...props}
            />
            <ChevronDownIcon
                className="la:pointer-events-none la:absolute la:top-1/2 la:right-3.5 la:size-4 la:-translate-y-1/2 la:text-muted-foreground la:opacity-50 la:select-none"
                aria-hidden="true"
                data-slot="native-select-icon"
            />
        </div>
    )
}

function NativeSelectOption({ className, ...props }: React.ComponentProps<"option">) {
    return <option data-slot="native-select-option" className={cn("la:bg-[Canvas] la:text-[CanvasText]", className)} {...props} />
}

function NativeSelectOptGroup({ className, ...props }: React.ComponentProps<"optgroup">) {
    return <optgroup data-slot="native-select-optgroup" className={cn("la:bg-[Canvas] la:text-[CanvasText]", className)} {...props} />
}

export { NativeSelect, NativeSelectOptGroup, NativeSelectOption }
