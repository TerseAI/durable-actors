import * as React from "react"

import * as PopoverPrimitive from "@radix-ui/react-popover"

import { cn } from "../../lib/utils.js"

function Popover({ ...props }: React.ComponentProps<typeof PopoverPrimitive.Root>) {
    return <PopoverPrimitive.Root data-slot="popover" {...props} />
}

function PopoverTrigger({ ...props }: React.ComponentProps<typeof PopoverPrimitive.Trigger>) {
    return <PopoverPrimitive.Trigger data-slot="popover-trigger" {...props} />
}

function PopoverContent({ className, align = "center", sideOffset = 4, ...props }: React.ComponentProps<typeof PopoverPrimitive.Content>) {
    return (
        <PopoverPrimitive.Portal>
            <PopoverPrimitive.Content
                data-slot="popover-content"
                align={align}
                sideOffset={sideOffset}
                className={cn(
                    "la:z-50 la:w-72 la:origin-(--radix-popover-content-transform-origin) la:rounded-md la:border la:bg-popover la:p-4 la:text-popover-foreground la:shadow-md la:outline-hidden",
                    className
                )}
                {...props}
            />
        </PopoverPrimitive.Portal>
    )
}

function PopoverAnchor({ ...props }: React.ComponentProps<typeof PopoverPrimitive.Anchor>) {
    return <PopoverPrimitive.Anchor data-slot="popover-anchor" {...props} />
}

function PopoverHeader({ className, ...props }: React.ComponentProps<"div">) {
    return <div data-slot="popover-header" className={cn("la:flex la:flex-col la:gap-1 la:text-sm", className)} {...props} />
}

function PopoverTitle({ className, ...props }: React.ComponentProps<"h2">) {
    return <div data-slot="popover-title" className={cn("la:font-medium", className)} {...props} />
}

function PopoverDescription({ className, ...props }: React.ComponentProps<"p">) {
    return <p data-slot="popover-description" className={cn("la:text-muted-foreground", className)} {...props} />
}

export { Popover, PopoverTrigger, PopoverContent, PopoverAnchor, PopoverHeader, PopoverTitle, PopoverDescription }
