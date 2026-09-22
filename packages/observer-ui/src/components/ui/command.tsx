import * as React from "react"

import { Command as CommandPrimitive } from "cmdk"
import { SearchIcon } from "lucide-react"

import { cn } from "../../lib/utils.js"

function Command({ className, ...props }: React.ComponentProps<typeof CommandPrimitive>) {
    return (
        <CommandPrimitive
            data-slot="command"
            className={cn("la:flex la:h-full la:w-full la:flex-col la:overflow-hidden la:rounded-md la:bg-popover la:text-popover-foreground", className)}
            {...props}
        />
    )
}

function CommandInput({ className, ...props }: React.ComponentProps<typeof CommandPrimitive.Input>) {
    return (
        <div data-slot="command-input-wrapper" className="la:flex la:h-9 la:items-center la:gap-2 la:border-b la:px-3">
            <SearchIcon className="la:size-4 la:shrink-0 la:opacity-50" />
            <CommandPrimitive.Input
                data-slot="command-input"
                className={cn(
                    "la:flex la:h-10 la:w-full la:rounded-md la:bg-transparent la:py-3 la:text-sm la:outline-hidden la:placeholder:text-muted-foreground la:disabled:cursor-not-allowed la:disabled:opacity-50",
                    className
                )}
                {...props}
            />
        </div>
    )
}

function CommandList({ className, ...props }: React.ComponentProps<typeof CommandPrimitive.List>) {
    return <CommandPrimitive.List data-slot="command-list" className={cn("la:max-h-[300px] la:scroll-py-1 la:overflow-x-hidden la:overflow-y-auto", className)} {...props} />
}

function CommandEmpty({ ...props }: React.ComponentProps<typeof CommandPrimitive.Empty>) {
    return <CommandPrimitive.Empty data-slot="command-empty" className="la:py-6 la:text-center la:text-sm" {...props} />
}

function CommandGroup({ className, ...props }: React.ComponentProps<typeof CommandPrimitive.Group>) {
    return (
        <CommandPrimitive.Group
            data-slot="command-group"
            className={cn(
                "la:overflow-hidden la:p-1 la:text-foreground la:[&_[cmdk-group-heading]]:px-2 la:[&_[cmdk-group-heading]]:py-1.5 la:[&_[cmdk-group-heading]]:text-xs la:[&_[cmdk-group-heading]]:font-medium la:[&_[cmdk-group-heading]]:text-muted-foreground",
                className
            )}
            {...props}
        />
    )
}

function CommandSeparator({ className, ...props }: React.ComponentProps<typeof CommandPrimitive.Separator>) {
    return <CommandPrimitive.Separator data-slot="command-separator" className={cn("la:-mx-1 la:h-px la:bg-border", className)} {...props} />
}

function CommandItem({ className, ...props }: React.ComponentProps<typeof CommandPrimitive.Item>) {
    return (
        <CommandPrimitive.Item
            data-slot="command-item"
            className={cn(
                "la:relative la:flex la:cursor-default la:items-center la:gap-2 la:rounded-sm la:px-2 la:py-1.5 la:text-sm la:outline-hidden la:select-none la:data-[disabled=true]:pointer-events-none la:data-[disabled=true]:opacity-50 la:data-[selected=true]:bg-accent la:data-[selected=true]:text-accent-foreground la:[&_svg]:pointer-events-none la:[&_svg]:shrink-0 la:[&_svg:not([class*='size-'])]:size-4 la:[&_svg:not([class*='text-'])]:text-muted-foreground",
                className
            )}
            {...props}
        />
    )
}

function CommandShortcut({ className, ...props }: React.ComponentProps<"span">) {
    return <span data-slot="command-shortcut" className={cn("la:ml-auto la:text-xs la:tracking-widest la:text-muted-foreground", className)} {...props} />
}

export { Command, CommandInput, CommandList, CommandEmpty, CommandGroup, CommandItem, CommandShortcut, CommandSeparator }
