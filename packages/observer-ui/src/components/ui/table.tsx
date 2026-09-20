import * as React from "react"

import { cn } from "../../lib/utils.js"

function Table({ className, ...props }: React.ComponentProps<"table">) {
    return (
        <div data-slot="table-container" className="la:relative la:w-full la:overflow-x-auto">
            <table data-slot="table" className={cn("la:w-full la:caption-bottom la:text-sm la:tabular-nums", className)} {...props} />
        </div>
    )
}

function TableHeader({ className, ...props }: React.ComponentProps<"thead">) {
    return <thead data-slot="table-header" className={cn("la:[&_tr]:border-b", className)} {...props} />
}

function TableBody({ className, ...props }: React.ComponentProps<"tbody">) {
    return <tbody data-slot="table-body" className={cn("la:[&_tr:last-child]:border-0", className)} {...props} />
}

function TableFooter({ className, ...props }: React.ComponentProps<"tfoot">) {
    return <tfoot data-slot="table-footer" className={cn("la:bg-muted/50 la:border-t la:font-medium la:[&>tr]:last:border-b-0", className)} {...props} />
}

function TableRow({ className, ...props }: React.ComponentProps<"tr">) {
    return <tr data-slot="table-row" className={cn("la:border-b la:transition-colors la:duration-150 la:hover:bg-muted/60 la:data-[state=selected]:bg-muted", className)} {...props} />
}

function TableHead({ className, ...props }: React.ComponentProps<"th">) {
    return (
        <th
            data-slot="table-head"
            className={cn(
                "la:h-10 la:whitespace-nowrap la:px-3 la:text-left la:align-middle la:text-xs la:font-medium la:text-muted-foreground la:[&:has([role=checkbox])]:pr-0 la:[&>[role=checkbox]]:translate-y-[2px]",
                className
            )}
            {...props}
        />
    )
}

function TableCell({ className, ...props }: React.ComponentProps<"td">) {
    return (
        <td
            data-slot="table-cell"
            className={cn("la:whitespace-nowrap la:px-3 la:py-2.5 la:align-middle la:[&:has([role=checkbox])]:pr-0 la:[&>[role=checkbox]]:translate-y-[2px]", className)}
            {...props}
        />
    )
}

function TableCaption({ className, ...props }: React.ComponentProps<"caption">) {
    return <caption data-slot="table-caption" className={cn("la:text-muted-foreground la:mt-4 la:text-sm", className)} {...props} />
}

export { Table, TableHeader, TableBody, TableFooter, TableHead, TableRow, TableCell, TableCaption }
