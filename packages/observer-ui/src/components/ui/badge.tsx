import type * as React from "react"

import { type VariantProps, cva } from "class-variance-authority"

import { cn } from "../../lib/utils.js"

const badgeVariants = cva("la:inline-flex la:items-center la:justify-center la:gap-1.5 la:rounded-md la:border la:px-2 la:py-0.5 la:text-xs la:font-medium la:whitespace-nowrap", {
    variants: { variant: { default: "la:border-transparent la:bg-muted la:text-muted-foreground", outline: "la:border-border la:text-muted-foreground" } },
    defaultVariants: { variant: "default" }
})
function Badge({ className, variant, ...props }: React.ComponentProps<"span"> & VariantProps<typeof badgeVariants>) {
    return <span data-slot="badge" className={cn(badgeVariants({ variant }), className)} {...props} />
}
export { Badge, badgeVariants }
