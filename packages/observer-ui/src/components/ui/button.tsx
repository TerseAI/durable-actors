import * as React from "react"

import { Slot } from "@radix-ui/react-slot"
import { type VariantProps, cva } from "class-variance-authority"

import { cn } from "../../lib/utils.js"

const buttonVariants = cva(
    "la:inline-flex la:shrink-0 la:items-center la:justify-center la:gap-2 la:whitespace-nowrap la:rounded-md la:text-sm la:font-medium la:outline-none la:transition-[background-color,border-color,color,box-shadow,transform] la:duration-150 la:ease-out la:active:translate-y-px la:disabled:pointer-events-none la:disabled:translate-y-0 la:disabled:opacity-45 la:focus-visible:ring-2 la:focus-visible:ring-ring la:focus-visible:ring-offset-2 la:focus-visible:ring-offset-background la:aria-invalid:border-destructive la:aria-invalid:ring-destructive/25 la:[&_svg]:pointer-events-none la:[&_svg]:shrink-0 la:[&_svg:not([class*='size-'])]:size-4",
    {
        variants: {
            variant: {
                default: "la:bg-primary la:text-primary-foreground la:shadow-[var(--shadow-control)] la:hover:bg-primary/88",
                destructive: "la:bg-destructive la:text-white la:hover:bg-destructive/90 la:focus-visible:ring-destructive/20 la:dark:focus-visible:ring-destructive/40 la:dark:bg-destructive/60",
                outline: "la:border la:border-input la:bg-card la:hover:border-foreground/20 la:hover:bg-accent la:hover:text-accent-foreground la:dark:bg-card",
                secondary: "la:bg-secondary la:text-secondary-foreground la:hover:bg-secondary/80",
                ghost: "la:hover:bg-accent la:hover:text-accent-foreground la:dark:hover:bg-accent/50",
                link: "la:text-primary la:underline-offset-4 la:hover:underline"
            },
            size: {
                default: "la:h-9 la:px-4 la:py-2 la:max-md:min-h-11 la:has-[>svg]:px-3",
                sm: "la:h-8 la:gap-1.5 la:px-3 la:max-md:min-h-11 la:has-[>svg]:px-2.5",
                lg: "la:h-10 la:px-5 la:max-md:min-h-11 la:has-[>svg]:px-4",
                icon: "la:size-9 la:max-md:size-11",
                "icon-sm": "la:size-8 la:max-md:size-11",
                "icon-lg": "la:size-10 la:max-md:size-11"
            }
        },
        defaultVariants: {
            variant: "default",
            size: "default"
        }
    }
)

function Button({
    className,
    variant,
    size,
    asChild = false,
    ...props
}: React.ComponentProps<"button"> &
    VariantProps<typeof buttonVariants> & {
        asChild?: boolean
    }) {
    const Comp = asChild ? Slot : "button"

    return <Comp data-slot="button" className={cn(buttonVariants({ variant, size, className }))} {...props} />
}

export { Button, buttonVariants }
