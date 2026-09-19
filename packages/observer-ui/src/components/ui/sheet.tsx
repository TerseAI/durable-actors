import * as React from "react"

import * as SheetPrimitive from "@radix-ui/react-dialog"
import { X } from "lucide-react"

import { cn } from "../../lib/utils.js"

import { Button } from "./button.js"

const Sheet = SheetPrimitive.Root
const SheetTitle = SheetPrimitive.Title
const SheetDescription = SheetPrimitive.Description

function SheetContent({ className, children, container, ...props }: React.ComponentProps<typeof SheetPrimitive.Content> & { container?: HTMLElement | null }) {
    return (
        <SheetPrimitive.Portal container={container}>
            <SheetPrimitive.Overlay className="la-sheet-overlay" />
            <SheetPrimitive.Content data-slot="sheet-content" className={cn("la-observer la-sheet", className)} {...props}>
                {children}
                <SheetPrimitive.Close asChild>
                    <Button className="la-sheet-close" variant="ghost" size="icon" aria-label="Close">
                        <X aria-hidden="true" />
                    </Button>
                </SheetPrimitive.Close>
            </SheetPrimitive.Content>
        </SheetPrimitive.Portal>
    )
}

export { Sheet, SheetContent, SheetTitle, SheetDescription }
