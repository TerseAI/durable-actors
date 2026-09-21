import * as React from "react"
import { type DayButton, DayPicker, getDefaultClassNames } from "react-day-picker"

import { ChevronDownIcon, ChevronLeftIcon, ChevronRightIcon } from "lucide-react"

import { cn } from "../../lib/utils.js"

import { Button, buttonVariants } from "./button.js"

function Calendar({
    className,
    classNames,
    showOutsideDays = true,
    captionLayout = "label",
    buttonVariant = "ghost",
    formatters,
    components,
    ...props
}: React.ComponentProps<typeof DayPicker> & {
    buttonVariant?: React.ComponentProps<typeof Button>["variant"]
}) {
    const defaultClassNames = getDefaultClassNames()

    return (
        <DayPicker
            showOutsideDays={showOutsideDays}
            className={cn(
                "la:group/calendar la:bg-background la:p-3 la:[--cell-size:--spacing(8)] la:[[data-slot=card-content]_&]:bg-transparent la:[[data-slot=popover-content]_&]:bg-transparent",
                className
            )}
            captionLayout={captionLayout}
            formatters={{
                formatMonthDropdown: date => date.toLocaleString("default", { month: "short" }),
                ...formatters
            }}
            classNames={{
                root: cn("la:w-fit", defaultClassNames.root),
                months: cn("la:relative la:flex la:flex-col la:gap-4 la:md:flex-row", defaultClassNames.months),
                month: cn("la:flex la:w-full la:flex-col la:gap-4", defaultClassNames.month),
                nav: cn("la:absolute la:inset-x-0 la:top-0 la:flex la:w-full la:items-center la:justify-between la:gap-1", defaultClassNames.nav),
                button_previous: cn(buttonVariants({ variant: buttonVariant }), "la:size-(--cell-size) la:p-0 la:select-none la:aria-disabled:opacity-50", defaultClassNames.button_previous),
                button_next: cn(buttonVariants({ variant: buttonVariant }), "la:size-(--cell-size) la:p-0 la:select-none la:aria-disabled:opacity-50", defaultClassNames.button_next),
                month_caption: cn("la:flex la:h-(--cell-size) la:w-full la:items-center la:justify-center la:px-(--cell-size)", defaultClassNames.month_caption),
                dropdowns: cn("la:flex la:h-(--cell-size) la:w-full la:items-center la:justify-center la:gap-1.5 la:text-sm la:font-medium", defaultClassNames.dropdowns),
                dropdown_root: cn(
                    "la:relative la:rounded-md la:border la:border-input la:shadow-xs la:has-focus:border-ring la:has-focus:ring-[3px] la:has-focus:ring-ring/50",
                    defaultClassNames.dropdown_root
                ),
                dropdown: cn("la:absolute la:inset-0 la:bg-popover la:opacity-0", defaultClassNames.dropdown),
                caption_label: cn(
                    "la:font-medium la:select-none",
                    captionLayout === "label" ? "la:text-sm" : "la:flex la:h-8 la:items-center la:gap-1 la:rounded-md la:pr-1 la:pl-2 la:text-sm la:[&>svg]:size-3.5 la:[&>svg]:text-muted-foreground",
                    defaultClassNames.caption_label
                ),
                month_grid: cn("la:w-full la:border-collapse", defaultClassNames.month_grid),
                weekdays: cn("la:flex", defaultClassNames.weekdays),
                weekday: cn("la:flex-1 la:rounded-md la:text-[0.8rem] la:font-normal la:text-muted-foreground la:select-none", defaultClassNames.weekday),
                week: cn("la:mt-2 la:flex la:w-full", defaultClassNames.week),
                week_number_header: cn("la:w-(--cell-size) la:select-none", defaultClassNames.week_number_header),
                week_number: cn("la:text-[0.8rem] la:text-muted-foreground la:select-none", defaultClassNames.week_number),
                day: cn(
                    "la:group/day la:relative la:aspect-square la:h-full la:w-full la:p-0 la:text-center la:select-none la:[&:last-child[data-selected=true]_button]:rounded-r-md",
                    props.showWeekNumber ? "la:[&:nth-child(2)[data-selected=true]_button]:rounded-l-md" : "la:[&:first-child[data-selected=true]_button]:rounded-l-md",
                    defaultClassNames.day
                ),
                range_start: cn("la:rounded-l-md la:bg-accent", defaultClassNames.range_start),
                range_middle: cn("la:rounded-none", defaultClassNames.range_middle),
                range_end: cn("la:rounded-r-md la:bg-accent", defaultClassNames.range_end),
                today: cn("la:rounded-md la:bg-accent la:text-accent-foreground la:data-[selected=true]:rounded-none", defaultClassNames.today),
                outside: cn("la:text-muted-foreground la:aria-selected:text-muted-foreground", defaultClassNames.outside),
                disabled: cn("la:text-muted-foreground la:opacity-50", defaultClassNames.disabled),
                hidden: cn("la:invisible", defaultClassNames.hidden),
                ...classNames
            }}
            components={{
                Root: ({ className, rootRef, ...props }) => {
                    return <div data-slot="calendar" ref={rootRef} className={cn(className)} {...props} />
                },
                Chevron: ({ className, orientation, ...props }) => {
                    if (orientation === "left") {
                        return <ChevronLeftIcon className={cn("la:size-4", className)} {...props} />
                    }

                    if (orientation === "right") {
                        return <ChevronRightIcon className={cn("la:size-4", className)} {...props} />
                    }

                    return <ChevronDownIcon className={cn("la:size-4", className)} {...props} />
                },
                DayButton: CalendarDayButton,
                WeekNumber: ({ children, ...props }) => {
                    return (
                        <td {...props}>
                            <div className="la:flex la:size-(--cell-size) la:items-center la:justify-center la:text-center">{children}</div>
                        </td>
                    )
                },
                ...components
            }}
            {...props}
        />
    )
}

function CalendarDayButton({ className, day, modifiers, ...props }: React.ComponentProps<typeof DayButton>) {
    const defaultClassNames = getDefaultClassNames()

    const ref = React.useRef<HTMLButtonElement>(null)
    React.useEffect(() => {
        if (modifiers.focused) ref.current?.focus()
    }, [modifiers.focused])

    return (
        <Button
            ref={ref}
            variant="ghost"
            size="icon"
            data-day={day.date.toLocaleDateString()}
            data-selected-single={modifiers.selected && !modifiers.range_start && !modifiers.range_end && !modifiers.range_middle}
            data-range-start={modifiers.range_start}
            data-range-end={modifiers.range_end}
            data-range-middle={modifiers.range_middle}
            className={cn(
                "la:flex la:aspect-square la:size-auto la:w-full la:min-w-(--cell-size) la:flex-col la:gap-1 la:leading-none la:font-normal la:group-data-[focused=true]/day:relative la:group-data-[focused=true]/day:z-10 la:group-data-[focused=true]/day:border-ring la:group-data-[focused=true]/day:ring-[3px] la:group-data-[focused=true]/day:ring-ring/50 la:data-[range-end=true]:rounded-md la:data-[range-end=true]:rounded-r-md la:data-[range-end=true]:bg-primary la:data-[range-end=true]:text-primary-foreground la:data-[range-middle=true]:rounded-none la:data-[range-middle=true]:bg-accent la:data-[range-middle=true]:text-accent-foreground la:data-[range-start=true]:rounded-md la:data-[range-start=true]:rounded-l-md la:data-[range-start=true]:bg-primary la:data-[range-start=true]:text-primary-foreground la:data-[selected-single=true]:bg-primary la:data-[selected-single=true]:text-primary-foreground la:dark:hover:text-accent-foreground la:[&>span]:text-xs la:[&>span]:opacity-70",
                defaultClassNames.day,
                className
            )}
            {...props}
        />
    )
}

export { Calendar, CalendarDayButton }
