import { useEffect, useLayoutEffect, useRef } from "react"
import type { RefObject } from "react"

import type { TimelineWindow } from "./TimelineZoom.js"

export function useTimelinePinch(viewport: RefObject<HTMLDivElement | null>, bounds: TimelineWindow, range: TimelineWindow, onChange: (range: TimelineWindow) => void) {
    const current = useRef(range)
    const gestureScale = useRef<number | null>(null)
    useLayoutEffect(() => {
        current.current = range
    }, [range])

    useEffect(() => {
        const element = viewport.current!
        const axis = () => element.querySelector(".request-waterfall-axis > div")!.getBoundingClientRect()
        const zoom = (factor: number, clientX: number) => {
            const { left, width } = axis()
            if (!width) return false
            current.current = anchoredWindow(current.current, bounds, factor, Math.max(0, Math.min(1, (clientX - left) / width)))
            onChange(current.current)
            return true
        }
        const wheel = (event: WheelEvent) => {
            if ((!event.ctrlKey && !event.metaKey) || !event.deltaY) return
            if (gestureScale.current !== null) {
                event.preventDefault()
                return
            }
            const unit = event.deltaMode === 1 ? 16 : event.deltaMode === 2 ? axis().width : 1
            if (zoom(Math.exp(event.deltaY * unit * 0.01), event.clientX)) event.preventDefault()
        }
        const begin = (event: Event) => {
            if (!axis().width) return
            gestureScale.current = 1
            event.preventDefault()
        }
        const change = (event: Event) => {
            const { scale, clientX } = event as Event & { scale: number; clientX: number }
            if (gestureScale.current === null || !Number.isFinite(scale) || scale <= 0) return
            if (zoom(gestureScale.current / scale, clientX)) event.preventDefault()
            gestureScale.current = scale
        }
        const end = (event: Event) => {
            if (gestureScale.current === null) return
            gestureScale.current = null
            event.preventDefault()
        }
        element.addEventListener("wheel", wheel, { passive: false })
        element.addEventListener("gesturestart", begin, { passive: false })
        element.addEventListener("gesturechange", change, { passive: false })
        element.addEventListener("gestureend", end, { passive: false })
        return () => {
            element.removeEventListener("wheel", wheel)
            element.removeEventListener("gesturestart", begin)
            element.removeEventListener("gesturechange", change)
            element.removeEventListener("gestureend", end)
        }
    }, [viewport, bounds.start, bounds.end, onChange])
}

function anchoredWindow(range: TimelineWindow, bounds: TimelineWindow, factor: number, anchor: number): TimelineWindow {
    const span = range.end - range.start
    const width = Math.max(1, Math.min(bounds.end - bounds.start, span * factor))
    const start = Math.max(bounds.start, Math.min(range.start + (span - width) * anchor, bounds.end - width))
    return { start, end: start + width }
}
