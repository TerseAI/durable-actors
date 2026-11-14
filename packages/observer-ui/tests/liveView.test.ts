import { expect, test } from "bun:test"
import { JSDOM } from "jsdom"

import type { ObserveEvent } from "../src/types/Events.js"

const { window } = new JSDOM("<!doctype html><html><body></body></html>", { pretendToBeVisual: true })
Object.assign(globalThis, { window, document: window.document, navigator: window.navigator, IS_REACT_ACT_ENVIRONMENT: true })
const { act, renderHook, waitFor } = await import("@testing-library/react")
const { EventBus } = await import("../src/client/events.js")
const { useLiveView } = await import("../src/hooks/useLiveView.js")

// SWR re-renders only for the fields a render reads, as the app's hooks read these.
function useView(...args: Parameters<typeof useLiveView<string[]>>) {
    const { data, error } = useLiveView(...args)
    return { data, error }
}

const alive = (gatewayId: string) => ({ type: "gatewayAlive", item: { gatewayId } }) as unknown as ObserveEvent
const names = (data: string[], events: ObserveEvent[]) => [...data, ...events.map(event => (event.item as { gatewayId: string }).gatewayId)]

test("a read that events overlap still lands, with those events applied", async () => {
    const bus = new EventBus()
    let finish = (_: string[]) => {}
    const read = () => new Promise<string[]>(resolve => (finish = resolve))
    const { result } = renderHook(() => useView(["overlapped"], read, names, bus))
    act(() => bus.publish([alive("during")]))
    await act(async () => finish(["snapshot"]))
    await waitFor(() => expect(result.current.data).toEqual(["snapshot", "during"]))
    act(() => bus.publish([alive("after")]))
    expect(result.current.data).toEqual(["snapshot", "during", "after"])
})
