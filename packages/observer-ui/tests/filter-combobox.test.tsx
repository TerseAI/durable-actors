import React from "react"

import assert from "node:assert/strict"
import { afterEach, test } from "node:test"

import "./dom.js"

// React DOM must see the jsdom globals when it loads, or it falls back to legacy input polyfills on focus.
const { cleanup, fireEvent, render } = await import("@testing-library/react")
const { FilterCombobox, matchSuggestions } = await import("../src/FilterCombobox.js")
afterEach(cleanup)

const suggestions = [
    { group: "Actor class", value: "ChatRoom", hint: "3 instances" },
    { group: "Instance", value: "general", hint: "ChatRoom" },
    { group: "Instance", value: "ops-general", hint: "ChatRoom" },
    { group: "Instance", value: "design", hint: "ChatRoom" },
    { group: "Instance", value: "general", hint: "ChatRoom" }
]

test("suggestions match by substring, lead with prefixes, drop duplicates and exact matches", () => {
    assert.deepEqual(
        matchSuggestions(suggestions, "gen", 8).map(suggestion => suggestion.value),
        ["general", "ops-general"]
    )
    assert.deepEqual(
        matchSuggestions(suggestions, "", 8).map(suggestion => suggestion.value),
        ["ChatRoom", "general", "ops-general", "design"]
    )
    assert.deepEqual(
        matchSuggestions(suggestions, "general", 8).map(suggestion => suggestion.value),
        ["ops-general"]
    )
    assert.equal(matchSuggestions(suggestions, "", 2).length, 2)
})

test("the combobox lists actor classes and instances and picks one with the keyboard or the mouse", () => {
    let value = ""
    const view = render(<FilterCombobox label="Filter connections" placeholder="Filter…" value={value} onChange={next => (value = next)} suggestions={suggestions} />)
    const input = view.getByRole("combobox", { name: "Filter connections" })
    assert.equal(view.queryByRole("listbox"), null)
    fireEvent.focus(input)
    const list = view.getByRole("listbox", { name: "Filter connections suggestions" })
    assert.deepEqual(
        view.getAllByRole("option").map(option => option.textContent),
        ["ChatRoom3 instances", "generalChatRoom", "ops-generalChatRoom", "designChatRoom"]
    )
    assert.deepEqual(
        [...list.querySelectorAll("[cmdk-group-heading]")].map(heading => heading.textContent),
        ["Actor class", "Instance"],
        "suggestions are grouped under their kind"
    )
    assert.equal(input.getAttribute("aria-expanded"), "true")
    fireEvent.keyDown(input, { key: "ArrowDown" })
    fireEvent.keyDown(input, { key: "ArrowDown" })
    assert.equal(view.getAllByRole("option")[2]!.getAttribute("aria-selected"), "true")
    fireEvent.keyDown(input, { key: "Enter" })
    assert.equal(value, "ops-general")
    view.rerender(<FilterCombobox label="Filter connections" placeholder="Filter…" value={value} onChange={next => (value = next)} suggestions={suggestions} />)
    assert.equal(view.queryByRole("listbox"), null)
    assert.ok(list)
    fireEvent.change(input, { target: { value: "des" } })
    assert.equal(value, "des")
    view.rerender(<FilterCombobox label="Filter connections" placeholder="Filter…" value={value} onChange={next => (value = next)} suggestions={suggestions} />)
    fireEvent.click(view.getByRole("option", { name: /design/u }))
    assert.equal(value, "design")
    fireEvent.keyDown(input, { key: "Escape" })
    assert.equal(view.queryByRole("listbox"), null)
})
