import { expect, test } from "bun:test"

import { PiCli } from "../src/cli.js"

test("sends all prompt arguments as one message and returns the reply", async () => {
    const messages: string[] = []
    const cli = new PiCli({
        async prompt(text: string) {
            messages.push(text)
            return "Hello Alice"
        }
    })
    expect(await cli.prompt(["Hello", "Alice"])).toBe("Hello Alice")
    expect(messages).toEqual(["Hello Alice"])
})

test("rejects an empty prompt before contacting the actor", async () => {
    let calls = 0
    const cli = new PiCli({
        async prompt() {
            calls++
            return "unused"
        }
    })
    for (const args of [[], [" ", "\t"]]) {
        await expect(cli.prompt(args)).rejects.toThrow('Usage: bun run chat "Your prompt"')
    }
    expect(calls).toBe(0)
})

test("surfaces failures from the actor", async () => {
    const failure = new Error("Model request failed")
    const cli = new PiCli({
        async prompt() {
            throw failure
        }
    })
    await expect(cli.prompt(["Hello"])).rejects.toBe(failure)
})
