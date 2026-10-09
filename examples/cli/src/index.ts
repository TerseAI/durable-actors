import { actors } from "../generated/index.js"

const [command, ...args] = process.argv.slice(2)
if (command !== "prompt" || !args.join(" ").trim()) {
    console.error('Usage: pnpm start prompt "Your prompt"')
    process.exit(1)
}
const agent = actors.PiAgent.get("demo")
const reply = await agent.prompt(args.join(" "))
console.log(reply)
