export interface PromptAgent {
    prompt(text: string): Promise<string>
}

export class PiCli {
    constructor(private readonly agent: PromptAgent) {}

    async prompt(args: readonly string[]): Promise<string> {
        const text = args.join(" ").trim()
        if (!text) throw new Error('Usage: bun run chat "Your prompt"')
        return this.agent.prompt(text)
    }
}
