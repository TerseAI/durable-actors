function usageComment(description: string, example?: string): string {
    const lines = [description, ...(example ? ["@example", ...example.split("\n")] : [])]
    return `/**\n${lines.map(line => ` * ${line.replaceAll("*/", "*\\/")}`).join("\n")}\n */`
}

export { usageComment }
