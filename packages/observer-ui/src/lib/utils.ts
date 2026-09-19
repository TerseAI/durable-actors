import { type ClassValue, clsx } from "clsx"
import { extendTailwindMerge } from "tailwind-merge"

const merge = extendTailwindMerge({ prefix: "la" })

export function cn(...inputs: ClassValue[]) {
    return merge(clsx(inputs))
}
