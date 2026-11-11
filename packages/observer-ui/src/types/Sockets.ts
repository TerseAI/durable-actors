/** `lost` means the gateway stopped reporting before the connection closed. */
export type SocketStatus = "open" | "closed" | "lost"

export interface SocketSession {
    connectionId: string
    actorName: string
    actorId: string
    status: SocketStatus
    openedAtMs: number
    /** When it closed, or for a lost session when its gateway last reported. */
    closedAtMs: number | null
    closeCode?: number
    closeReason?: string
    messagesIn?: number
    messagesOut?: number
    metadata?: unknown
}
