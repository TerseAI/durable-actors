import type { ComponentType, ReactNode } from "react"

export type ObserverDestination = { page: "overview" } | { page: "actorType"; actorName: string }

export interface ObserverLinkProps {
    to: ObserverDestination
    className?: string
    children: ReactNode
}

export type ObserverLink = ComponentType<ObserverLinkProps>
