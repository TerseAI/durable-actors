export interface ActorResidencyMetrics {
    live: number
    dormant: number
}

export interface RequestRateMetrics {
    current: number
    series: number[]
}

export interface RequestCountMetrics {
    total: number
    series: number[]
}

export interface SocketMetrics {
    open: number
    series: number[]
}
