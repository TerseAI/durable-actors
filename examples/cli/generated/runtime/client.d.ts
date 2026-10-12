import type { ActorHostTarget, ActorHostTransport, ActorInvocation } from "./http.js";
import { configuredSettings } from "./settings.js";
import type { DurableActorsClientOptions, Environment } from "./settings.js";
import { LatencyTimeline } from "./telemetry.js";
import type { TelemetrySink } from "./telemetry.js";
export declare class HttpActorClient {
    private settingsValue;
    protected readonly environment: Environment;
    protected readonly fetchRequest: typeof globalThis.fetch;
    protected readonly requestId: () => string;
    protected readonly actorHost: ActorHostTransport;
    protected readonly now: () => number;
    protected readonly monotonicNow: () => number;
    protected readonly telemetry: TelemetrySink;
    protected readonly targets: Map<string, TargetResolution>;
    constructor(options?: DurableActorsClientOptions, dependencies?: HttpActorClientDependencies);
    invoke(actorName: string, actorId: string, method: string, args: readonly unknown[]): Promise<unknown>;
    private invokeAttempt;
    private direct;
    private resolveAndInvoke;
    private invalidateTarget;
    private cachedTarget;
    protected target(invocation: ActorAddress, timeline: LatencyTimeline): Promise<ActorHostTarget>;
    private resolveTarget;
    private throwResponseFailure;
    private invocation;
    protected get settings(): RemoteActorSettings;
    private jsonArguments;
}
type ActorAddress = Pick<ActorInvocation, "requestId" | "projectId" | "actorName" | "actorId">;
interface TargetResolution {
    readonly promise: Promise<ActorHostTarget>;
    target?: ActorHostTarget;
}
type RemoteActorSettings = ReturnType<typeof configuredSettings>;
export interface HttpActorClientDependencies {
    readonly environment?: Environment;
    readonly fetch?: typeof globalThis.fetch;
    readonly requestId?: () => string;
    readonly actorHost?: ActorHostTransport;
    readonly now?: () => number;
    readonly monotonicNow?: () => number;
    readonly telemetry?: TelemetrySink;
}
export type { DurableActorsClientOptions };
