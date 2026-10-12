import { ActorInvocationError, ActorProtocolError } from "./errors.js";
import { HttpActorHostTransport, connectionRefused, isRecord, parseActorHostReply, responseDocument } from "./http.js";
import { cloneJson } from "./json.js";
import { authorizationHeaders, configuredSettings, environmentSettings, projectActorPath, runtimeEnvironment, validateActorComponent, validateOrigin } from "./settings.js";
import { LatencyTimeline } from "./telemetry.js";
const TARGET_EXPIRATION_SAFETY_MS = 5_000;
export class HttpActorClient {
    settingsValue;
    environment;
    fetchRequest;
    requestId;
    actorHost;
    now;
    monotonicNow;
    telemetry;
    targets = new Map();
    constructor(options, dependencies = {}) {
        this.environment = dependencies.environment ?? runtimeEnvironment();
        this.fetchRequest = dependencies.fetch ?? globalThis.fetch;
        this.requestId = dependencies.requestId ?? (() => globalThis.crypto.randomUUID());
        this.actorHost = dependencies.actorHost ?? new HttpActorHostTransport(this.fetchRequest);
        // Workflow runtimes can replace Date.now with replayable logical time.
        this.now = dependencies.now ?? (() => performance.timeOrigin + performance.now());
        this.monotonicNow = dependencies.monotonicNow ?? (() => performance.now());
        this.telemetry = dependencies.telemetry ?? (() => { });
        this.settingsValue = options === undefined ? undefined : configuredSettings(options);
    }
    async invoke(actorName, actorId, method, args) {
        const requestId = validateActorComponent("request ID", this.requestId());
        const timeline = new LatencyTimeline(this.monotonicNow);
        let outcome = "failed";
        try {
            const invocation = this.invocation(requestId, actorName, actorId, method, args);
            timeline.mark("invocation_built");
            const result = await this.invokeAttempt(invocation, true, timeline);
            outcome = "completed";
            return result;
        }
        finally {
            this.telemetry({
                event: "actor_client_invocation",
                request_id: requestId,
                actor_name: actorName,
                actor_id: actorId,
                method,
                ...timeline.finish(),
                outcome
            });
        }
    }
    async invokeAttempt(invocation, retryAvailable, timeline) {
        let target = await this.cachedTarget(invocation, timeline);
        let reply;
        if (target) {
            reply = await this.direct(target, invocation);
            timeline.mark("host_rpc_completed");
        }
        else {
            const key = actorKey(invocation.actorName, invocation.actorId);
            const current = this.targets.get(key);
            const resolved = await this.resolveAndInvoke(invocation);
            target = resolved.target;
            reply = resolved.reply;
            if ((reply.type === "completed" || reply.type === "failed") && this.targets.get(key) === current) {
                this.targets.set(key, { promise: Promise.resolve(target), target });
            }
            timeline.mark("control_plane_invocation_completed");
        }
        if (reply.type === "completed")
            return reply.result;
        if (reply.type === "failed")
            throw new ActorInvocationError(reply.code, invocation.requestId, reply.message);
        this.invalidateTarget(invocation, target);
        if (!retryAvailable)
            throw new ActorInvocationError(reply.type === "unauthenticated" ? "unauthenticated" : "unavailable", invocation.requestId, "actor host rejected the invocation before execution after recovery");
        return this.invokeAttempt(invocation, false, timeline);
    }
    async direct(target, invocation) {
        try {
            return await this.actorHost.invoke(target, invocation);
        }
        catch (error) {
            this.invalidateTarget(invocation, target);
            if (error instanceof ActorInvocationError || error instanceof ActorProtocolError)
                throw error;
            const message = error instanceof Error ? error.message : String(error);
            throw new ActorInvocationError("outcome_unknown", invocation.requestId, `actor-host HTTP request failed after dispatch: ${message}`);
        }
    }
    async resolveAndInvoke(invocation) {
        let response;
        try {
            response = await this.fetchRequest(`${this.settings.controlPlaneUrl}${projectActorPath(this.settings.projectId, invocation.actorName, invocation.actorId)}/invoke`, {
                method: "POST",
                redirect: "manual",
                headers: {
                    accept: "application/json",
                    ...authorizationHeaders(this.settings.credential),
                    "x-request-id": invocation.requestId,
                    "content-type": "application/json"
                },
                body: JSON.stringify({
                    requestId: invocation.requestId,
                    method: invocation.method,
                    args: invocation.args,
                    homeRegion: this.settings.homeRegion
                })
            });
        }
        catch (error) {
            const message = error instanceof Error ? error.message : String(error);
            if (connectionRefused(error))
                throw new ActorInvocationError("unavailable", invocation.requestId, `could not connect to ${this.settings.controlPlaneUrl}: ${message}`);
            throw new ActorInvocationError("outcome_unknown", invocation.requestId, `control-plane invocation failed after it may have been dispatched: ${message}`);
        }
        const document = await responseDocument(response);
        if (!response.ok)
            this.throwResponseFailure(response, document, invocation.requestId);
        if (!isRecord(document))
            throw new ActorProtocolError("control-plane response did not contain an invocation outcome and target");
        const target = parseTarget(document.target);
        const reply = isRecord(document.outcome) && document.outcome.type === "unauthenticated"
            ? { type: "unauthenticated" }
            : parseActorHostReply(document.outcome);
        return { target, reply };
    }
    invalidateTarget(invocation, target) {
        const key = actorKey(invocation.actorName, invocation.actorId);
        if (this.targets.get(key)?.target === target)
            this.targets.delete(key);
    }
    async cachedTarget(invocation, timeline) {
        const key = actorKey(invocation.actorName, invocation.actorId);
        const current = this.targets.get(key);
        timeline.mark("target_cache_checked");
        if (!current)
            return undefined;
        const target = await current.promise;
        if (this.targets.get(key) !== current)
            return this.cachedTarget(invocation, timeline);
        if (target.expiresAtMs > this.now() + TARGET_EXPIRATION_SAFETY_MS)
            return target;
        this.targets.delete(key);
        return undefined;
    }
    async target(invocation, timeline) {
        const cached = await this.cachedTarget(invocation, timeline);
        if (cached)
            return cached;
        const key = actorKey(invocation.actorName, invocation.actorId);
        if (this.targets.has(key))
            return this.target(invocation, timeline);
        const resolving = {
            promise: this.resolveTarget(invocation)
                .then(target => {
                resolving.target = target;
                return target;
            })
                .catch(error => {
                if (this.targets.get(key) === resolving)
                    this.targets.delete(key);
                throw error;
            })
        };
        this.targets.set(key, resolving);
        return resolving.promise;
    }
    async resolveTarget(invocation) {
        let response;
        try {
            response = await this.fetchRequest(targetUrl(this.settings, invocation.actorName, invocation.actorId), {
                method: "POST",
                redirect: "manual",
                headers: {
                    accept: "application/json",
                    ...authorizationHeaders(this.settings.credential),
                    "x-request-id": invocation.requestId,
                    "content-type": "application/json"
                },
                body: JSON.stringify({ homeRegion: this.settings.homeRegion })
            });
        }
        catch (error) {
            const message = error instanceof Error ? error.message : String(error);
            throw new ActorInvocationError("unavailable", invocation.requestId, `control-plane HTTP request failed before dispatch: ${message}`);
        }
        const document = await responseDocument(response);
        if (!response.ok)
            this.throwResponseFailure(response, document, invocation.requestId);
        return parseTarget(document);
    }
    throwResponseFailure(response, document, requestId) {
        if (response.status === 401 || response.status === 403)
            throw new ActorInvocationError("unauthenticated", requestId, "the durable-actors application credential was rejected");
        const failure = isRecord(document) ? document.error : undefined;
        if (!isRecord(failure) ||
            typeof failure.code !== "string" ||
            !failure.code ||
            typeof failure.message !== "string")
            throw new ActorProtocolError(`control-plane HTTP ${response.status} response did not contain a valid error`);
        throw new ActorInvocationError(failure.code, typeof failure.requestId === "string" ? failure.requestId : requestId, failure.message);
    }
    invocation(requestId, actorName, actorId, method, args) {
        return {
            requestId,
            projectId: this.settings.projectId,
            actorName: validateActorComponent("actor name", actorName),
            actorId: validateActorComponent("actor ID", actorId),
            method: validateActorComponent("actor method", method),
            args: this.jsonArguments(args)
        };
    }
    get settings() {
        if (this.settingsValue !== undefined)
            return this.settingsValue;
        this.settingsValue = configuredSettings(environmentSettings(this.environment));
        return this.settingsValue;
    }
    jsonArguments(args) {
        const value = cloneJson(args, "actor arguments");
        if (!Array.isArray(value))
            throw new ActorProtocolError("actor arguments must be a JSON array");
        return value;
    }
}
function parseTarget(document) {
    if (!isRecord(document) ||
        typeof document.route !== "string" ||
        typeof document.token !== "string" ||
        !document.token.trim() ||
        !positiveInteger(document.ownerEpoch) ||
        !positiveInteger(document.expiresAtMs))
        throw new ActorProtocolError("control-plane response did not contain a valid actor host target");
    validateOrigin(document.route);
    return {
        route: document.route,
        token: document.token,
        ownerEpoch: document.ownerEpoch,
        expiresAtMs: document.expiresAtMs
    };
}
function targetUrl(settings, actorName, actorId) {
    const actor = validateActorComponent("actor name", actorName);
    const id = validateActorComponent("actor ID", actorId);
    return `${settings.controlPlaneUrl}${projectActorPath(settings.projectId, actor, id)}/find-actor`;
}
function actorKey(actorName, actorId) {
    return `${actorName}\u001f${actorId}`;
}
function positiveInteger(value) {
    return typeof value === "number" && Number.isSafeInteger(value) && value > 0;
}
