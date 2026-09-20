import { actorClient } from "../client/client.js"
import { ActorDefinitionError } from "../errors.js"
import type { JsonObject, JsonValue } from "../json.js"

import { validateActorComponent } from "./identity.js"
import type { ActorSchema } from "./schema.js"
import { actorConnections, broadcastActor } from "./socket.js"
import type { ActorBroadcastOptions, ActorConnection, ActorSocket, ActorSocketMessage } from "./socket.js"
import { outgoingMessage, socketMetadata } from "./socketValidation.js"
import type { ActorSchemas } from "./socketValidation.js"

declare const actorTypes: unique symbol

const actorMetadata = new WeakMap<object, ActorMetadata>()
const actorDefinitions = new Map<string, ActorDefinition>()
const asyncFunction = Object.getPrototypeOf(async () => {}).constructor
const referenceClasses = new WeakMap<Function, ActorReferenceClass>()

interface Actor<Metadata = JsonValue, Incoming = JsonValue, Outgoing = Incoming, Tag extends string = string> {
    onConnect?(socket: ActorSocket<Metadata, Outgoing, Tag>): Promise<void>
    onMessage?(socket: ActorSocket<Metadata, Outgoing, Tag>, message: Incoming): Promise<void>
    onDisconnect?(
        socket: ActorSocket<Metadata, Outgoing, Tag>,
        code: number,
        reason: string,
        wasClean: boolean
    ): Promise<void>
}

abstract class Actor<Metadata = JsonValue, Incoming = JsonValue, Outgoing = Incoming, Tag extends string = string> {
    declare readonly [actorTypes]: { metadata: Metadata; incoming: Incoming; outgoing: Outgoing; tag: Tag }

    protected constructor() {}

    static get<TActorClass extends ActorClass>(
        this: ValidActorClass<TActorClass>,
        actorId: string
    ): ActorReference<TActorClass["prototype"]> {
        return getActorReference(this, validateActorComponent("actor ID", actorId))
    }

    protected get id(): string {
        return metadataFor(this).actorId
    }

    protected getConnections(): Promise<readonly ActorSocket<Metadata, Outgoing, Tag>[]> {
        return actorConnections<Metadata, Outgoing, Tag>(this)
    }

    protected broadcast(message: Outgoing, options?: ActorBroadcastOptions<Tag>): void {
        broadcastActor(this, message, options)
    }
}

function registerActorClass<Instance extends AnyActor>(
    actorClass: ActorClass<Instance>,
    state: ActorSchema
): ActorDefinition {
    const actorName = actorClassName(actorClass)
    const existing = actorDefinitions.get(actorName)
    if (existing !== undefined) {
        if (existing.actorClass !== actorClass) throw new ActorDefinitionError(`duplicate actor name ${actorName}`)
        existing.state = state
        return existing
    }

    const definition = { ...describeActorClass(actorClass), state }
    actorDefinitions.set(actorName, definition)
    return definition
}

function findActorDefinition(actorName: string): ActorDefinition | undefined {
    return actorDefinitions.get(actorName)
}

function getActorReference<TActorClass extends ActorClass>(
    actorClass: TActorClass,
    actorId: string
): ActorReference<TActorClass["prototype"]> {
    const Reference = referenceClass(actorClass)
    return new Reference(actorId) as unknown as ActorReference<TActorClass["prototype"]>
}

function bindActorIdentity(instance: AnyActor, actorId: string): void {
    actorMetadata.set(instance, { actorId: validateActorComponent("actor ID", actorId) })
}

function referenceClass(actorClass: ActorClass): ActorReferenceClass {
    const existing = referenceClasses.get(actorClass)
    if (existing !== undefined) return existing
    const definition = describeActorClass(actorClass)

    class ActorReference extends Actor {
        constructor(actorId: string) {
            super()
            bindActorIdentity(this, actorId)
        }
    }

    Object.defineProperty(ActorReference.prototype, "connect", {
        configurable: false,
        enumerable: false,
        writable: false,
        value: function connectActor(this: Actor, metadata: unknown): Promise<ActorConnection> {
            const actor = metadataFor(this)
            const attachment = socketMetadata(metadata, definition.schemas)
            return actorClient().then(client =>
                client.connect(definition.actorName, actor.actorId, attachment, definition.schemas)
            )
        }
    })

    Object.defineProperty(ActorReference.prototype, "broadcast", {
        configurable: false,
        enumerable: false,
        writable: false,
        value: function broadcastActorMessage(this: Actor, message: ActorSocketMessage): Promise<void> {
            const actor = metadataFor(this)
            const outgoing = outgoingMessage(message, definition.schemas)
            return actorClient().then(client => client.broadcast(definition.actorName, actor.actorId, outgoing))
        }
    })

    definition.methods.forEach(method => {
        Object.defineProperty(ActorReference.prototype, method, {
            configurable: false,
            enumerable: false,
            writable: false,
            value: function forwardActorMethod(this: Actor, ...args: unknown[]): Promise<unknown> {
                const metadata = metadataFor(this)
                return actorClient().then(client => client.invoke(definition.actorName, metadata.actorId, method, args))
            }
        })
    })
    referenceClasses.set(definition.actorClass, ActorReference)
    return ActorReference
}

function metadataFor(instance: AnyActor): ActorMetadata {
    const metadata = actorMetadata.get(instance)
    if (metadata === undefined)
        throw new ActorDefinitionError("actor identity is unavailable outside an actor invocation")
    return metadata
}

function describeActorClass(actorClass: ActorClass): ActorClassDescription {
    const actorName = actorClassName(actorClass)
    validateActorClass(actorClass, actorName)
    return {
        actorName: validateActorComponent("actor name", actorName),
        actorClass,
        schemas: actorClass.schemas ?? {},
        methods: new Set(discoverMethods(actorClass, actorName))
    }
}

function discoverMethods(actorClass: ActorClass, actorName: string): string[] {
    if (Object.getOwnPropertySymbols(actorClass.prototype).length > 0)
        throw new ActorDefinitionError(`actor class ${actorName} cannot define symbol methods`)

    return Object.entries(Object.getOwnPropertyDescriptors(actorClass.prototype)).flatMap(([name, descriptor]) => {
        if (name === "constructor") return []
        if (descriptor.get !== undefined || descriptor.set !== undefined)
            throw new ActorDefinitionError(`actor class ${actorName} cannot define accessor ${name}`)
        if (typeof descriptor.value !== "function") return []
        validateActorComponent("actor method", name)
        if (name === "then") throw new ActorDefinitionError(`actor class ${actorName} cannot define method then`)
        if (name === "connect" || name === "broadcast")
            throw new ActorDefinitionError(`actor class ${actorName} cannot define reserved method ${name}`)
        if (!(descriptor.value instanceof asyncFunction))
            throw new ActorDefinitionError(`actor method ${actorName}.${name} must be async`)
        if (lifecycleMethods.has(name)) return []
        return [name]
    })
}

const lifecycleMethods = new Set(["onConnect", "onMessage", "onDisconnect"])

function validateActorClass(actorClass: ActorClass, actorName: string): void {
    if (Object.getPrototypeOf(actorClass.prototype) !== Actor.prototype)
        throw new ActorDefinitionError(`actor class ${actorName} must extend Actor directly`)
    if (actorClass.length !== 0)
        throw new ActorDefinitionError(`actor class ${actorName} cannot require constructor arguments`)
}

function actorClassName(actorClass: ActorClass): string {
    if (actorClass.name.length === 0) throw new ActorDefinitionError("actor classes must be named")
    return actorClass.name
}

interface ActorDefinition extends ActorClassDescription {
    state: ActorSchema
}

interface ActorClassDescription {
    readonly actorName: string
    readonly actorClass: ActorClass
    readonly schemas: ActorSchemas
    readonly methods: ReadonlySet<string>
}

interface ActorMetadata {
    readonly actorId: string
}

type AnyActor = Actor<unknown, unknown, unknown>
type ActorClass<Instance extends AnyActor = AnyActor> = Function & {
    readonly prototype: Instance
    readonly schemas?: ActorSchemas
}
type ActorReferenceClass = new (actorId: string) => Actor
type AsyncMethod = (...args: never[]) => Promise<unknown>
type PubliclyConstructibleActorClass = abstract new (...args: never[]) => AnyActor
type InvalidActorMethod<Instance extends AnyActor> = {
    [Key in keyof Instance]-?: NonNullable<Instance[Key]> extends (...args: never[]) => unknown
        ? NonNullable<Instance[Key]> extends AsyncMethod
            ? never
            : Key
        : never
}[keyof Instance]
type ValidActorClass<TActorClass extends ActorClass> = TActorClass extends PubliclyConstructibleActorClass
    ? never
    : InvalidActorMethod<TActorClass["prototype"]> extends never
      ? TActorClass extends {
            readonly schemas: ActorSchemas<
                SocketMetadata<TActorClass["prototype"]>,
                SocketIncoming<TActorClass["prototype"]>,
                SocketOutgoing<TActorClass["prototype"]>,
                SocketTag<TActorClass["prototype"]>
            >
        }
          ? TActorClass
          : TActorClass extends { readonly schemas: unknown }
            ? never
            : TActorClass
      : never
type ActorReference<Instance extends AnyActor> = {
    [
        Key in keyof Instance as Instance[Key] extends AsyncMethod
            ? Key extends SocketLifecycleMethod
                ? never
                : Key
            : never
    ]: Instance[Key]
} & {
    connect(
        metadata: SocketMetadata<Instance>
    ): Promise<ActorConnection<SocketIncoming<Instance>, SocketOutgoing<Instance>, JsonObject>>
    broadcast(message: SocketOutgoing<Instance>): Promise<void>
}
type SocketLifecycleMethod = "onConnect" | "onMessage" | "onDisconnect"
type SocketMetadata<Instance extends AnyActor> = Instance[typeof actorTypes]["metadata"]
type SocketIncoming<Instance extends AnyActor> = Instance[typeof actorTypes]["incoming"]
type SocketOutgoing<Instance extends AnyActor> = Instance[typeof actorTypes]["outgoing"]
type SocketTag<Instance extends AnyActor> = Instance[typeof actorTypes]["tag"]
type ActorSocketOf<Instance extends AnyActor> = ActorSocket<
    SocketMetadata<Instance>,
    SocketOutgoing<Instance>,
    SocketTag<Instance>
>
type ActorMessageOf<Instance extends AnyActor> = SocketIncoming<Instance>

export { Actor, bindActorIdentity, findActorDefinition, registerActorClass }
export type { ActorClass, ActorDefinition, ActorMessageOf, ActorReference, ActorSocketOf, AnyActor }
