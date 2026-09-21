import { ActorDefinitionError } from "../errors.js"

function Persisted(_value: undefined, context: ClassFieldDecoratorContext): void {
    validateField("Persisted", context)
    if (context.private) throw new ActorDefinitionError("@Persisted cannot decorate a JavaScript private field")
}

function Ephemeral(_value: undefined, context: ClassFieldDecoratorContext): void {
    validateField("Ephemeral", context)
}

function Emittable(_value: undefined, context: ClassFieldDecoratorContext): void {
    validateField("Emittable", context)
    if (context.private) throw new ActorDefinitionError("@Emittable requires a public field")
}

function validateField(name: string, context: ClassFieldDecoratorContext): void {
    if (context.kind !== "field" || context.static || typeof context.name !== "string")
        throw new ActorDefinitionError(`@${name} requires an instance field with a string name`)
}

export { Emittable, Ephemeral, Persisted }
