import { AnnotationKind, DecoratorResult, DecoratorUse } from "../types.js";

function readResource(use: DecoratorUse): DecoratorResult {
    return { annotations: [{ kind: AnnotationKind.Resource, node: use.node }], diagnostics: [] }
}

export { readResource }