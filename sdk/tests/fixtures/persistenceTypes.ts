import type { ActorFieldSchema } from "../../src/actor/schema.js"
import { Persistence } from "../../src/compiler/actor-compiler.js"
import { AnnotationKind } from "../../src/compiler/types.js"
import type { Annotation } from "../../src/compiler/types.js"

const persisted: ActorFieldSchema = { name: "count", persistence: Persistence.Persisted }
const ephemeral: ActorFieldSchema = { name: "cache", persistence: Persistence.Ephemeral }
const kind: Annotation["kind"] = AnnotationKind.Persistence

// @ts-expect-error Persistence must use a Persistence enum member.
const rawPersisted: ActorFieldSchema["persistence"] = "persisted"
// @ts-expect-error Persistence must use a Persistence enum member.
const rawEphemeral: ActorFieldSchema["persistence"] = "ephemeral"
// @ts-expect-error Annotation kinds must use an AnnotationKind enum member.
const rawKind: Annotation["kind"] = "persistence"

void [persisted, ephemeral, kind, rawPersisted, rawEphemeral, rawKind]
