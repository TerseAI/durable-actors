import { Ajv } from "ajv"
import type { ValidateFunction } from "ajv"

import { ActorValidationError } from "../errors.js"

import type { SocketContract } from "./contract.js"

const validators = new WeakMap<SocketContract, Map<string, ValidateFunction>>()

function validateContract(
    value: unknown,
    kind: "Metadata" | "Incoming" | "Outgoing" | "State",
    contract?: SocketContract
): void {
    if (!contract) return
    let compiled = validators.get(contract)
    if (!compiled) {
        compiled = new Map()
        const ajv = new Ajv({ strict: false, validateFormats: false })
        for (const name of ["Metadata", "Incoming", "Outgoing", "State"])
            compiled.set(name, ajv.compile({ ...contract.schema, $ref: `#/definitions/${name}` }))
        validators.set(contract, compiled)
    }
    const validate = compiled.get(kind)!
    if (!validate(value))
        throw new ActorValidationError(
            `${contract.actorName} ${kind.toLowerCase()} violates its socket contract: ${JSON.stringify(validate.errors)}`
        )
}

export { validateContract }
