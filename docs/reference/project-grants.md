# Direct RPC project grants

A trusted application authorization service validates its API credential and
current project ACL, then signs an Ed25519 JWT. Clients send that grant as the
Bearer credential to `POST /v1/projects/{projectId}/actors/{actorName}/{actorId}/find-actor`.
They cache the returned host target and call the host directly.

## Signing contract

The JWT header must contain `alg: EdDSA`, `typ: JWT`, and a configured `kid`.
Required claims are `iss`, `aud`, `sub`, `jti`, `iat`, `nbf`, `exp`,
`projectId`, and `scope: actor:resolve`. Subject and grant ID must be nonempty
and at most 128 bytes. Use an opaque, stable credential fingerprint for `sub`;
never use the API secret itself. Keep the authorization-service signing key
separate from the control-plane signing key.

The lifetime is at most 60 seconds. Set `iat` to the start of credential and ACL
validation, not the end of a slow external check, and set `exp` to `iat + 60`.
The verifier does not extend expiry for clock skew. Synchronize service clocks.

## Request path and boundaries

1. The application backend authenticates the caller and validates the project ACL.
2. The client exchanges its credential for a project grant and caches it.
3. Discovery verifies the grant locally and reads the published contract once
   per target resolution. Undeployed actor types cannot be resolved with grants.
4. The control plane signs an `actor:delegated-invoke` ticket restricted to the
   project, actor type, actor ID, host session, owner epoch, and published RPC
   method names. Its expiry cannot exceed the project grant expiry.
5. The host verifies signatures, identity, method permissions, expiry, and
   ownership before dispatch. Warm invocations do not contact the application
   backend, identity provider, deployment contract endpoint, or discovery API.

Project grants cannot administer deployments, read inspection APIs, mint socket
tickets, or access another project. Invocation tickets cannot call unpublished
methods, lifecycle callbacks, or the raw socket-effects endpoint. Actor methods
can still emit their own authorized socket effects.

The host logs delegated admissions with the credential fingerprint, grant ID,
actor address, method, and request ID. It applies an in-memory admission budget
of 120 RPCs per credential per host per 60-second window. Renewing a grant does
not reset its credential bucket. The cache retains at most 4,096 buckets;
eviction, host restart, or moving to another host can reset a bucket. This is
local abuse protection, not a global account quota. Keep gateway/WAF protections
for public ingress, including discovery and HEAD traffic. Rate-limited and
method-denied invocations return typed failures without dispatching actor code.

Revocation, membership removal, and ACL changes prevent renewal, but an already
issued grant remains usable until its absolute expiry. An admitted invocation
may finish after expiry. This is a bounded 60-second authorization model, not
instant revocation or a per-user ACL inside an actor. Applications must enforce
any finer business permissions in their actor methods.

## Coordinated rollout

Deploy the new control plane and actor-host runtime image together, refresh
existing hosts onto that runtime, and configure the authorization service's
public JWKS, issuer, and audience. Then deploy the grant issuer and direct
client. Neither the client nor the actor hosts receive the administrative
shared secret from the grant exchange.

For signing-key rotation, first distribute a JWKS containing both public keys,
then switch the issuer's signing key and `kid`. Remove the retired public key
after all grants it signed have expired. Disabling grant issuance prevents new
authorizations but does not revoke host tickets already issued.

The SDK still sends HEAD before POST and retains its existing bounded
pre-dispatch retry rules. It must not retry a mutation after an ambiguous POST
failure. This change does not add durable idempotency or remove that round trip.
