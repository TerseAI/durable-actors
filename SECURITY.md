# Security Policy

## Reporting a vulnerability

Email [security@useterse.ai](mailto:security@useterse.ai) with suspected vulnerabilities in Durable Actors. Please keep vulnerability details out of public issues and pull requests until a fix or mitigation is available.

Include the affected version or commit, deployment configuration, impact, and minimal reproduction steps. Remove credentials, personal data, and customer data from logs and examples. We will coordinate investigation and disclosure with you privately.

## Security updates

Use the latest published release. Security fixes are made on the current development line; do not assume older releases receive backports. Check [GitHub Releases](https://github.com/TerseAI/durable-actors/releases) for release notes.

## Deployment boundaries

The local development runtime binds to localhost. Hosted deployments require deliberate authentication and network configuration: a server with `DURABLE_ACTORS_SECRET` unset accepts unauthenticated API requests, even when listening beyond localhost.

Keep server credentials out of browser code and public logs. See the [HTTP authentication reference](docs/reference/openapi.md) and [configuration reference](docs/reference/configuration.md) before exposing a deployment.
