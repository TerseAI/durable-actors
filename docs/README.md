# Documentation

| Need                              | Start here                                                           |
| --------------------------------- | -------------------------------------------------------------------- |
| Run an application                | [Local development](guides/local-development.md)                     |
| Deploy a server                   | [Self-hosting](guides/self-hosting.md)                               |
| Configure credentials and storage | [Configuration](reference/configuration.md)                          |
| Use the CLI                       | [Workflows](reference/cli.md), then `little-actors <command> --help` |
| Understand actor behavior         | [TypeScript API](reference/api.md)                                   |
| Integrate over HTTP               | [OpenAPI](reference/openapi.yaml), also served at `/openapi.yaml`    |
| Connect a browser                 | [WebSockets](guides/websockets.md)                                   |
| Inspect activity                  | [Observability](guides/observability.md)                             |

## Maintaining references

- **HTTP:** edit `reference/openapi.yaml`; validate with `pnpm docs:check`.
- **CLI:** keep options in Commander definitions in `sdk/src/cli/`; use `--help` as the reference.
- **TypeScript:** use [TSDoc](https://tsdoc.org/) for behavior that signatures do not explain.

Keep examples short. Document only what callers need to use the API correctly; omit runtime internals and repeated explanations.

Run `pnpm docs:build` from the repository root to validate OpenAPI and generate [TypeDoc](https://typedoc.org/) HTML in `.artifacts/api/`. Open `index.html` locally. CI runs the same build and uploads the `typescript-api` artifact. Generated HTML is not checked in.
