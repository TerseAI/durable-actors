# CLI

Append `--help` to any command for options and defaults:

```sh
npx little-actors --help
npx little-actors start --help
```

## Start an application

```sh
npx little-actors init my-app
cd my-app
npm install
cp .env.example .env
npm run dev:actors
```

Follow the generated README to run the frontend. For an existing project, see [local development](../guides/local-development.md).

## Deploy and generate

Set your server URL, API key, and project ID in `.env`; see [configuration](configuration.md). Exported environment variables take precedence.

```sh
npx little-actors deploy src/actors.ts --image im-customer-build
npx little-actors generate --url
```

Each deploy replaces the current deployment and restarts its actors. `generate --url` uses the current contract.

Deployment requires a [published image](../guides/self-hosting.md#4-package-and-deploy-customer-code). Import the generated backend helpers from `generated/index.js`. To generate from local source, use `npx little-actors generate src/actors.ts`.

## Observe

```sh
npx little-actors observe
```

Browse actors, connections, and request history in the UI. Press Ctrl+C to stop it.
