# TypeScript documentation

Hover over SDK exports in your editor for types and API documentation.

To generate browsable TypeDoc documentation, run from the repository root:

```sh
pnpm install
pnpm docs:build
```

Open `.artifacts/api/index.html` in your browser. The documentation covers the SDK, backend helpers, proxy, and local runtime APIs.

Rerun `pnpm docs:build` after changing the SDK to refresh the documentation.
