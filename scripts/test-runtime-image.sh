#!/usr/bin/env bash
set -euo pipefail

image=$1
target=$2
docker run --rm "$image" --version
case "$target" in
    control-plane)
        docker run --rm --entrypoint sh "$image" -ec 'test -z "$(command -v bun)"; test -z "$(command -v python3)"'
        ;;
    typescript)
        docker run --rm --entrypoint sh "$image" -ec 'test -z "$(command -v python3)"; test ! -d /node_modules/typescript'
        docker run --rm --entrypoint bun -v "$PWD/sdk/tests/fixtures/image-smoke.mjs:/tmp/image-smoke.mjs:ro" "$image" /tmp/image-smoke.mjs
        ;;
    python)
        docker run --rm --entrypoint sh "$image" -ec 'test -z "$(command -v bun)"'
        docker run --rm --entrypoint python3 -v "$PWD/sdk-python/tests/fixtures/image_smoke.py:/tmp/image_smoke.py:ro" "$image" /tmp/image_smoke.py
        ;;
    *) echo "Unknown image target: $target" >&2; exit 1 ;;
esac
