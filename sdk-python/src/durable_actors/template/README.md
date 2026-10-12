# Python actors

Requires Python 3.11+ and uv. Edit `src/actors.py`.

```sh
uv run durable-actors dev
# In another terminal:
uv run durable-actors generate
uv run durable-actors observe
```

`dev` checks types, reloads source changes, and keeps local state in `.durable-actors/`.
Use `--no-watch` to disable reloads. Generate without a server using `generate src/actors.py`.

[Python guide](https://github.com/TerseAI/durable-actors/blob/main/docs/reference/python-guide.md)
