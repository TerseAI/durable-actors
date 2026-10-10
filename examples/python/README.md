# Python chat actor

Persistent messages, typed RPCs, and live state updates.

[Quickstart](../../sdk-python/README.md#quickstart) · [Reference](../../docs/reference/python-guide.md) · [Actor](actors.py) · [Client](client.py) · [Subscriber](watch.py)

## Run

Install Python 3.11+, uv, and Bun 1.4.2+. Bun runs the shared CLI; actor code and clients run in Python.

Get the example and install its Python dependencies:

```sh
git clone --depth 1 https://github.com/TerseAI/durable-actors.git
cd durable-actors/examples/python
uv sync --no-sources
bunx durable-actors dev
```

The CLI downloads the native runtime automatically. To write your own actor in a new project, follow the [quickstart](../../sdk-python/README.md#quickstart).

In a second terminal, from the same `durable-actors/examples/python` directory:

```sh
bunx durable-actors generate
uv run --no-sources client.py
uv run --no-sources watch.py
```

The client saves a message; the watcher prints live history until you press Enter. Restart the actor server to verify that messages persist.

## Call the actor

```python
from generated import actors

chat = actors.Chat.get("lobby")
print(chat.append(actors.Chat.Message(text="Hello from Python")))
```

`--no-sources` installs the published Python SDK instead of the repository dependency in `tool.uv.sources`. For SDK development, see the [contributing guide](../../CONTRIBUTING.md).
