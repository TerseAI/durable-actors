import asyncio

from generated import Chat
from generated.chat_models import Message
from little_actors import Client


async def main() -> None:
    async with Client() as transport:
        messages = await Chat("lobby", transport).append(Message(text="Hello from Python"))
        print(messages)


asyncio.run(main())
