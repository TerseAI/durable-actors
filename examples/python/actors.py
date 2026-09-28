from typing import Annotated

from pydantic import BaseModel

from little_actors import Actor, ActorSocket, Emittable, Persisted


class Member(BaseModel):
    name: str


class Message(BaseModel):
    text: str


class Chat(Actor[Member, Message, Message]):
    messages: Annotated[list[Message], Persisted(), Emittable()] = []

    async def append(self, message: Message) -> list[Message]:
        self.messages.append(message)
        self.broadcast(message)
        return self.messages

    async def on_message(self, socket: ActorSocket[Member, Message], message: Message) -> None:
        await self.append(Message(text=f"{socket.metadata.name}: {message.text}"))
