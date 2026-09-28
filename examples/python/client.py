from generated import Chat
from generated.chat_models import Message

from little_actors import Client

with Client() as transport:
    messages = Chat("lobby", transport).append(Message(text="Hello from Python"))
    print(messages)
