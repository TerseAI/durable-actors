from generated import Chat
from generated.chat_models import Message

messages = Chat("lobby").append(Message(text="Hello from Python"))
print(messages)
