from generated import Chat
from generated.chat_models import Member, Message

chat = Chat("lobby")
subscription = chat.subscribe(
    lambda state: print(state.messages), metadata=Member(name="Ada")
)
try:
    chat.append(Message(text="Hello from a state subscriber"))
    input("Listening for state changes. Press Enter to stop.\n")
finally:
    subscription.close()
