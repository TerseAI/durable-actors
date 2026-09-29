import asyncio
import socket
import sys

from durable_actors.executor_wire import Channel


async def test_large_actor_state_round_trips_through_executor_channel():
    left, right = socket.socketpair()
    reader_a, writer_a = await asyncio.open_connection(sock=left, limit=sys.maxsize)
    reader_b, writer_b = await asyncio.open_connection(sock=right, limit=sys.maxsize)
    sender, receiver = Channel(reader_a, writer_a), Channel(reader_b, writer_b)
    message = {"state": "x" * (33 * 1024 * 1024)}
    try:
        _, received = await asyncio.gather(sender.send(message), receiver.read())
        assert received == message
    finally:
        writer_a.close()
        writer_b.close()
        await asyncio.gather(writer_a.wait_closed(), writer_b.wait_closed())
