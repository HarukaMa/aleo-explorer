import asyncio
from enum import IntEnum

from node import Node


class ExplorerMessage:

    class Type(IntEnum):
        ConnectTimeout = 0
        ConnectError = 1
        Connected = 2
        Disconnected = 3

    def __init__(self, type_: Type, data: any):
        self.type = type_
        self.data = data

class Explorer:

    def __init__(self):
        self.task = None
        self.message_queue = asyncio.Queue()
        self.node = None

    def start(self):
        self.task = asyncio.create_task(self.main_loop())

    async def message(self, msg: ExplorerMessage):
        await self.message_queue.put(msg)

    async def main_loop(self):
        self.node = Node(self)
        self.node.connect("127.0.0.1", 4132)
        while True:
            msg = await self.message_queue.get()
            match msg.type:
                case ExplorerMessage.Type.ConnectTimeout:
                    print("connect timeout")
                case ExplorerMessage.Type.ConnectError:
                    print("connect error")
                case ExplorerMessage.Type.Connected:
                    print("connected")
                case ExplorerMessage.Type.Disconnected:
                    print("disconnected")