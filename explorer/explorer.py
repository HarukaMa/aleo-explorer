import asyncio
import os

from db import Database
from node import Node
from node.testnet2 import Testnet2
from node.type import Block
from .type import Request, Message


class Explorer:

    def __init__(self):
        self.task = None
        self.message_queue = asyncio.Queue()
        self.node = None
        self.db = Database(server=os.environ["DB_HOST"], user=os.environ["DB_USER"], password=os.environ["DB_PASS"],
                           database=os.environ["DB_DATABASE"], schema=os.environ["DB_SCHEMA"],
                           explorer_message=self.message)

        # states
        self.latest_height = 0
        self.latest_block_hash = Testnet2.genesis_block.block_hash

    def start(self):
        self.task = asyncio.create_task(self.main_loop())

    async def message(self, msg: Message):
        await self.message_queue.put(msg)

    async def node_request(self, request):
        match type(request):
            case Request.GetLatestHeight:
                height = await self.db.get_latest_height()
                print("get latest height:", height)
                if height is None:
                    await self.node_request(Request.ProcessBlock(Testnet2.genesis_block))
                return height
            case Request.GetLatestWeight:
                weight = await self.db.get_latest_weight()
                print("get latest weight:", weight)
                return weight
            case Request.ProcessBlock:
                await self.add_block(request.block)
            case Request.GetBlockHashByHeight:
                return await self.db.get_block_hash_by_height(request.height)
            case Request.GetBlockHeaderByHeight:
                return await self.db.get_block_header_by_height(request.height)
            case _:
                print("unhandled explorer request")

    async def main_loop(self):
        await self.db.connect()
        await self.node_request(Request.GetLatestHeight())

        self.node = Node(explorer_message=self.message, explorer_request=self.node_request)
        self.node.connect("127.0.0.1", 4132)
        while True:
            msg = await self.message_queue.get()
            match msg.type:
                case Message.Type.NodeConnectError:
                    print("node connect error")
                case Message.Type.NodeConnected:
                    print("node connected")
                case Message.Type.NodeDisconnected:
                    print("node disconnected")
                case Message.Type.DatabaseConnectError:
                    print("database connect error")
                case Message.Type.DatabaseConnected:
                    print("database connected")
                case Message.Type.DatabaseDisconnected:
                    print("database disconnected")
                case Message.Type.DatabaseError:
                    print("database error:", msg.data)
                case Message.Type.DatabaseBlockAdded:
                    print("database block added:", msg.data)
                case _:
                    raise ValueError("unhandled explorer message type")

    async def add_block(self, block: Block):
        if block is Testnet2.genesis_block:
            await self.db.save_canonical_block(block)
            return
        raise NotImplementedError

    async def get_latest_block(self):
        return await self.db.get_latest_block()
