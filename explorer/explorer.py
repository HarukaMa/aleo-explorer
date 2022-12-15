import asyncio
import os
import traceback

import api
import webui
from db import Database
from node import Node
from node.light_node import LightNodeState
from node.testnet3 import Testnet3
from node.types import Block
from .types import Request, Message


class Explorer:

    def __init__(self):
        self.task = None
        self.message_queue = asyncio.Queue()
        self.node = None
        self.db = Database(server=os.environ["DB_HOST"], user=os.environ["DB_USER"], password=os.environ["DB_PASS"],
                           database=os.environ["DB_DATABASE"], schema=os.environ["DB_SCHEMA"],
                           message_callback=self.message)

        # states
        self.latest_height = 0
        self.latest_block_hash = Testnet3.genesis_block.block_hash
        self.db_lock = asyncio.Lock()
        self.light_node_state = LightNodeState()

    def start(self):
        self.task = asyncio.create_task(self.main_loop())

    async def message(self, msg: Message):
        await self.message_queue.put(msg)

    async def node_request(self, request):
        await self.db_lock.acquire()
        try:
            match type(request):
                case Request.GetLatestHeight:
                    return self.latest_height
                case Request.ProcessBlock:
                    await self.add_block(request.block)
                case Request.GetBlockByHeight:
                    return await self.db.get_block_by_height(request.height)
                case Request.GetBlockHashByHeight:
                    if request.height == self.latest_height:
                        return self.latest_block_hash
                    return await self.db.get_block_hash_by_height(request.height)
                case Request.GetBlockHeaderByHeight:
                    return await self.db.get_block_header_by_height(request.height)
                case Request.RevertToBlock:
                    await self.revert_to_block(request.height)
                case _:
                    print("unhandled explorer request")
        finally:
            self.db_lock.release()

    async def check_genesis(self):
        height = await self.db.get_latest_height()
        if height is None:
            await self.node_request(Request.ProcessBlock(Testnet3.genesis_block))

    async def main_loop(self):
        try:
            await self.db.connect()
            await self.check_dev_mode()
            await self.check_genesis()
            self.latest_height = await self.db.get_latest_height()
            self.latest_block_hash = await self.db.get_block_hash_by_height(self.latest_height)
            print(f"latest height: {self.latest_height}")
            self.node = Node(explorer_message=self.message, explorer_request=self.node_request,
                             light_node_state=self.light_node_state)
            await self.node.connect(os.environ.get("NODE_HOST", "127.0.0.1"), int(os.environ.get("NODE_PORT", "4132")))
            asyncio.create_task(webui.run(self.light_node_state))
            asyncio.create_task(api.run())
            while True:
                msg = await self.message_queue.get()
                match msg.type:
                    case Message.Type.NodeConnectError:
                        print("node connect error:", msg.data)
                    case Message.Type.NodeConnected:
                        print("node connected")
                    case Message.Type.NodeDisconnected:
                        print("node disconnected")
                    case Message.Type.DatabaseConnectError:
                        print("database connect error:", msg.data)
                    case Message.Type.DatabaseConnected:
                        print("database connected")
                    case Message.Type.DatabaseDisconnected:
                        print("database disconnected")
                    case Message.Type.DatabaseError:
                        print("database error:", msg.data)
                    case Message.Type.DatabaseBlockAdded:
                        # maybe do something later?
                        pass
                    case _:
                        raise ValueError("unhandled explorer message type")
        except Exception as e:
            print("explorer error:", e)
            traceback.print_exc()
            raise

    async def add_block(self, block: Block):
        if block is Testnet3.genesis_block:
            await self.db.save_block(block)
            return
        # if block.previous_hash != self.latest_block_hash:
        #     print(f"ignoring block {block} because previous block hash does not match")
        else:
            print(f"adding block {block}")
            await self.db.save_block(block)
            self.latest_height = block.header.metadata.height
            self.latest_block_hash = block.block_hash

    async def get_latest_block(self):
        return await self.db.get_latest_block()

    async def check_dev_mode(self):
        try:
            with open("/tmp/explorer_dev_mode", "rb") as f:
                from hashlib import md5
                if md5(f.read()).hexdigest() == "1c28714e40263e4c4afa1aa7f7272a3f":
                    i = 10
                    while i > 0:
                        print(f"!!! Clearing database in {i} seconds !!!          ", end="\r")
                        await asyncio.sleep(1)
                        i -= 1
                    print("!!! Clearing database now !!!          ")
                    await self.db.clear_database()
        except:
            pass

