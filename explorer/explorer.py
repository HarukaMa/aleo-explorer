import asyncio
import os
import traceback

import webui
from db import Database
from node import Node
from node.light_node import LightNodeState
from node.testnet2 import Testnet2
from node.type import Block, Transaction
from .type import Request, Message


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
        self.latest_block_hash = Testnet2.genesis_block.block_hash
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
                case Request.GetLatestWeight:
                    weight = await self.db.get_latest_canonical_weight()
                    return weight
                case Request.ProcessBlock:
                    await self.add_block(request.block)
                case Request.GetBlockByHeight:
                    return await self.db.get_canonical_block_by_height(request.height)
                case Request.GetBlockHashByHeight:
                    if request.height == self.latest_height:
                        return self.latest_block_hash
                    return await self.db.get_canonical_block_hash_by_height(request.height)
                case Request.GetBlockHeaderByHeight:
                    return await self.db.get_canonical_block_header_by_height(request.height)
                case Request.RevertToBlock:
                    await self.revert_to_block(request.height)
                case _:
                    print("unhandled explorer request")
        finally:
            self.db_lock.release()

    async def check_genesis(self):
        height = await self.db.get_latest_canonical_height()
        if height is None:
            await self.node_request(Request.ProcessBlock(Testnet2.genesis_block))

    async def main_loop(self):
        try:
            await self.db.connect()
            await self.check_genesis()
            self.latest_height = await self.db.get_latest_canonical_height()
            self.latest_block_hash = await self.db.get_canonical_block_hash_by_height(self.latest_height)
            print(f"latest height: {self.latest_height}")
            self.node = Node(explorer_message=self.message, explorer_request=self.node_request,
                             light_node_state=self.light_node_state)
            await self.node.connect(os.environ.get("NODE_HOST", "127.0.0.1"), int(os.environ.get("NODE_PORT", "4132")))
            asyncio.create_task(webui.run(light_node_state=self.light_node_state))
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
        if block is Testnet2.genesis_block:
            await self.db.save_canonical_block(block)
            return
        # testnet2 bug check
        testnet2_bug = False
        for tx in block.transactions.transactions:
            tx: Transaction
            if str(tx.ledger_root) in Testnet2.non_canonical_ledger_roots:
                testnet2_bug = True
                break
        if block.previous_block_hash != self.latest_block_hash or testnet2_bug:
            if await self.db.is_block_hash_canonical(block.block_hash):
                # should only cancel canonical state during revertion
                return
            print(f"adding non-canonical block {block}")
            await self.db.save_non_canonical_block(block)
        else:
            print(f"adding canonical block {block}")
            await self.db.save_canonical_block(block)
            self.latest_height = block.header.metadata.height
            self.latest_block_hash = block.block_hash

    async def revert_to_block(self, height):
        await self.db.revert_to_block(height)
        self.latest_height = height
        self.latest_block_hash = await self.db.get_canonical_block_hash_by_height(height)

    async def get_latest_block(self):
        return await self.db.get_latest_canonical_block()
