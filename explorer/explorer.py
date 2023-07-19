import asyncio
import os
import traceback
from sys import stdout

import api
import webui
from db import Database
from interpreter.interpreter import init_builtin_program
from node import Node
from node.testnet3 import Testnet3
from node.types import Block, BlockHash
from .types import Request, Message, ExplorerRequest


class Explorer:

    def __init__(self):
        self.task = None
        self.message_queue: asyncio.Queue[Message] = asyncio.Queue()
        self.node = None
        self.db = Database(server=os.environ["DB_HOST"], user=os.environ["DB_USER"], password=os.environ["DB_PASS"],
                           database=os.environ["DB_DATABASE"], schema=os.environ["DB_SCHEMA"],
                           message_callback=self.message)

        # states
        self.dev_mode = False
        self.latest_height = 0
        self.latest_block_hash: BlockHash = Testnet3.genesis_block.block_hash
        #self.light_node_state = LightNodeState()

    def start(self):
        self.task = asyncio.create_task(self.main_loop())

    async def message(self, msg: Message):
        await self.message_queue.put(msg)

    async def node_request(self, request: ExplorerRequest):
        if isinstance(request, Request.GetLatestHeight):
            return self.latest_height
        elif isinstance(request, Request.ProcessBlock):
            await self.add_block(request.block)
        elif isinstance(request, Request.GetBlockByHeight):
            return await self.db.get_block_by_height(request.height)
        elif isinstance(request, Request.GetBlockHashByHeight):
            if request.height == self.latest_height:
                return self.latest_block_hash
            return await self.db.get_block_hash_by_height(request.height)
        elif isinstance(request, Request.GetBlockHeaderByHeight):
            return await self.db.get_block_header_by_height(request.height)
        elif isinstance(request, Request.RevertToBlock):
            raise NotImplementedError
        elif isinstance(request, Request.GetDevMode):
            return self.dev_mode
        else:
            print("unhandled explorer request")

    async def check_genesis(self):
        height = await self.db.get_latest_height()
        if height is None:
            if self.dev_mode:
                await self.add_block(Testnet3.dev_genesis_block)
            else:
                await self.add_block(Testnet3.genesis_block)

    async def main_loop(self):
        try:
            await self.db.connect()
            await self.check_dev_mode()
            await self.check_genesis()
            latest_height = await self.db.get_latest_height()
            if latest_height is None:
                raise ValueError("no block in database")
            self.latest_height = latest_height
            latest_block_hash = await self.db.get_block_hash_by_height(self.latest_height)
            if latest_block_hash is None:
                raise ValueError("no block in database")
            self.latest_block_hash = latest_block_hash
            await self.db.migrate()
            print(f"latest height: {self.latest_height}")
            self.node = Node(explorer_message=self.message, explorer_request=self.node_request)
            await self.node.connect(os.environ.get("P2P_NODE_HOST", "127.0.0.1"), int(os.environ.get("P2P_NODE_PORT", "4133")))
            asyncio.create_task(webui.run())
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
        except Exception as e:
            print("explorer error:", e)
            traceback.print_exc()
            raise

    async def add_block(self, block: Block):
        if block in [Testnet3.genesis_block, Testnet3.dev_genesis_block]:
            for program in Testnet3.builtin_programs:
                await init_builtin_program(self.db, program)
                await self.db.save_builtin_program(program)
            await self.db.save_block(block)
            return
        if block.previous_hash != self.latest_block_hash:
            print(f"ignoring block {block} because previous block hash does not match")
        else:
            print(f"adding block {block}")
            await self.db.save_block(block)
            self.latest_height = block.header.metadata.height
            self.latest_block_hash = block.block_hash

    async def get_latest_block(self):
        return await self.db.get_latest_block()

    async def check_dev_mode(self):
        dev_mode = os.environ.get("DEV_MODE", "")
        if dev_mode == "1":
            self.dev_mode = True

        if await self.db.get_latest_height() is not None:
            db_genesis = await self.db.get_block_by_height(0)
            if self.dev_mode:
                genesis_block = Testnet3.dev_genesis_block
            else:
                genesis_block = Testnet3.genesis_block
            if db_genesis.header.transactions_root != genesis_block.header.transactions_root:
                await self.clear_database()

    async def clear_database(self):
        print("The current database has a different genesis block!\nPress Ctrl+C to abort, or wait 10 seconds to clear the database.")
        i = 10
        while i > 0:
            print(f"\x1b[G\x1b[2K!!! Clearing database in {i} seconds !!!", end="")
            stdout.flush()
            await asyncio.sleep(1)
            i -= 1
        print("\x1b[G\x1b[2K!!! Clearing database now !!!")
        stdout.flush()
        await self.db.clear_database()

