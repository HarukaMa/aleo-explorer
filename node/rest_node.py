import asyncio
import os
import traceback
from typing import Callable

import aiohttp

import explorer
from .light_node import LightNodeState
from .types import *  # too many types


class RESTNode:
    def __init__(self, explorer_message: Callable, explorer_request: Callable, light_node_state: LightNodeState):
        self.worker_task: asyncio.Task | None = None
        self.explorer_message = explorer_message
        self.explorer_request = explorer_request
        self.light_node_state = light_node_state

    async def connect(self, host: str, port: int):
        # return
        self.worker_task = asyncio.create_task(self.worker(host, port))
        # self.light_node_state.connect("127.0.0.1", 4133)

    async def worker(self, host: str, port: int):
        async with aiohttp.ClientSession() as session:
            while True:
                await asyncio.sleep(5)
                try:
                    async with session.get(f"{os.environ.get('PROTOCOL', 'http')}://{host}:{port}/testnet3/latest/height") as resp:
                        if not resp.ok:
                            print("failed to get latest height")
                            continue
                        latest_height = int(await resp.text())
                        print("remote latest height:", latest_height)
                        local_height = await self.explorer_request(explorer.Request.GetLatestHeight())
                        while latest_height > local_height:
                            start = local_height + 1
                            end = min(start + 50, latest_height + 1)
                            print(f"fetching blocks {start} to {end - 1}")
                            async with session.get(f"{os.environ.get('PROTOCOL', 'http')}://{host}:{port}/testnet3/blocks?start={start}&end={end}") as block_resp:
                                if not block_resp.ok:
                                    print("failed to get blocks")
                                    continue
                                for block in await block_resp.json():
                                    block = Block.load_json(block)
                                    await self.explorer_request(explorer.Request.ProcessBlock(block))
                                    local_height = block.header.metadata.height
                except Exception:
                    traceback.print_exc()
                    continue