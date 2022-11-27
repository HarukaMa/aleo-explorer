# Copyright (C) 2019-2022 Aleo Systems Inc.
# This file is part of the snarkOS library.
#
# The snarkOS library is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# The snarkOS library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with the snarkOS library. If not, see <https://www.gnu.org/licenses/>.
#
# -----------------------------------------------------------------------------
#
# This file contains rewritten code of snarkOS.
#

import asyncio
import traceback
from typing import Callable

import aiohttp

import explorer
# from .light_node import LightNodeState
from .types import *  # too many types


class RESTNode:
    def __init__(self, explorer_message: Callable, explorer_request: Callable):
        self.worker_task: asyncio.Task | None = None
        self.explorer_message = explorer_message
        self.explorer_request = explorer_request

    async def connect(self, _: str, __: int):
        return
        self.worker_task = asyncio.create_task(self.worker())

    async def worker(self):
        async with aiohttp.ClientSession() as session:
            while True:
                await asyncio.sleep(5)
                try:
                    async with session.get("https://vm.aleo.org/api/testnet3/latest/height") as resp:
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
                            async with session.get(f"https://vm.aleo.org/api/testnet3/blocks?start={start}&end={end}") as block_resp:
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