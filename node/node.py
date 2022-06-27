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
import random
import time
import traceback
from collections import OrderedDict
from typing import Callable

from more_itertools.recipes import take

import explorer
from util.buffer import Buffer
from .light_node import LightNodeState
from .testnet2.param import Testnet2
from .type import *  # too many types


class Node:
    def __init__(self, explorer_message: Callable, explorer_request: Callable, light_node_state: LightNodeState):
        self.reader, self.writer = None, None
        self.buffer = Buffer()
        self.worker_task: asyncio.Task | None = None
        self.explorer_message = explorer_message
        self.explorer_request = explorer_request

        self.node_ip = None
        self.node_port = None

        # states
        self.handshake_state = 0
        # noinspection PyArgumentList
        self.peer_nonce = random.randint(0, 2 ** 64 - 1)
        self.status = Status.Peering
        self.peer_block_height = 0
        self.peer_cumulative_weight = 0
        self.is_fork = False
        self.peer_block_locators = OrderedDict()
        self.block_requests = []
        self.block_requests_deadline = float('inf')
        self.ping_task = None
        self.light_node_state = light_node_state

    async def connect(self, ip: str, port: int):
        self.node_port = port
        self.node_ip = ip
        self.worker_task = asyncio.create_task(self.worker(ip, port))
        await asyncio.start_server(self.listen_worker, host="0.0.0.0", port=14132)

    async def listen_worker(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        # print(f"Connected from {writer.get_extra_info('peername')}")
        buffer = Buffer()
        while True:
            data = await reader.read(4096)
            if not data:
                break
            buffer.write(data)
            while buffer.count():
                if buffer.count() >= 4:
                    size = int.from_bytes(buffer.peek(4), byteorder="little")
                    if buffer.count() < size + 4:
                        break
                    buffer.read(4)
                    frame = Frame.load(buffer.read(size))
                    if frame.type != Message.Type.ChallengeRequest:
                        writer.close()
                        await writer.wait_closed()
                    msg: ChallengeRequest = frame.message
                    if msg.version < Testnet2.version:
                        raise ValueError("peer is outdated")
                    if msg.fork_depth != Testnet2.fork_depth:
                        raise ValueError("peer has wrong fork depth")
                    response = ChallengeResponse(
                        block_header=Testnet2.genesis_block.header,
                    )
                    frame = Frame(type_=response.type, message=response)
                    data = frame.dump()
                    size = len(data)
                    writer.write(size.to_bytes(4, "little") + data)
                    try:
                        await writer.drain()
                    except:
                        pass

    async def worker(self, host: str, port: int):
        try:
            self.reader, self.writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=5)
        except asyncio.TimeoutError as e:
            await self.explorer_message(explorer.Message(explorer.Message.Type.NodeConnectError, e))
            await self.close()
            return
        except Exception as e:
            await self.explorer_message(explorer.Message(explorer.Message.Type.NodeConnectError, e))
            await self.close()
            return
        await self.explorer_message(explorer.Message(explorer.Message.Type.NodeConnected, None))
        try:
            challenge_request = ChallengeRequest(
                version=Testnet2.version,
                fork_depth=Testnet2.fork_depth,
                node_type=NodeType.Client,
                peer_status=self.status,
                listener_port=u16(14132),
                peer_nonce=u64(self.peer_nonce),
                peer_cumulative_weight=u128(),
            )
            await self.send_message(challenge_request)
            while True:
                data = await self.reader.read(4096)
                if not data:
                    raise Exception("connection closed")
                self.buffer.write(data)
                while self.buffer.count():
                    if self.buffer.count() >= 4:
                        size = int.from_bytes(self.buffer.peek(4), byteorder="little")
                        if self.buffer.count() < size + 4:
                            break
                        self.buffer.read(4)
                        frame = self.buffer.read(size)
                        await self.parse_message(Frame.load(frame))
                    else:
                        print(f"buffer.count() < 4: {self.buffer.count()}")
                        break
        except Exception:
            traceback.print_exc()
            await self.explorer_message(explorer.Message(explorer.Message.Type.NodeDisconnected, None))
            await self.close()
            return

    async def parse_message(self, frame: Frame):
        if not isinstance(frame, Frame):
            raise TypeError("frame must be instance of Frame")
        match frame.type:
            case Message.Type.BlockRequest:
                msg: BlockRequest = frame.message
                for height in range(msg.start_block_height, msg.end_block_height):
                    block = await self.explorer_request(explorer.Request.GetBlockByHeight(height))
                    print("sending block", height)
                    await self.send_message(BlockResponse(block=block))

            case Message.Type.BlockResponse:
                if self.handshake_state != 1:
                    raise Exception("handshake is not done")
                msg: BlockResponse = frame.message
                block = msg.block
                self.block_requests.remove(block.header.metadata.height)
                await self.explorer_request(explorer.Request.ProcessBlock(block))
                if not self.block_requests:
                    self.status = Status.Ready
                    self.block_requests_deadline = float('inf')
                    await self._sync()

            case Message.Type.ChallengeRequest:
                if self.handshake_state != 0:
                    raise Exception("handshake is already done")
                msg: ChallengeRequest = frame.message
                if msg.version < Testnet2.version:
                    raise ValueError("peer is outdated")
                if msg.fork_depth != Testnet2.fork_depth:
                    raise ValueError("peer has wrong fork depth")
                response = ChallengeResponse(
                    block_header=Testnet2.genesis_block.header,
                )
                await self.send_message(response)

            case Message.Type.ChallengeResponse:
                if self.handshake_state != 0:
                    raise Exception("handshake is already done")
                msg: ChallengeResponse = frame.message
                if msg.block_header != Testnet2.genesis_block.header:
                    raise ValueError("peer has wrong genesis block")
                self.handshake_state = 1
                self.status = Status.Ready
                await self.send_ping()

            case Message.Type.Ping:
                if self.handshake_state != 1:
                    raise Exception("handshake is not done")
                msg: Ping = frame.message
                peer_height = msg.block_header.metadata.height
                print("peer height:", peer_height)
                block_hash = await self.explorer_request(explorer.Request.GetBlockHashByHeight(peer_height))
                if block_hash is None:
                    is_fork = None
                elif block_hash == msg.block_hash:
                    is_fork = bool_()
                else:
                    is_fork = bool_(True)
                latest_height = await self.explorer_request(explorer.Request.GetLatestHeight())
                num_block_headers = min(Testnet2.maximum_linear_block_locators, latest_height)
                locators = {}
                for i in range(num_block_headers):
                    height = latest_height - i
                    block_hash = await self.explorer_request(explorer.Request.GetBlockHashByHeight(height))
                    block_header = await self.explorer_request(explorer.Request.GetBlockHeaderByHeight(height))
                    locators[u32(height)] = (block_hash, block_header)
                locator_height = latest_height - num_block_headers
                accumulator = 1
                while locator_height > 0:
                    block_hash = await self.explorer_request(explorer.Request.GetBlockHashByHeight(locator_height))
                    locators[u32(locator_height)] = (block_hash, None)
                    locator_height -= accumulator
                    accumulator *= 2
                locators[u32()] = (Testnet2.genesis_block.block_hash, None)
                pong = Pong(
                    is_fork=is_fork,
                    block_locators=BlockLocators(block_locators=locators)
                )
                await self.send_message(pong)
                await self.send_message(PeerRequest())

            case Message.Type.Pong:
                if self.handshake_state != 1:
                    raise Exception("handshake is not done")
                msg: Pong = frame.message

                if len(msg.block_locators) == 0:
                    raise ValueError("block locators is empty")
                if msg.block_locators[0][0] != Testnet2.genesis_block.block_hash:
                    raise ValueError("incorrect genesis block")

                num_linear_block_headers = min(Testnet2.maximum_linear_block_locators, len(msg.block_locators) - 1)
                num_quadratic_block_headers = len(msg.block_locators) - 1 - num_linear_block_headers
                self.peer_block_height = last_block_height = max(msg.block_locators.keys())
                block_locators = OrderedDict(sorted(msg.block_locators.items(), key=lambda k: k[0], reverse=True))
                linear_block_locators = take(num_linear_block_headers, block_locators.items())
                block_header: BlockHeader | None
                for block_height, (block_hash, block_header) in linear_block_locators:
                    if last_block_height != block_height:
                        raise ValueError("block locators is not linear")
                    last_block_height -= 1
                    if block_header is None:
                        raise ValueError("block header is missing")
                    if block_header.metadata.height != block_height:
                        raise ValueError("block header has wrong height")
                if len(block_locators) > Testnet2.maximum_linear_block_locators:
                    previous_block_height = -1
                    accumulator = 1
                    quadratic_block_locators = take(num_linear_block_headers + num_quadratic_block_headers,
                                                    block_locators.items())[num_linear_block_headers + 1:]
                    for block_height, (block_hash, block_header) in quadratic_block_locators:
                        if previous_block_height != -1 and previous_block_height - accumulator != block_height:
                            raise ValueError("block locators is not quadratic")
                        if block_header is not None:
                            raise ValueError("block header is not None")
                        previous_block_height = block_height
                        accumulator *= 2

                common_ancestor = 0
                latest_block_height_of_peer = 0
                self.peer_block_locators = OrderedDict(sorted(msg.block_locators.items(), key=lambda k: k[0]))
                for block_height, (block_hash, _) in self.peer_block_locators.items():
                    expected_block_hash = await self.explorer_request(
                        explorer.Request.GetBlockHashByHeight(block_height))
                    if expected_block_hash is not None:
                        if block_hash != expected_block_hash:
                            continue
                        if block_height > common_ancestor:
                            common_ancestor = block_height
                    if block_height > latest_block_height_of_peer:
                        latest_block_height_of_peer = block_height

                if msg.is_fork is not None:
                    self.is_fork = msg.is_fork == True  # convert back to bool
                elif common_ancestor == latest_block_height_of_peer or common_ancestor == await self.explorer_request(
                        explorer.Request.GetLatestHeight()):
                    self.is_fork = False
                else:
                    self.is_fork = None

                self.peer_cumulative_weight = self.peer_block_locators[latest_block_height_of_peer][
                    1].metadata.cumulative_weight

                print(
                    f"Peer is at block {latest_block_height_of_peer} (is_fork = {self.is_fork}, cumulative_weight = {self.peer_cumulative_weight}, common_ancestor = {common_ancestor})")

                async def ping_task():
                    await asyncio.sleep(60)
                    await self.send_ping()

                self.ping_task = asyncio.create_task(ping_task())
                asyncio.create_task(self._sync())

            case Message.Type.UnconfirmedBlock:
                if self.handshake_state != 1:
                    raise Exception("handshake is not done")
                msg: UnconfirmedBlock = frame.message
                await self.explorer_request(explorer.Request.ProcessBlock(msg.block))

            case Message.Type.PeerResponse:
                msg: PeerResponse = frame.message
                for peer in msg.peer_ips:
                    peer: SocketAddr
                    self.light_node_state.connect(*peer.ip_port())

            case Message.Type.Disconnect:
                msg: Disconnect = frame.message
                print("Disconnected:", msg.reason)

            case _:
                print("unhandled message type:", frame.type)

    async def _sync(self):
        if self.block_requests_deadline < time.time():
            self.block_requests.clear()
            self.block_requests_deadline = float("inf")
            self.status = Status.Ready
        if self.status != Status.Syncing:
            common_ancestor = 0
            first_deviating_locator = None
            for block_height, (block_hash, _) in self.peer_block_locators.items():
                expected_block_hash = await self.explorer_request(
                    explorer.Request.GetBlockHashByHeight(block_height))
                if block_hash != expected_block_hash:
                    if first_deviating_locator is None:
                        first_deviating_locator = block_height
                    else:
                        if block_height < first_deviating_locator:
                            first_deviating_locator = block_height
                else:
                    if block_height > common_ancestor:
                        common_ancestor = block_height
            latest_block_height = await self.explorer_request(explorer.Request.GetLatestHeight())
            latest_cumulative_weight = await self.explorer_request(explorer.Request.GetLatestWeight())
            if latest_cumulative_weight >= self.peer_cumulative_weight:
                return
            if latest_block_height < common_ancestor:
                return
            if not self.is_fork:
                ledger_is_on_fork = False
                if first_deviating_locator is None:
                    latest_common_ancestor = common_ancestor
                else:
                    latest_common_ancestor = latest_block_height
            else:
                if latest_block_height - common_ancestor <= Testnet2.fork_depth:
                    print(
                        f"Discovered a canonical chain with common ancestor {common_ancestor} and cumulative weight {self.peer_cumulative_weight}")
                    latest_common_ancestor = common_ancestor
                    ledger_is_on_fork = latest_block_height != common_ancestor
                elif first_deviating_locator is not None:
                    if latest_block_height - first_deviating_locator > Testnet2.fork_depth:
                        print("Peer exceeded the permitted fork range")
                        return
                    else:
                        print(
                            f"Discovered a potentially better canonical chain with common ancestor {common_ancestor} and cumulative weight {self.peer_cumulative_weight}")
                        latest_common_ancestor = common_ancestor
                        ledger_is_on_fork = True
                else:
                    print("Peer is missing first deviating locator")
                    return
            self.status = Status.Syncing

            if ledger_is_on_fork:
                print(f"Reverting to common ancestor {latest_common_ancestor}")
                await self.explorer_request(explorer.Request.RevertToBlock(common_ancestor))

            number_of_block_requests = min(self.peer_block_height - latest_common_ancestor, 250)
            start_block_height = latest_common_ancestor + 1
            end_block_height = start_block_height + number_of_block_requests - 1
            print(f"Synchronizing from block {start_block_height} to {end_block_height}")

            self.block_requests.extend(range(start_block_height, end_block_height + 1))
            self.block_requests_deadline = time.time() + 30
            msg = BlockRequest(start_block_height=u32(start_block_height), end_block_height=u32(end_block_height))
            await self.send_message(msg)

    async def send_ping(self):
        latest_height = await self.explorer_request(explorer.Request.GetLatestHeight())
        ping = Ping(
            version=Testnet2.version,
            fork_depth=Testnet2.fork_depth,
            node_type=NodeType.Client,
            status=self.status,
            block_hash=await self.explorer_request(explorer.Request.GetBlockHashByHeight(latest_height)),
            block_header=await self.explorer_request(explorer.Request.GetBlockHeaderByHeight(latest_height)),
        )
        await self.send_message(ping)

    async def send_message(self, message: Message):
        if not issubclass(type(message), Message):
            raise TypeError("message must be subclass of Message")
        frame = Frame(type_=message.type, message=message)
        data = frame.dump()
        size = len(data)
        self.writer.write(size.to_bytes(4, "little") + data)
        await self.writer.drain()

    async def close(self):
        if self.writer is not None and not self.writer.is_closing():
            self.writer.close()
            await self.writer.wait_closed()
        # reset states
        self.handshake_state = 0
        # noinspection PyArgumentList
        self.peer_nonce = random.randint(0, 2 ** 64 - 1)
        self.status = Status.Peering
        self.peer_block_height = 0
        self.peer_cumulative_weight = 0
        self.is_fork = False
        self.peer_block_locators = OrderedDict()
        self.block_requests = []
        self.block_requests_deadline = float('inf')
        if self.ping_task is not None:
            self.ping_task.cancel()
        await asyncio.sleep(5)
        self.worker_task = asyncio.create_task(self.worker(self.node_ip, self.node_port))
