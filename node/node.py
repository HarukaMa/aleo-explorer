import asyncio
import random
import traceback
from collections import OrderedDict
from typing import Callable

from more_itertools.recipes import take

import explorer
from node.type import *  # too many types
from util.buffer import Buffer
from .testnet2.param import Testnet2


class Node:
    def __init__(self, explorer_message: Callable, explorer_request: Callable):
        self.reader, self.writer = None, None
        self.buffer = Buffer()
        self.worker_task: asyncio.Task | None = None
        self.explorer_message = explorer_message
        self.explorer_request = explorer_request

        # states
        self.handshake_state = 0
        # noinspection PyArgumentList
        self.peer_nonce = random.randint(0, 2 ** 64 - 1)
        self.status = Status.Peering

    def connect(self, ip: str, port: int):
        self.worker_task = asyncio.create_task(self.worker(ip, port))

    async def worker(self, host: str, port: int):
        try:
            self.reader, self.writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=5)
        except asyncio.TimeoutError as e:
            await self.explorer_message(explorer.Message(explorer.Message.Type.NodeConnectError, e))
            return
        except Exception as e:
            await self.explorer_message(explorer.Message(explorer.Message.Type.NodeConnectError, e))
            return
        await self.explorer_message(explorer.Message(explorer.Message.Type.NodeConnected, None))
        try:
            challenge_request = ChallengeRequest(
                version=Testnet2.version,
                fork_depth=Testnet2.fork_depth,
                node_type=NodeType.Client,
                peer_status=self.status,
                listener_port=u16(),
                peer_nonce=u64(self.peer_nonce),
                peer_cumulative_weight=u128(),
            )
            await self.send_message(challenge_request)
            while True:
                data = await self.reader.read(4096)
                if not data:
                    break
                self.buffer.write(data)
                while self.buffer.count():
                    if self.buffer.count() >= 4:
                        size = int.from_bytes(self.buffer.peek(4), byteorder="little")
                        if self.buffer.count() < size + 4:
                            break
                        self.buffer.read(4)
                        frame = self.buffer.read(size)
                        await self.parse_message(Frame.load(frame))
        except Exception:
            traceback.print_exc()
            await self.explorer_message(explorer.Message(explorer.Message.Type.NodeDisconnected, None))
            await self.close()
            return

    async def parse_message(self, frame: Frame):
        if not isinstance(frame, Frame):
            raise TypeError("frame must be instance of Frame")
        match frame.type:
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
                self.status = Status.Syncing
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
                    is_fork = False
                else:
                    is_fork = True
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
                last_block_height = max(msg.block_locators.keys())
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
                block_locators = OrderedDict(sorted(msg.block_locators.items(), key=lambda k: k[0]))
                for block_height, (block_hash, _) in block_locators.items():
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
                    is_fork = msg.is_fork
                elif common_ancestor == latest_block_height_of_peer or common_ancestor == await self.explorer_request(
                        explorer.Request.GetLatestHeight()):
                    is_fork = bool_()
                else:
                    is_fork = None

                print(
                    f"Peer is at block {latest_block_height_of_peer} (is_fork = {is_fork}, cumulative_weight = {block_locators[latest_block_height_of_peer][1].metadata.cumulative_weight}, common_ancestor = {common_ancestor})")

                async def task():
                    await asyncio.sleep(60)
                    await self.send_ping()

                asyncio.create_task(task())

            case _:
                print("unhandled message type:", frame.type)

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
        self.writer.close()
        await self.writer.wait_closed()

    async def get_block(self, height: int):
        request = GetBlockRequest.init(
            height=u32(height),
        )
        await self.send_message(request)
