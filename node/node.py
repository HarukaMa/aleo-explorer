import asyncio
import random
import time
import traceback
from collections import OrderedDict
from typing import Callable

import explorer
# from .light_node import LightNodeState
from .testnet3.param import Testnet3
from .types import *  # too many types

PING_SLEEP_IN_SECS = 9


class Node:
    def __init__(self, explorer_message: Callable, explorer_request: Callable):
        self.reader, self.writer = None, None
        self.worker_task: asyncio.Task | None = None
        self.explorer_message = explorer_message
        self.explorer_request = explorer_request

        self.node_ip = None
        self.node_port = None

        # states
        self.handshake_state = 0
        self.nonce = u64(random.randint(0, 2 ** 64 - 1))
        self.peer_block_height = 0
        self.is_fork = False
        self.peer_block_locators = None
        self.block_requests = []
        self.block_requests_deadline = float('inf')
        self.ping_task = None
        self.is_syncing = False
        # self.light_node_state = light_node_state

    async def connect(self, ip: str, port: int):
        self.node_port = port
        self.node_ip = ip
        self.worker_task = asyncio.create_task(self.worker(ip, port))

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
                version=Testnet3.version,
                listener_port=u16(14133),
                node_type=NodeType.Client,
                address=Address.loads("aleo1rhgdu77hgyqd3xjj8ucu3jj9r2krwz6mnzyd80gncr5fxcwlh5rsvzp9px"),
                nonce=self.nonce,
            )
            await self.send_message(challenge_request)
            while True:
                try:
                    size = await self.reader.readexactly(4)
                except:
                    raise Exception("connection closed")
                size = int.from_bytes(size, byteorder="little")
                try:
                    frame = await self.reader.readexactly(size)
                except:
                    raise Exception("connection closed")
                await self.parse_message(Frame.load(bytearray(frame)))
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
                if self.handshake_state != 1:
                    raise Exception("handshake is not done")
                msg: BlockRequest = frame.message
                for height in range(msg.start_height, msg.end_height):
                    block = await self.explorer_request(explorer.Request.GetBlockByHeight(height))
                    print("sending block", height)
                    await self.send_message(BlockResponse(block=block))

            case Message.Type.BlockResponse:
                if self.handshake_state != 1:
                    raise Exception("handshake is not done")
                msg: BlockResponse = frame.message
                for block in msg.blocks:
                    height = block.header.metadata.height
                    if height in self.block_requests:
                        self.block_requests.remove(height)
                        await self.explorer_request(explorer.Request.ProcessBlock(block))
                if not self.block_requests:
                    self.is_syncing = False
                    self.block_requests_deadline = float('inf')
                    self.is_fork = False
                await self._sync()

            case Message.Type.ChallengeRequest:
                if self.handshake_state != 2:
                    raise Exception("incorrect handshake state")
                msg: ChallengeRequest = frame.message
                if msg.version < Testnet3.version:
                    raise ValueError("peer is outdated")
                nonce = msg.nonce
                response = ChallengeResponse(
                    genesis_header=Testnet3.genesis_block.header,
                    signature=Signature.load(bytearray(aleo.sign_nonce("APrivateKey1zkp8CZNn3yeCseEtxuVPbDCwSyhGW6yZKUYKfgXmcpoGPWH", nonce.dump()))),
                )
                self.handshake_state = 1
                await self.send_message(response)
                await self.send_ping()

                async def ping_task():
                    while True:
                        await asyncio.sleep(PING_SLEEP_IN_SECS)
                        await self.send_ping()

                self.ping_task = asyncio.create_task(ping_task())

            case Message.Type.ChallengeResponse:
                if self.handshake_state != 0:
                    raise Exception("incorrect handshake state")
                msg: ChallengeResponse = frame.message
                if msg.genesis_header != Testnet3.genesis_block.header and not await self.explorer_request(explorer.Request.GetDevMode()):
                    raise ValueError("peer has wrong genesis block")
                self.handshake_state = 2

            case Message.Type.Ping:
                if self.handshake_state != 1:
                    raise Exception("handshake is not done")
                msg: Ping = frame.message
                locators = msg.block_locators.value
                is_fork = bool_()
                if locators is None:
                    is_fork = None
                else:
                    locators: BlockLocators
                    recents: dict[u32, BlockHash] = locators.recents
                    if not recents:
                        raise ValueError("invalid block locator: recents is empty")
                    if len(recents) > Testnet3.block_locator_num_recents:
                        raise ValueError("invalid block locator: recents is too long")
                    latest_recents_height = 0
                    for i, (height, block_hash) in enumerate(recents.items()):
                        if i == 0 and len(recents) < Testnet3.block_locator_num_recents and height != 0:
                            raise ValueError("invalid block locator: first height must be 0")
                        if i > 0 and height != latest_recents_height + Testnet3.block_locator_recent_interval:
                            raise ValueError("invalid block locator: recent heights must be in sequence")
                        latest_recents_height = height

                    checkpoints: dict[u32, BlockHash] = locators.checkpoints
                    if not checkpoints:
                        raise ValueError("invalid block locator: checkpoints is empty")
                    latest_checkpoints_height = 0
                    for i, (height, block_hash) in enumerate(checkpoints.items()):
                        if i == 0 and height != 0:
                            raise ValueError("invalid block locator: first height must be 0")
                        if i > 0 and height != latest_checkpoints_height + Testnet3.block_locator_checkpoint_interval:
                            raise ValueError("invalid block locator: checkpoint heights must be in sequence")
                        latest_checkpoints_height = height

                    # skipping other checks

                    latest_height = await self.explorer_request(explorer.Request.GetLatestHeight())
                    common_ancestor = 0
                    if latest_height in recents:
                        common_ancestor = latest_height
                        remote_hash = recents[latest_height]
                    elif latest_height // 10000 in checkpoints:
                        common_ancestor = latest_height // 10000
                        remote_hash = checkpoints[latest_height // 10000]
                    else:
                        remote_hash = checkpoints[u32()]

                    local_hash = await self.explorer_request(explorer.Request.GetBlockHashByHeight(common_ancestor))
                    if local_hash != remote_hash and not await self.explorer_request(explorer.Request.GetDevMode()):
                        is_fork = bool_(True)
                        raise ValueError("peer is on a fork")

                    self.peer_block_locators = locators
                    self.is_fork = is_fork == True

                pong = Pong(
                    is_fork=Option[bool_](is_fork),
                )
                await self.send_message(pong)
                if not self.is_syncing:
                    await self._sync()

            case Message.Type.Pong:
                if self.handshake_state != 1:
                    raise Exception("handshake is not done")
                msg: Pong = frame.message
                if msg.is_fork.value is not None:
                    if msg.is_fork.value:
                        raise ValueError("peer think we are on fork")

            case Message.Type.Disconnect:
                msg: Disconnect = frame.message
                print("Disconnected:", msg.reason)

            case _:
                print("unhandled message type:", frame.type)

    async def _sync(self):
        if self.block_requests_deadline < time.time():
            self.block_requests.clear()
            self.block_requests_deadline = float("inf")
            self.is_syncing = False
        if self.is_syncing:
            next_block = self.block_requests[0]
            self.block_requests_deadline = time.time() + 30
            msg = BlockRequest(start_height=u32(next_block), end_height=u32(next_block + 100))
            await self.send_message(msg)
        else:
            locators = self.peer_block_locators
            if locators is None:
                return
            recents = locators.recents
            self.peer_block_height = max(recents.keys())
            latest_height = await self.explorer_request(explorer.Request.GetLatestHeight())
            if latest_height == self.peer_block_height:
                return

            start_block_height = latest_height + 1
            end_block_height = min(self.peer_block_height, start_block_height)
            print(f"Synchronizing from block {start_block_height} to {end_block_height}")
            self.is_syncing = True

            self.block_requests.extend(range(start_block_height, end_block_height + 100))
            self.block_requests_deadline = time.time() + 30
            msg = BlockRequest(start_height=u32(start_block_height), end_height=u32(end_block_height + 100))
            await self.send_message(msg)

    async def send_ping(self):
        ping = Ping(
            version=Testnet3.version,
            node_type=NodeType.Client,
            block_locators=Option[BlockLocators](None)
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
        self.nonce = u64(random.randint(0, 2 ** 64 - 1))
        self.peer_block_height = 0
        self.peer_cumulative_weight = 0
        self.is_fork = False
        self.peer_block_locators = OrderedDict()
        self.block_requests = []
        self.block_requests_deadline = float('inf')
        self.is_syncing = False
        if self.ping_task is not None:
            self.ping_task.cancel()
        await asyncio.sleep(5)
        self.worker_task = asyncio.create_task(self.worker(self.node_ip, self.node_port))
