import asyncio
import random
import time
from io import BytesIO

import aleo_explorer_rust
import requests

from aleo_types import ChallengeRequest, NodeType, u16, u64, Frame, Message, ChallengeResponse, \
    PeerRequest, Ping, PeerResponse, Pong, bool_, BlockLocators, Address, Signature, Option, Data
from .testnet3 import Testnet3


class LightNodeState:
    def __init__(self):
        self.states: dict[str, dict] = {}
        self.nodes: dict[str, LightNode] = {}

        # prevent infinite self connection loop
        r = requests.get("https://api.ipify.org/?format=json")
        self.self_ip = r.json()["ip"]

    def connect(self, ip: str, port: int):
        if ip == self.self_ip and port == 14133:
            return
        key = ":".join([ip, str(port)])
        if key not in self.states:
            self.states[key] = {}
            self.nodes[key] = LightNode(ip, port, self)
            self.nodes[key].connect()

    def node_connected(self, ip: str, port: int, address: str):
        key = ":".join([ip, str(port)])
        print(f"light node connected: {key}")
        if key in self.states:
            self.states[key]["address"] = address
            self.states[key]["last_ping"] = time.time()

    def node_ping(self, ip: str, port: int, node_type: NodeType, height: int):
        key = ":".join([ip, str(port)])
        if key in self.states:
            self.states[key]["last_ping"] = time.time()
            self.states[key]["node_type"] = node_type
            self.states[key]["height"] = height

    def disconnected(self, ip: str, port: int):
        key = ":".join([ip, str(port)])
        if key in self.states:
            del self.states[key]
            del self.nodes[key]


class LightNode:
    def __init__(self, ip: str, port: int, state: LightNodeState):
        self.ip = ip
        self.port = port
        self.state = state

        self.reader, self.writer = None, None
        self.ping_task: asyncio.Task | None = None
        self.worker_task: asyncio.Task | None = None

        self.nonce = u64(random.randint(0, 2 ** 64 - 1))

    def connect(self):
        self.worker_task = asyncio.create_task(self.worker(self.ip, self.port))

    async def worker(self, host: str, port: int):
        try:
            self.reader, self.writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=5)
        except Exception:
            await self.close()
            return
        try:
            challenge_request = ChallengeRequest(
                version=Testnet3.version,
                listener_port=u16(14134),
                node_type=NodeType.Prover,
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
                await self.parse_message(Frame.load(BytesIO(frame)))
        except Exception as e:
            await self.close()
            return

    async def parse_message(self, frame: Frame):

        if isinstance(frame.message, ChallengeRequest):
            msg = frame.message
            if msg.version < Testnet3.version:
                raise ValueError("peer is outdated")
            nonce = msg.nonce
            self.state.node_connected(self.ip, self.port, str(msg.address))
            response = ChallengeResponse(
                genesis_header=Testnet3.genesis_block.header,
                signature=Data[Signature](Signature.load(BytesIO(bytes(aleo_explorer_rust.sign_nonce("APrivateKey1zkp8CZNn3yeCseEtxuVPbDCwSyhGW6yZKUYKfgXmcpoGPWH", nonce.dump()))))),
            )
            await self.send_message(response)
            await self.send_ping()

            async def ping_task():
                while True:
                    await asyncio.sleep(5)
                    await self.send_ping()

            self.ping_task = asyncio.create_task(ping_task())

        elif isinstance(frame.message, ChallengeResponse):
            msg = frame.message
            if msg.genesis_header.transactions_root != Testnet3.genesis_block.header.transactions_root:
                raise ValueError("peer has wrong genesis block")

        elif isinstance(frame.message, Ping):
            msg = frame.message
            locators = msg.block_locators.value
            if locators is not None:
                locators: BlockLocators
                height = max(locators.recents.keys())
            else:
                height = None
            self.state.node_ping(self.ip, self.port, msg.node_type, height)
            # print(f"Peer {self.ip}:{self.port} is at block {height} (type = {msg.node_type})")

            pong = Pong(
                is_fork=Option[bool_](None),
            )
            await self.send_message(pong)
            await self.send_message(PeerRequest())

        # case Message.Type.Pong:
        #     msg: Pong = frame.message
        #
        #     latest_block_height_of_peer = 0
        #     peer_block_locators = OrderedDict(sorted(msg.block_locators.items(), key=lambda k: k[0]))
        #     for block_height, (block_hash, _) in peer_block_locators.items():
        #         if block_height > latest_block_height_of_peer:
        #             latest_block_height_of_peer = block_height
        #
        #     peer_cumulative_weight = peer_block_locators[latest_block_height_of_peer][1].metadata.cumulative_weight
        #     self.state.node_pong(self.ip, self.port, latest_block_height_of_peer, peer_cumulative_weight)

        elif isinstance(frame.message, PeerResponse):
            msg = frame.message
            for peer in msg.peers:
                self.state.connect(*peer.ip_port())

        else:
            pass

    async def send_ping(self):
        ping = Ping(
            version=Testnet3.version,
            node_type=NodeType.Prover,
            block_locators=Option[BlockLocators](None),
        )
        await self.send_message(ping)

    async def send_message(self, message: Message):
        if self.writer is None:
            raise Exception("connection is not established")
        frame = Frame(message=message)
        data = frame.dump()
        size = len(data)
        self.writer.write(size.to_bytes(4, "little") + data)
        await self.writer.drain()

    async def close(self):
        self.state.disconnected(self.ip, self.port)
        if self.ping_task is not None:
            self.ping_task.cancel()
        if self.writer is not None and not self.writer.is_closing():
            self.writer.close()
            await self.writer.wait_closed()
