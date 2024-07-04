import asyncio
import random
import time
from io import BytesIO
from typing import Optional, Any, cast

import aiohttp
import aleo_explorer_rust
import requests

from aleo_types import ChallengeRequest, NodeType, u16, u64, Frame, Message, ChallengeResponse, \
    PeerRequest, Ping, PeerResponse, Pong, bool_, BlockLocators, Address, Signature, Option, Data
from . import Network


class LightNodeState:
    def __init__(self):
        self.states: dict[str, dict[str, Any]] = {}
        self.nodes: dict[str, LightNode] = {}
        self.last_connect_attempt: dict[str, float] = {}

        # prevent infinite self connection loop
        r = requests.get("https://api.ipify.org/?format=json")
        self.self_ip = r.json()["ip"]
        print(f"self ip: {self.self_ip}")
        self.listener = LightNodeListener(self)

    def start_listener(self):
        self.listener.start()

    def connect(self, ip: str, port: int, node_type: Optional[NodeType]):
        if ip == self.self_ip and port == 14133:
            return
        key = ":".join([ip, str(port)])
        if key not in self.states:
            self.states[key] = {
                "last_ping": time.time(),
                "node_type": node_type,
                "direction": "disconnected",
            }
            self.nodes[key] = LightNode(self)
            self.nodes[key].connect(ip, port)
            self.last_connect_attempt[key] = time.time()
        else:
            connected = key in self.nodes
            if not connected:
                if node_type is not None:
                    self.states[key]["last_ping"] = time.time()
                    self.states[key]["node_type"] = node_type
                if time.time() - self.last_connect_attempt[key] > 60:
                    self.nodes[key] = LightNode(self)
                    self.nodes[key].connect(ip, port)
                    self.last_connect_attempt[key] = time.time()
                    self.states[key]["last_ping"] = time.time()

    def incoming(self, ip: str, port: int, node: "LightNode"):
        key = ":".join([ip, str(port)])
        if key not in self.states:
            self.states[key] = {
                "last_ping": time.time(),
                "node_type": None,
                "direction": "disconnected",
            }
            self.nodes[key] = node
            self.last_connect_attempt[key] = time.time()

    def node_connected(self, ip: str, port: int, address: str, is_incoming: bool):
        key = ":".join([ip, str(port)])
        if key in self.states:
            self.states[key]["address"] = address
            self.states[key]["last_ping"] = time.time()
            if is_incoming:
                self.states[key]["direction"] = "incoming"
            else:
                self.states[key]["direction"] = "outgoing"

    def node_ping(self, ip: str, port: int, node_type: NodeType, height: Optional[int]):
        key = ":".join([ip, str(port)])
        if key in self.states:
            self.states[key]["last_ping"] = time.time()
            self.states[key]["node_type"] = node_type
            self.states[key]["height"] = height

    def node_peer_count(self, ip: str, port: int, peer_count: int):
        key = ":".join([ip, str(port)])
        if key in self.states:
            self.states[key]["peer_count"] = peer_count

    def disconnected(self, ip: str, port: int):
        key = ":".join([ip, str(port)])
        if key in self.states:
            if key in self.nodes:
                del self.nodes[key]
            self.states[key]["direction"] = "disconnected"

    def cleanup(self):
        outdated: list[str] = []
        for k, v in self.states.items():
            if time.time() - v["last_ping"] > 300:
                outdated.append(k)
        for k in outdated:
            del self.states[k]
            if k in self.nodes:
                self.nodes[k].close_outdated()
                del self.nodes[k]

class LightNode:
    def __init__(self, state: LightNodeState, is_incoming: bool = False):
        self.state = state
        self.is_incoming = is_incoming

        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None
        self.ping_task: Optional[asyncio.Task[None]] = None
        self.worker_task: Optional[asyncio.Task[None]] = None
        self.ip: str
        self.port: int

        self.aiohttp_session: Optional[aiohttp.ClientSession] = None
        self.last_rest_query = 0

        self.nonce = u64(random.randint(0, 2 ** 64 - 1))

    def incoming(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.ip = writer.get_extra_info("peername")[0]
        self.port = writer.get_extra_info("peername")[1]
        self.worker_task = asyncio.create_task(self.__incoming_worker(reader, writer))

    def connect(self, ip: str, port: int):
        self.ip = ip
        self.port = port
        self.worker_task = asyncio.create_task(self.__worker(self.ip, self.port))

    async def __worker(self, host: str, port: int):
        try:
            self.reader, self.writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=5)
        except Exception:
            await self.close()
            return
        try:
            challenge_request = ChallengeRequest(
                version=Network.version,
                listener_port=u16(14134),
                node_type=NodeType.Prover,
                address=Address.loads("aleo1rhgdu77hgyqd3xjj8ucu3jj9r2krwz6mnzyd80gncr5fxcwlh5rsvzp9px"),
                nonce=self.nonce,
            )
            await self.send_message(challenge_request)
            self.aiohttp_session = aiohttp.ClientSession(f"http://{self.ip}:3030", timeout=aiohttp.ClientTimeout(total=1))
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
        except Exception:
            await self.close()
            return

    async def __incoming_worker(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.reader = reader
        self.writer = writer
        try:
            self.aiohttp_session = aiohttp.ClientSession(f"http://{self.ip}:3030", timeout=aiohttp.ClientTimeout(total=1))
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
        except Exception:
            await self.close()
            return

    async def ping_task_func(self):
        while True:
            await asyncio.sleep(5)
            await self.send_ping()

    async def parse_message(self, frame: Frame):

        if isinstance(frame.message, ChallengeRequest):
            msg = frame.message
            if msg.version < Network.version:
                raise ValueError("peer is outdated")
            if self.is_incoming:
                self.port = int(msg.listener_port)
                self.state.incoming(self.ip, self.port, self)
            self.state.node_connected(self.ip, self.port, str(msg.address), self.is_incoming)
            resp_nonce = u64(random.randint(0, 2 ** 64 - 1))
            response = ChallengeResponse(
                genesis_header=Network.genesis_block.header,
                restrictions_id=Network.restrictions_id,
                signature=Data[Signature](Signature.load(BytesIO(aleo_explorer_rust.sign_nonce("APrivateKey1zkp8CZNn3yeCseEtxuVPbDCwSyhGW6yZKUYKfgXmcpoGPWH", msg.nonce.dump() + resp_nonce.dump())))),
                nonce=resp_nonce,
            )
            await self.send_message(response)

            if not self.is_incoming:
                await self.send_ping()
                self.ping_task = asyncio.create_task(self.ping_task_func())
            else:
                challenge_request = ChallengeRequest(
                    version=Network.version,
                    listener_port=u16(14134),
                    node_type=NodeType.Prover,
                    address=Address.loads("aleo1rhgdu77hgyqd3xjj8ucu3jj9r2krwz6mnzyd80gncr5fxcwlh5rsvzp9px"),
                    nonce=self.nonce,
                )
                await self.send_message(challenge_request)

        elif isinstance(frame.message, ChallengeResponse):
            if self.is_incoming:
                await self.send_ping()
                self.ping_task = asyncio.create_task(self.ping_task_func())

        elif isinstance(frame.message, Ping):
            msg = frame.message
            locators = msg.block_locators.value
            if locators is not None:
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
            self.state.node_peer_count(self.ip, self.port, len(msg.peers))
            peer_types: dict[str, NodeType] = {}
            if time.time() - self.last_rest_query > 300:
                self.last_rest_query = time.time()
                try:
                    r = await cast(aiohttp.ClientSession, self.aiohttp_session).get("/testnet/peers/all/metrics")
                    if r.ok:
                        data = await r.json()
                        for p in data:
                            peer_types[p[0]] = NodeType[p[1]]
                except Exception:
                    pass
            for peer in msg.peers:
                if str(peer) in peer_types:
                    peer_type = peer_types[str(peer)]
                else:
                    peer_type = None
                self.state.connect(str(peer.ip), peer.port, peer_type)

        else:
            pass

    async def send_ping(self):
        ping = Ping(
            version=Network.version,
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
        if self.aiohttp_session is not None:
            await self.aiohttp_session.close()

    async def close_session(self):
        if self.aiohttp_session is not None:
            await self.aiohttp_session.close()

    def close_outdated(self):
        if self.worker_task is not None:
            self.worker_task.cancel()
        if self.ping_task is not None:
            self.ping_task.cancel()
        if self.writer is not None and not self.writer.is_closing():
            self.writer.close()
        asyncio.create_task(self.close_session())


class LightNodeListener:
    def __init__(self, state: LightNodeState):
        self.state = state
        self.listen_task: Optional[asyncio.Task[asyncio.Server]] = None

    def start(self):
        print("Starting light node listener")
        self.listen_task = asyncio.create_task(asyncio.start_server(self.incoming, host="0.0.0.0", port=14134))

    async def incoming(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        node = LightNode(self.state, is_incoming=True)
        node.incoming(reader, writer)




