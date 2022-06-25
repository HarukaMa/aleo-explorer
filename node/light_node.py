import asyncio
import random
import time

from node.testnet2 import Testnet2
from node.type import ChallengeRequest, NodeType, Status, u16, u64, u128, Frame, Message, ChallengeResponse, \
    PeerRequest, Ping, PeerResponse, SocketAddr, u32, Pong, bool_, BlockLocators
from util.buffer import Buffer


class LightNodeState:
    def __init__(self):
        self.states: dict[str, dict] = {}
        self.nodes: dict[str, LightNode] = {}

    def connect(self, ip: str, port: int):
        if ip == "127.0.0.1":
            return
        key = ":".join([ip, str(port)])
        if key not in self.states:
            self.states[key] = {}
            self.nodes[key] = LightNode(ip, port, self)
            self.nodes[key].connect()

    def node_ping(self, ip: str, port: int, node_type: NodeType, status: Status, height: int, cumulative_weight: int):
        key = ":".join([ip, str(port)])
        if key in self.states:
            self.states[key]["node_type"] = node_type
            self.states[key]["status"] = status
            self.states[key]["height"] = height
            self.states[key]["cumulative_weight"] = cumulative_weight
            self.states[key]["last_ping"] = time.time()

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
        self.buffer = Buffer()
        self.ping_task: asyncio.Task | None = None
        self.worker_task: asyncio.Task | None = None

        self.peer_nonce = random.randint(0, 2 ** 64 - 1)

    def connect(self):
        self.worker_task = asyncio.create_task(self.worker(self.ip, self.port))

    async def worker(self, host: str, port: int):
        try:
            self.reader, self.writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=5)
        except Exception as e:
            await self.close()
            return
        try:
            challenge_request = ChallengeRequest(
                version=Testnet2.version,
                fork_depth=Testnet2.fork_depth,
                node_type=NodeType.Client,
                peer_status=Status.Peering,
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
            await self.close()
            return

    async def parse_message(self, frame: Frame):
        if not isinstance(frame, Frame):
            raise TypeError("frame must be instance of Frame")
        match frame.type:

            case Message.Type.ChallengeRequest:
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
                msg: ChallengeResponse = frame.message
                if msg.block_header != Testnet2.genesis_block.header:
                    raise ValueError("peer has wrong genesis block")
                await self.send_ping()

            case Message.Type.Ping:
                msg: Ping = frame.message
                height = msg.block_header.metadata.height
                cumulative_weight = msg.block_header.metadata.cumulative_weight
                self.state.node_ping(self.ip, self.port, msg.node_type, msg.status, height, cumulative_weight)
                # print(f"Peer {self.ip}:{self.port} is at block {height} (type = {msg.node_type}, status = {msg.status}, cumulative_weight = {cumulative_weight})")

                locators = {
                    u32(): (Testnet2.genesis_block.block_hash, None)
                }
                pong = Pong(
                    is_fork=bool_(),
                    block_locators=BlockLocators(block_locators=locators)
                )
                await self.send_message(pong)
                await self.send_message(PeerRequest())

            case Message.Type.Pong:
                async def ping_task():
                    await asyncio.sleep(60)
                    await self.send_ping()

                self.ping_task = asyncio.create_task(ping_task())

            case Message.Type.PeerResponse:
                msg: PeerResponse = frame.message
                for peer in msg.peer_ips:
                    peer: SocketAddr
                    self.state.connect(*peer.ip_port())

            case _:
                pass

    async def send_ping(self):
        ping = Ping(
            version=Testnet2.version,
            fork_depth=Testnet2.fork_depth,
            node_type=NodeType.Client,
            status=Status.Peering,
            block_hash=Testnet2.genesis_block.block_hash,
            block_header=Testnet2.genesis_block.header,
        )
        await self.send_message(ping)

    async def send_message(self, message: Message):
        if not issubclass(type(message), Message):
            raise TypeError("message must be subclass of Message")
        frame = Frame(type_=message.type, message=message)
        data = frame.dump()
        size = len(data)
        self.writer.write(size.to_bytes(4, "little") + data)
        try:
            await self.writer.drain()
        except:
            await self.close()

    async def close(self):
        if self.writer is not None and not self.writer.is_closing():
            self.writer.close()
            await self.writer.wait_closed()
        self.state.disconnected(self.ip, self.port)
