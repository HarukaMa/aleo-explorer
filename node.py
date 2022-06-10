import asyncio
import random
import traceback

from buffer import Buffer
from type import * # too many types


class Testnet2:
    version = 12
    fork_depth = 4096

    genesis_block = Block.load(bytearray(open("testnet2/block.genesis", "rb").read()))

class Node:
    def __init__(self, explorer):
        self.reader, self.writer = None, None
        self.buffer = Buffer()
        self.worker_task = None
        self.explorer = explorer

        # states
        self.handshake_state = 0
        self.peer_nonce = random.randint(0, 2 ** 64 - 1)

    def connect(self, ip: str, port: int):
        self.worker_task = asyncio.create_task(self.worker(ip, port))

    async def worker(self, host: str, port: int):
        from explorer import ExplorerMessage
        try:
            self.reader, self.writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=5)
        except asyncio.TimeoutError as e:
            await self.explorer.message(ExplorerMessage(ExplorerMessage.Type.ConnectTimeout, e))
            return
        except Exception as e:
            await self.explorer.message(ExplorerMessage(ExplorerMessage.Type.ConnectError, e))
            return
        await self.explorer.message(ExplorerMessage(ExplorerMessage.Type.Connected, None))
        try:
            challenge_request = ChallengeRequest.init(
                version=u32(Testnet2.version),
                fork_depth=u32(Testnet2.fork_depth),
                node_type=NodeType.Client,
                peer_status=Status.Syncing,
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
                        await self.parse_message(Frame().load(frame))
        except Exception:
            traceback.print_exc()
            await self.explorer.message(ExplorerMessage(ExplorerMessage.Type.Disconnected, None))
            await self.close()
            return

    async def parse_message(self, frame: Frame):
        if not isinstance(frame, Frame):
            raise TypeError("frame must be instance of Frame")
        print(frame)
        match frame.type:
            case Message.Type.ChallengeRequest:
                if self.handshake_state != 0:
                    raise Exception("handshake is already done")
                msg: ChallengeRequest = frame.message
                if msg.version < Testnet2.version:
                    raise ValueError("peer is outdated")
                if msg.fork_depth != Testnet2.fork_depth:
                    raise ValueError("peer has wrong fork depth")



    async def send_message(self, message: Message):
        if not issubclass(type(message), Message):
            raise TypeError("message must be subclass of Message")
        frame = Frame.init(type_=message.type, message=message)
        data = frame.dump()
        size = len(data)
        self.writer.write(size.to_bytes(4, "little") + data)
        await self.writer.drain()

    async def close(self):
        self.writer.close()
        await self.writer.wait_closed()
