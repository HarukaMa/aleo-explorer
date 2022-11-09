
from .vm_block import *

class NodeType(IntEnumu32):
    Client = 0
    Miner = 1
    Beacon = 2
    Sync = 3
    Operator = 4
    Prover = 5
    PoolServer = 6
    Explorer = 7

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.name


class Status(IntEnumu32):
    Ready = 0
    Mining = 1
    Peering = 2
    Syncing = 3
    ShuttingDown = 4

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.name


class Message(Serialize, Deserialize, metaclass=ABCMeta):
    class Type(IntEnumu32):
        BlockRequest = 0
        BlockResponse = 1
        ChallengeRequest = 2
        ChallengeResponse = 3
        Disconnect = 4
        PeerRequest = 5
        PeerResponse = 6
        Ping = 7
        Pong = 8
        PuzzleRequest = 9
        PuzzleResponse = 10
        UnconfirmedBlock = 11
        UnconfirmedSolution = 12
        UnconfirmedTransaction = 13

        def __str__(self):
            return self.name

        def __repr__(self):
            return self.__class__.__name__ + "." + self.name

    @property
    @abstractmethod
    def type(self):
        raise NotImplementedError


class BlockRequest(Message):
    type = Message.Type.BlockRequest

    def __init__(self, *, start_block_height: u32, end_block_height: u32):
        if not isinstance(start_block_height, u32):
            raise TypeError("start_block_height must be u32")
        if not isinstance(end_block_height, u32):
            raise TypeError("end_block_height must be u32")
        self.start_block_height = start_block_height
        self.end_block_height = end_block_height

    def dump(self) -> bytes:
        return self.start_block_height.dump() + self.end_block_height.dump()

    @classmethod
    def load(cls, data: bytearray):
        if not isinstance(data, bytearray):
            raise TypeError("data must be bytearray")
        start_block_height = u32.load(data)
        end_block_height = u32.load(data)
        return cls(start_block_height=start_block_height, end_block_height=end_block_height)


class BlockResponse(Message):
    type = Message.Type.BlockResponse

    def __init__(self, *, block: Block):
        if not isinstance(block, Block):
            raise TypeError("block must be Block")
        self.block = block

    def dump(self) -> bytes:
        data = self.block.dump()
        return u64(len(data)).dump() + data

    @classmethod
    def load(cls, data: bytearray):
        if not isinstance(data, bytearray):
            raise TypeError("data must be bytearray")
        del data[:8]
        block = Block.load(data)
        return cls(block=block)


class ChallengeRequest(Message):
    type = Message.Type.ChallengeRequest

    def __init__(self, *, version: u32, fork_depth: u32, node_type: NodeType, peer_status: Status,
                 listener_port: u16, peer_nonce: u64, peer_cumulative_weight: u128):
        if not isinstance(version, u32):
            raise TypeError("version must be u32")
        if not isinstance(fork_depth, u32):
            raise TypeError("fork_depth must be u32")
        if not isinstance(node_type, NodeType):
            raise TypeError("node_type must be NodeType")
        if not isinstance(peer_status, Status):
            raise TypeError("peer_status must be Status")
        if not isinstance(listener_port, u16):
            raise TypeError("listener_port must be u16")
        if not isinstance(peer_nonce, u64):
            raise TypeError("peer_nonce must be u64")
        if not isinstance(peer_cumulative_weight, u128):
            raise TypeError("peer_cumulative_weight must be u128")
        self.version = version
        self.fork_depth = fork_depth
        self.node_type = node_type
        self.peer_status = peer_status
        self.listener_port = listener_port
        self.peer_nonce = peer_nonce
        self.peer_cumulative_weight = peer_cumulative_weight

    def dump(self) -> bytes:
        return b"".join([
            self.version.dump(),
            self.fork_depth.dump(),
            self.node_type.dump(),
            self.peer_status.dump(),
            self.listener_port.dump(),
            self.peer_nonce.dump(),
            self.peer_cumulative_weight.dump(),
        ])

    @classmethod
    def load(cls, data: bytearray):
        if not isinstance(data, bytearray):
            raise TypeError("data must be bytearray")
        if len(data) != 42:
            raise ValueError("incorrect length")
        version = u32.load(data)
        fork_depth = u32.load(data)
        node_type = NodeType.load(data)
        peer_status = Status.load(data)
        listener_port = u16.load(data)
        peer_nonce = u64.load(data)
        peer_cumulative_weight = u128.load(data)
        return cls(version=version, fork_depth=fork_depth, node_type=node_type, peer_status=peer_status,
                   listener_port=listener_port, peer_nonce=peer_nonce, peer_cumulative_weight=peer_cumulative_weight)

    def __str__(self):
        return "ChallengeRequest(version={}, fork_depth={}, node_type={}, peer_status={}, listener_port={}, peer_nonce={}, peer_cumulative_weight={})".format(
            self.version, self.fork_depth, self.node_type, self.peer_status, self.listener_port, self.peer_nonce,
            self.peer_cumulative_weight)

    def __repr__(self):
        return self.__str__()


class ChallengeResponse(Message):
    type = Message.Type.ChallengeResponse

    def __init__(self, *, block_header: BlockHeader):
        if not isinstance(block_header, BlockHeader):
            raise TypeError("block_header must be BlockHeader")
        self.block_header = block_header

    def dump(self) -> bytes:
        return self.block_header.dump()

    @classmethod
    def load(cls, data: bytearray):
        if not isinstance(data, bytearray):
            raise TypeError("data must be bytearray")
        return cls(block_header=BlockHeader.load(data))


class YourPortIsClosed(int):
    def __new__(cls, **kwargs):
        return int.__new__(cls, 11)

    def __init__(self, *, port: u16):
        if not isinstance(port, u16):
            raise TypeError("port must be u16")
        self.port = port

    @classmethod
    def load(cls, data: bytearray):
        if not isinstance(data, bytearray):
            raise TypeError("data must be bytearray")
        port = u16.load(data)
        return cls(port=port)

    def __str__(self):
        return f"{str(DisconnectReason(self))}(port={self.port})"

    def __repr__(self):
        return f"{repr(DisconnectReason(self))} port={self.port}"


class DisconnectReason(IntEnumu32):
    ExceededForkRange = 0
    InvalidForkDepth = 1
    INeedToSyncFirst = 2
    NoReasonGiven = 3
    OutdatedClientVersion = 4
    PeerHasDisconnected = 5
    ShuttingDown = 6
    SyncComplete = 7
    TooManyFailures = 8
    TooManyPeers = 9
    YouNeedToSyncFirst = 10
    YourPortIsClosed = YourPortIsClosed(port=u16()),

    @classmethod
    def load(cls, data: bytearray):
        if not isinstance(data, bytearray):
            raise TypeError("data must be bytearray")
        if len(data) == 0:
            return cls(cls.NoReasonGiven)
        reason = u32.load(data)
        if reason == 11:
            return YourPortIsClosed.load(data)
        return cls(reason)


class Disconnect(Message):
    type = Message.Type.Disconnect

    def __init__(self, *, reason: DisconnectReason):
        if not isinstance(reason, DisconnectReason):
            raise TypeError("reason must be DisconnectReason")
        self.reason = reason

    def dump(self) -> bytes:
        return self.reason.dump()

    @classmethod
    def load(cls, data: bytearray):
        if not isinstance(data, bytearray):
            raise TypeError("data must be bytearray")
        return cls(reason=DisconnectReason.load(data))


class PeerRequest(Message):
    type = Message.Type.PeerRequest

    def __init__(self):
        pass

    def dump(self) -> bytes:
        return b""

    @classmethod
    def load(cls, data: bytearray):
        if not isinstance(data, bytearray):
            raise TypeError("data must be bytearray")
        return cls()


class PeerResponse(Message):
    type = Message.Type.PeerResponse

    def __init__(self, *, peer_ips: Vec[SocketAddr, u64]):
        if not isinstance(peer_ips, Vec):
            raise TypeError("peer_ips must be Vec")
        self.peer_ips = peer_ips

    def dump(self) -> bytes:
        raise NotImplementedError

    @classmethod
    def load(cls, data: bytearray):
        if not isinstance(data, bytearray):
            raise TypeError("data must be bytearray")
        # noinspection PyArgumentList
        peer_ips = Vec[SocketAddr, u64].load(data)
        return cls(peer_ips=peer_ips)


class Ping(Message):
    type = Message.Type.Ping

    def __init__(self, *, version: u32, fork_depth: u32, node_type: NodeType, status: Status,
                 block_hash: BlockHash, block_header: BlockHeader):
        if not isinstance(version, u32):
            raise TypeError("version must be u32")
        if not isinstance(fork_depth, u32):
            raise TypeError("fork_depth must be u32")
        if not isinstance(node_type, NodeType):
            raise TypeError("node_type must be NodeType")
        if not isinstance(status, Status):
            raise TypeError("status must be Status")
        if not isinstance(block_hash, BlockHash):
            raise TypeError("block_hash must be BlockHash")
        if not isinstance(block_header, BlockHeader):
            raise TypeError("block_header must be BlockHeader")
        self.version = version
        self.fork_depth = fork_depth
        self.node_type = node_type
        self.status = status
        self.block_hash = block_hash
        self.block_header = block_header

    def dump(self) -> bytes:
        return b"".join([
            self.version.dump(),
            self.fork_depth.dump(),
            self.node_type.dump(),
            self.status.dump(),
            self.block_hash.dump(),
            self.block_header.dump(),
        ])

    @classmethod
    def load(cls, data: bytearray):
        if not isinstance(data, bytearray):
            raise TypeError("data must be bytearray")
        version = u32.load(data)
        fork_depth = u32.load(data)
        node_type = NodeType.load(data)
        status = Status.load(data)
        block_hash = BlockHash.load(data)
        block_header = BlockHeader.load(data)
        return cls(version=version, fork_depth=fork_depth, node_type=node_type, status=status,
                   block_hash=block_hash, block_header=block_header)


# class Pong(Message):
#     type = Message.Type.Pong
#
#     def __init__(self, *, is_fork: bool_ | None, block_locators: BlockLocators):
#         if not isinstance(is_fork, bool_ | NoneType):
#             raise TypeError("is_fork must be bool_ | None")
#         if not isinstance(block_locators, BlockLocators):
#             raise TypeError("block_locators must be BlockLocators")
#         self.is_fork = is_fork
#         self.block_locators = block_locators
#
#     def dump(self) -> bytes:
#         match self.is_fork:
#             case None:
#                 res = u8()
#             case bool_(True):
#                 res = u8(1)
#             case bool_():
#                 res = u8(2)
#             case _:
#                 raise ValueError("is_fork is not bool_ | None")
#         locators = self.block_locators.dump()
#         return res.dump() + u64(len(locators)).dump() + locators
#
#     @classmethod
#     def load(cls, data: bytearray):
#         if not isinstance(data, bytearray):
#             raise TypeError("data must be bytearray")
#         fork_flag = u8.load(data)
#         match fork_flag:
#             case 0:
#                 is_fork = None
#             case 1:
#                 is_fork = bool_(True)
#             case 2:
#                 is_fork = bool_()
#             case _:
#                 raise ValueError("fork_flag is not 0, 1, or 2")
#         # deferred Data type ignored
#         del data[:8]
#         block_locators = BlockLocators.load(data)
#         return cls(is_fork=is_fork, block_locators=block_locators)


class UnconfirmedBlock(Message):
    type = Message.Type.UnconfirmedBlock

    def __init__(self, *, block_height: u32, block_hash: BlockHash, block: Block):
        if not isinstance(block_height, u32):
            raise TypeError("block_height must be u32")
        if not isinstance(block_hash, BlockHash):
            raise TypeError("block_hash must be BlockHash")
        if not isinstance(block, Block):
            raise TypeError("block must be Block")
        self.block_height = block_height
        self.block_hash = block_hash
        self.block = block

    def dump(self) -> bytes:
        return b"".join([
            self.block_height.dump(),
            self.block_hash.dump(),
            self.block.dump(),
        ])

    @classmethod
    def load(cls, data: bytearray):
        if not isinstance(data, bytearray):
            raise TypeError("data must be bytearray")
        block_height = u32.load(data)
        block_hash = BlockHash.load(data)
        # deferred Data type ignored
        del data[:8]
        block = Block.load(data)
        return cls(block_height=block_height, block_hash=block_hash, block=block)


class UnconfirmedTransaction(Message):
    type = Message.Type.UnconfirmedTransaction

    def __init__(self, *, transaction: Transaction):
        if not isinstance(transaction, Transaction):
            raise TypeError("transaction must be Transaction")
        self.transaction = transaction

    def dump(self) -> bytes:
        data = self.transaction.dump()
        return u64(len(data)).dump() + data

    @classmethod
    def load(cls, data: bytearray):
        if not isinstance(data, bytearray):
            raise TypeError("data must be bytearray")
        # deferred Data type ignored
        del data[:8]
        transaction = Transaction.load(data)
        return cls(transaction=transaction)


class Frame(Serialize, Deserialize):

    def __init__(self, *, type_: Message.Type, message: Message):
        if not isinstance(type_, Message.Type):
            raise TypeError("type must be Message.Type")
        if not isinstance(message, Message):
            raise TypeError("message must be Message")
        self.type = type_
        self.message = message

    def dump(self) -> bytes:
        return self.type.to_bytes(2, "little") + self.message.dump()

    @classmethod
    def load(cls, data: bytearray, *, ignore_blocks=False):
        if not isinstance(data, bytearray):
            raise TypeError("data must be bytearray")
        if len(data) < 2:
            raise ValueError("missing message id")
        type_ = Message.Type(struct.unpack("<H", data[:2])[0])
        del data[:2]
        match type_:
            # case Message.Type.BlockRequest:
            #     message = BlockRequest.load(data)
            # case Message.Type.BlockResponse:
            #     message = BlockResponse.load(data)
            # case Message.Type.ChallengeRequest:
            #     message = ChallengeRequest.load(data)
            # case Message.Type.ChallengeResponse:
            #     message = ChallengeResponse.load(data)
            # case Message.Type.Disconnect:
            #     message = Disconnect.load(data)
            # case Message.Type.PeerResponse:
            #     message = PeerResponse.load(data)
            # case Message.Type.Ping:
            #     message = Ping.load(data)
            # case Message.Type.Pong:
            #     message = Pong.load(data)
            # case Message.Type.PuzzleRequest:
            #     message = PuzzleRequest.load(data)
            # case Message.Type.PuzzleResponse:
            #     message = PuzzleResponse.load(data)
            # case Message.Type.UnconfirmedBlock:
            #     if ignore_blocks:
            #         message = None
            #     else:
            #         message = UnconfirmedBlock.load(data)
            # case Message.Type.UnconfirmedSolution:
            #     message = UnconfirmedSolution.load(data)
            # case Message.Type.UnconfirmedTransaction:
            #     message = UnconfirmedTransaction.load(data)
            case _:
                raise ValueError(f"unknown message type {type_}")

        return cls(type_=type_, message=message)

    def __str__(self):
        return f"Frame(type={self.type}, message={self.message})"

    def __repr__(self):
        return self.__str__()