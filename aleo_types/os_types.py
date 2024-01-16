
from .vm_block import *

class NodeType(IntEnumu8):
    Client = 0
    Prover = 1
    Validator = 2
    Beacon = 3

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.name


class Message(EnumBaseSerialize, RustEnum, Serializable):
    class Type(IntEnumu16):
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
        UnconfirmedSolution = 11
        UnconfirmedTransaction = 12

        def __str__(self):
            return self.name

        def __repr__(self):
            return self.__class__.__name__ + "." + self.name

    type: Type

    @classmethod
    def load(cls, data: BytesIO):
        type_ = Message.Type(struct.unpack("<H", data.read(2))[0])
        match type_:
            case Message.Type.BlockRequest:
                message = BlockRequest.load(data)
            case Message.Type.BlockResponse:
                message = BlockResponse.load(data)
            case Message.Type.ChallengeRequest:
                message = ChallengeRequest.load(data)
            case Message.Type.ChallengeResponse:
                message = ChallengeResponse.load(data)
            case Message.Type.Disconnect:
                message = Disconnect.load(data)
            case Message.Type.PeerRequest:
                message = PeerRequest.load(data)
            case Message.Type.PeerResponse:
                message = PeerResponse.load(data)
            case Message.Type.Ping:
                message = Ping.load(data)
            case Message.Type.Pong:
                message = Pong.load(data)
            case Message.Type.PuzzleRequest:
                message = PuzzleRequest.load(data)
            case Message.Type.PuzzleResponse:
                message = PuzzleResponse.load(data)
            case Message.Type.UnconfirmedSolution:
                message = UnconfirmedSolution.load(data)
            case Message.Type.UnconfirmedTransaction:
                message = UnconfirmedTransaction.load(data)
        # noinspection PyUnboundLocalVariable
        return message

class BlockRequest(Message):
    type = Message.Type.BlockRequest

    def __init__(self, *, start_height: u32, end_height: u32):
        self.start_height = start_height
        self.end_height = end_height

    def dump(self) -> bytes:
        return self.type.dump() + self.start_height.dump() + self.end_height.dump()

    @classmethod
    def load(cls, data: BytesIO):
        start_height = u32.load(data)
        end_height = u32.load(data)
        return cls(start_height=start_height, end_height=end_height)


class BlockResponse(Message):
    type = Message.Type.BlockResponse

    def __init__(self, *, request: BlockRequest, blocks: Data[Vec[Block, u8]]):
        self.request = request
        self.blocks = blocks

    def dump(self) -> bytes:
        return self.type.dump() + self.request.dump() + self.blocks.dump()

    @classmethod
    def load(cls, data: BytesIO):
        request = BlockRequest.load(data)
        blocks = Data[Vec[Block, u8]].load(data)
        return cls(request=request, blocks=blocks)


class ChallengeRequest(Message):
    type = Message.Type.ChallengeRequest

    def __init__(self, *, version: u32, listener_port: u16, node_type: NodeType, address: Address, nonce: u64):
        self.version = version
        self.listener_port = listener_port
        self.node_type = node_type
        self.address = address
        self.nonce = nonce


    def dump(self) -> bytes:
        return b"".join([
            self.type.dump(),
            self.version.dump(),
            self.listener_port.dump(),
            self.node_type.dump(),
            self.address.dump(),
            self.nonce.dump(),
        ])

    @classmethod
    def load(cls, data: BytesIO):
        version = u32.load(data)
        listener_port = u16.load(data)
        node_type = NodeType.load(data)
        address = Address.load(data)
        nonce = u64.load(data)
        return cls(version=version, listener_port=listener_port, node_type=node_type, address=address, nonce=nonce)

    def __str__(self):
        return "ChallengeRequest(version={}, listener_port={}, node_type={}, address={}, nonce={})".format(
            self.version, self.listener_port, self.node_type, self.address, self.nonce
        )

    def __repr__(self):
        return self.__str__()


class ChallengeResponse(Message):
    type = Message.Type.ChallengeResponse

    def __init__(self, *, genesis_header: BlockHeader, signature: Data[Signature]):
        self.genesis_header = genesis_header
        self.signature = signature

    def dump(self) -> bytes:
        return self.type.dump() + self.genesis_header.dump() + self.signature.dump()

    @classmethod
    def load(cls, data: BytesIO):
        genesis_header = BlockHeader.load(data)
        signature = Data[Signature].load(data)
        return cls(genesis_header=genesis_header, signature=signature)


class DisconnectReason(IntEnumu8):
    ExceededForkRange = 0
    InvalidChallengeResponse = 1
    InvalidForkDepth = 2
    INeedToSyncFirst = 3
    NoReasonGiven = 4
    ProtocolViolation = 5
    OutdatedClientVersion = 6
    PeerHasDisconnected = 7
    PeerRefresh = 8
    ShuttingDown = 9
    SyncComplete = 10
    TooManyFailures = 11
    TooManyPeers = 12
    YouNeedToSyncFirst = 13
    YourPortIsClosed = 14,

    @classmethod
    def load(cls, data: BytesIO):
        if data.getbuffer().nbytes == 0:
            return cls(cls.NoReasonGiven)
        reason = u8.load(data)
        return cls(reason)


class Disconnect(Message):
    type = Message.Type.Disconnect

    def __init__(self, *, reason: DisconnectReason):
        self.reason = reason

    def dump(self) -> bytes:
        return self.type.dump() + self.reason.dump()

    @classmethod
    def load(cls, data: BytesIO):
        return cls(reason=DisconnectReason.load(data))


class PeerRequest(Message):
    type = Message.Type.PeerRequest

    def __init__(self):
        pass

    def dump(self) -> bytes:
        return self.type.dump()

    @classmethod
    def load(cls, data: BytesIO):
        return cls()


class PeerResponse(Message):
    type = Message.Type.PeerResponse

    def __init__(self, *, peers: Vec[SocketAddr, u8]):
        self.peers = peers

    def dump(self) -> bytes:
        raise NotImplementedError

    @classmethod
    def load(cls, data: BytesIO):
        peers = Vec[SocketAddr, u8].load(data)
        return cls(peers=peers)

class BlockLocators(Serializable):

    def __init__(self, *, recents: dict[u32, BlockHash], checkpoints: dict[u32, BlockHash]):
        self.recents = recents
        self.checkpoints = checkpoints

    def dump(self) -> bytes:
        res = u32(len(self.recents)).dump()
        for height, block_hash in self.recents.items():
            res += height.dump() + block_hash.dump()
        res += u32(len(self.checkpoints)).dump()
        for height, block_hash in self.checkpoints.items():
            res += height.dump() + block_hash.dump()
        return res

    @classmethod
    def load(cls, data: BytesIO):
        num_locators = u32.load(data)
        recents: dict[u32, BlockHash] = {}
        for _ in range(num_locators):
            height = u32.load(data)
            block_hash = BlockHash.load(data)
            recents[height] = block_hash
        num_checkpoints = u32.load(data)
        checkpoints: dict[u32, BlockHash] = {}
        for _ in range(num_checkpoints):
            height = u32.load(data)
            block_hash = BlockHash.load(data)
            checkpoints[height] = block_hash
        return cls(recents=recents, checkpoints=checkpoints)

class Ping(Message):
    type = Message.Type.Ping

    def __init__(self, *, version: u32, node_type: NodeType, block_locators: Option[BlockLocators]):
        self.version = version
        self.node_type = node_type
        self.block_locators = block_locators

    def dump(self) -> bytes:
        return self.type.dump() + self.version.dump() + self.node_type.dump() + self.block_locators.dump()

    @classmethod
    def load(cls, data: BytesIO):
        version = u32.load(data)
        node_type = NodeType.load(data)
        block_locators = Option[BlockLocators].load(data)
        return cls(version=version, node_type=node_type, block_locators=block_locators)

class Pong(Message):
    type = Message.Type.Pong

    def __init__(self, *, is_fork: Option[bool_]):
        self.is_fork = is_fork

    def dump(self) -> bytes:
        match self.is_fork.value:
            case bool_(True):
                res = u8()
            case bool_():
                res = u8(1)
            case None:
                res = u8(2)
        # noinspection PyUnboundLocalVariable
        return self.type.dump() + res.dump()

    @classmethod
    def load(cls, data: BytesIO):
        fork_flag = u8.load(data)
        match fork_flag:
            case 0:
                is_fork = Option[bool_](bool_(True))
            case 1:
                is_fork = Option[bool_](bool_())
            case 2:
                is_fork = Option[bool_](None)
            case _:
                raise ValueError("fork_flag is not 0, 1, or 2")
        return cls(is_fork=is_fork)



class PuzzleRequest(Message):
    type = Message.Type.PuzzleRequest

    def __init__(self):
        pass

    def dump(self) -> bytes:
        return self.type.dump()

    @classmethod
    def load(cls, data: BytesIO):
        return cls()


class PuzzleResponse(Message):
    type = Message.Type.PuzzleResponse

    def __init__(self, *, epoch_challenge: EpochChallenge, block_header: BlockHeader):
        self.epoch_challenge = epoch_challenge
        self.block_header = block_header

    def dump(self) -> bytes:
        return self.type.dump() + self.epoch_challenge.dump() + self.block_header.dump()

    @classmethod
    def load(cls, data: BytesIO):
        epoch_challenge = EpochChallenge.load(data)
        block_header = BlockHeader.load(data)
        return cls(epoch_challenge=epoch_challenge, block_header=block_header)


class UnconfirmedSolution(Message):
    type = Message.Type.UnconfirmedSolution

    def __init__(self, *, solution_id: PuzzleCommitment, solution: Data[ProverSolution]):
        self.solution_id = solution_id
        self.solution = solution

    def dump(self) -> bytes:
        return self.type.dump() + self.solution_id.dump() + self.solution.dump()

    @classmethod
    def load(cls, data: BytesIO):
        solution_id = PuzzleCommitment.load(data)
        solution = Data[ProverSolution].load(data)
        return cls(solution_id=solution_id, solution=solution)


class UnconfirmedTransaction(Message):
    type = Message.Type.UnconfirmedTransaction

    def __init__(self, *, transaction_id: TransactionID, transaction: Data[Transaction]):
        self.transaction_id = transaction_id
        self.transaction = transaction

    def dump(self) -> bytes:
        return self.type.dump() + self.transaction_id.dump() + self.transaction.dump()

    @classmethod
    def load(cls, data: BytesIO):
        transaction_id = TransactionID.load(data)
        transaction = Data[Transaction].load(data)
        return cls(transaction_id=transaction_id, transaction=transaction)


class Frame(Serializable):

    def __init__(self, *, message: Message):
        self.message = message

    def dump(self) -> bytes:
        return self.message.dump()

    @classmethod
    def load(cls, data: BytesIO):
        message = Message.load(data)

        return cls(message=message)

    def __str__(self):
        return f"Frame(message={self.message})"

    def __repr__(self):
        return self.__str__()