
from .vm_block import *

class NodeType(IntEnumu32):
    Client = 0
    Prover = 1
    Validator = 2
    Beacon = 3

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.name


class Message(Serialize, Deserialize, metaclass=ABCMeta):
    class Type(IntEnumu16):
        BeaconPropose = 0
        BeaconTimeout = 1
        BeaconVote = 2
        BlockRequest = 3
        BlockResponse = 4
        ChallengeRequest = 5
        ChallengeResponse = 6
        Disconnect = 7
        PeerRequest = 8
        PeerResponse = 9
        Ping = 10
        Pong = 11
        PuzzleRequest = 12
        PuzzleResponse = 13
        UnconfirmedSolution = 14
        UnconfirmedTransaction = 15

        def __str__(self):
            return self.name

        def __repr__(self):
            return self.__class__.__name__ + "." + self.name

    @property
    @abstractmethod
    def type(self):
        raise NotImplementedError

class BeaconPropose(Message):
    type = Message.Type.BeaconPropose

    # @type_check
    def __init__(self, version: u8, round_: u64, block_height: u32, block_hash: BlockHash, block: Block):
        self.version = version
        self.round = round_
        self.block_height = block_height
        self.block_hash = block_hash
        self.block = block

    def dump(self) -> bytes:
        return self.version.dump() + self.round.dump() + self.block_height.dump() + self.block_hash.dump() + \
               self.block.dump()

    @classmethod
    # @type_check
    def load(cls, data: BytesIO):
        version = u8.load(data)
        round_ = u64.load(data)
        block_height = u32.load(data)
        block_hash = BlockHash.load(data)
        block = Block.load(data)
        return cls(version, round_, block_height, block_hash, block)


class BeaconTimeout(Message):
    type = Message.Type.BeaconTimeout

    # @type_check
    def __init__(self, version: u8, round_: u64, block_height: u32, block_hash: BlockHash, signature: Signature):
        self.version = version
        self.round = round_
        self.block_height = block_height
        self.block_hash = block_hash
        self.signature = signature

    def dump(self) -> bytes:
        return self.version.dump() + self.round.dump() + self.block_height.dump() + self.block_hash.dump() + \
               self.signature.dump()

    @classmethod
    # @type_check
    def load(cls, data: BytesIO):
        version = u8.load(data)
        round_ = u64.load(data)
        block_height = u32.load(data)
        block_hash = BlockHash.load(data)
        signature = Signature.load(data)
        return cls(version, round_, block_height, block_hash, signature)


class BeaconVote(Message):
    type = Message.Type.BeaconVote

    # @type_check
    def __init__(self, version: u8, round_: u64, block_height: u32, block_hash: BlockHash,
                 timestamp: u64, signature: Signature):
        self.version = version
        self.round = round_
        self.block_height = block_height
        self.block_hash = block_hash
        self.timestamp = timestamp
        self.signature = signature

    def dump(self) -> bytes:
        return self.version.dump() + self.round.dump() + self.block_height.dump() + self.block_hash.dump() + \
               self.timestamp.dump() + self.signature.dump()

    @classmethod
    # @type_check
    def load(cls, data: BytesIO):
        version = u8.load(data)
        round_ = u64.load(data)
        block_height = u32.load(data)
        block_hash = BlockHash.load(data)
        timestamp = u64.load(data)
        signature = Signature.load(data)
        return cls(version, round_, block_height, block_hash, timestamp, signature)


class BlockRequest(Message):
    type = Message.Type.BlockRequest

    # @type_check
    def __init__(self, *, start_height: u32, end_height: u32):
        self.start_height = start_height
        self.end_height = end_height

    def dump(self) -> bytes:
        return self.start_height.dump() + self.end_height.dump()

    @classmethod
    # @type_check
    def load(cls, data: BytesIO):
        start_height = u32.load(data)
        end_height = u32.load(data)
        return cls(start_height=start_height, end_height=end_height)


class BlockResponse(Message):
    type = Message.Type.BlockResponse

    # @type_check
    @generic_type_check
    def __init__(self, *, request: BlockRequest, blocks: Vec[Block, u8]):
        self.request = request
        self.blocks = blocks

    def dump(self) -> bytes:
        return self.request.dump() + self.blocks.dump()

    @classmethod
    # @type_check
    def load(cls, data: BytesIO):
        request = BlockRequest.load(data)
        blocks = Vec[Block, u8].load(data)
        return cls(request=request, blocks=blocks)


class ChallengeRequest(Message):
    type = Message.Type.ChallengeRequest

    # @type_check
    def __init__(self, *, version: u32, listener_port: u16, node_type: NodeType, address: Address, nonce: u64):
        self.version = version
        self.listener_port = listener_port
        self.node_type = node_type
        self.address = address
        self.nonce = nonce


    def dump(self) -> bytes:
        return b"".join([
            self.version.dump(),
            self.listener_port.dump(),
            self.node_type.dump(),
            self.address.dump(),
            self.nonce.dump(),
        ])

    @classmethod
    # @type_check
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

    # @type_check
    def __init__(self, *, genesis_header: BlockHeader, signature: Signature):
        self.genesis_header = genesis_header
        self.signature = signature

    def dump(self) -> bytes:
        return self.genesis_header.dump() + self.signature.dump()

    @classmethod
    # @type_check
    def load(cls, data: BytesIO):
        genesis_header = BlockHeader.load(data)
        signature = Signature.load(data)
        return cls(genesis_header=genesis_header, signature=signature)


class YourPortIsClosed(int):
    def __new__(cls, **kwargs):
        return int.__new__(cls, 14)

    # @type_check
    def __init__(self, *, port: u16):
        self.port = port

    @classmethod
    # @type_check
    def load(cls, data: BytesIO):
        port = u16.load(data)
        return cls(port=port)

    def __str__(self):
        return f"{str(DisconnectReason(self))}(port={self.port})"

    def __repr__(self):
        return f"{repr(DisconnectReason(self))} port={self.port}"


class DisconnectReason(IntEnumu32):
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
    YourPortIsClosed = YourPortIsClosed(port=u16()),

    @classmethod
    # @type_check
    def load(cls, data: BytesIO):
        if len(data) == 0:
            return cls(cls.NoReasonGiven)
        reason = u32.load(data)
        if reason == 14:
            return YourPortIsClosed.load(data)
        return cls(reason)


class Disconnect(Message):
    type = Message.Type.Disconnect

    # @type_check
    def __init__(self, *, reason: DisconnectReason):
        self.reason = reason

    def dump(self) -> bytes:
        return self.reason.dump()

    @classmethod
    # @type_check
    def load(cls, data: BytesIO):
        return cls(reason=DisconnectReason.load(data))


class PeerRequest(Message):
    type = Message.Type.PeerRequest

    def __init__(self):
        pass

    def dump(self) -> bytes:
        return b""

    @classmethod
    def load(cls, data: BytesIO):
        return cls()


class PeerResponse(Message):
    type = Message.Type.PeerResponse

    # @type_check
    def __init__(self, *, peers: Vec[SocketAddr, u64]):
        self.peers = peers

    def dump(self) -> bytes:
        raise NotImplementedError

    @classmethod
    # @type_check
    def load(cls, data: BytesIO):
        peers = Vec[SocketAddr, u64].load(data)
        return cls(peers=peers)

class BlockLocators(Serialize, Deserialize):

    # @type_check
    def __init__(self, *, recents, checkpoints):
        self.recents = recents
        self.checkpoints = checkpoints

    def dump(self) -> bytes:
        res = u64(len(self.recents)).dump()
        for height, block_hash in self.recents.items():
            res += height.dump() + block_hash.dump()
        res += u64(len(self.checkpoints)).dump()
        for height, block_hash in self.checkpoints.items():
            res += height.dump() + block_hash.dump()
        return res

    @classmethod
    # @type_check
    def load(cls, data: BytesIO):
        num_locators = u64.load(data)
        recents = {}
        for _ in range(num_locators):
            height = u32.load(data)
            block_hash = BlockHash.load(data)
            recents[height] = block_hash
        num_checkpoints = u64.load(data)
        checkpoints = {}
        for _ in range(num_checkpoints):
            height = u32.load(data)
            block_hash = BlockHash.load(data)
            checkpoints[height] = block_hash
        return cls(recents=recents, checkpoints=checkpoints)

class Ping(Message):
    type = Message.Type.Ping

    # @type_check
    # @generic_type_check
    def __init__(self, *, version: u32, node_type: NodeType, block_locators: Option[BlockLocators]):
        self.version = version
        self.node_type = node_type
        self.block_locators = block_locators

    def dump(self) -> bytes:
        return self.version.dump() + self.node_type.dump() + self.block_locators.dump()

    @classmethod
    # @type_check
    def load(cls, data: BytesIO):
        version = u32.load(data)
        node_type = NodeType.load(data)
        block_locators = Option[BlockLocators].load(data)
        return cls(version=version, node_type=node_type, block_locators=block_locators)

class Pong(Message):
    type = Message.Type.Pong

    # @generic_type_check
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
            case _:
                raise ValueError("is_fork is not bool_ | None")
        return res.dump()

    @classmethod
    # @type_check
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
        return b""

    @classmethod
    def load(cls, data: BytesIO):
        return cls()


class PuzzleResponse(Message):
    type = Message.Type.PuzzleResponse

    # @type_check
    def __init__(self, *, epoch_challenge: EpochChallenge, block_header: BlockHeader):
        self.epoch_challenge = epoch_challenge
        self.block_header = block_header

    def dump(self) -> bytes:
        return self.epoch_challenge.dump() + self.block_header.dump()

    @classmethod
    # @type_check
    def load(cls, data: BytesIO):
        epoch_challenge = EpochChallenge.load(data)
        block_header = BlockHeader.load(data)
        return cls(epoch_challenge=epoch_challenge, block_header=block_header)


class UnconfirmedSolution(Message):
    type = Message.Type.UnconfirmedSolution

    # @type_check
    def __init__(self, *, puzzle_commitment: PuzzleCommitment, solution: ProverSolution):
        self.puzzle_commitment = puzzle_commitment
        self.solution = solution

    def dump(self) -> bytes:
        return self.puzzle_commitment.dump() + self.solution.dump()

    @classmethod
    # @type_check
    def load(cls, data: BytesIO):
        puzzle_commitment = PuzzleCommitment.load(data)
        solution = ProverSolution.load(data)
        return cls(puzzle_commitment=puzzle_commitment, solution=solution)


class UnconfirmedTransaction(Message):
    type = Message.Type.UnconfirmedTransaction

    # @type_check
    def __init__(self, *, transaction_id: TransactionID, transaction: Transaction):
        self.transaction_id = transaction_id
        self.transaction = transaction

    def dump(self) -> bytes:
        return self.transaction_id.dump() + self.transaction.dump()

    @classmethod
    # @type_check
    def load(cls, data: BytesIO):
        transaction_id = TransactionID.load(data)
        transaction = Transaction.load(data)
        return cls(transaction_id=transaction_id, transaction=transaction)


class Frame(Serialize, Deserialize):

    # @type_check
    def __init__(self, *, type_: Message.Type, message: Message):
        self.type = type_
        self.message = message

    def dump(self) -> bytes:
        return self.type.to_bytes(2, "little") + self.message.dump()

    @classmethod
    # @type_check
    def load(cls, data: BytesIO):
        if data.tell() + 2 > data.getbuffer().nbytes:
            raise ValueError("missing message id")
        type_ = Message.Type(struct.unpack("<H", data.read(2))[0])
        match type_:
            case Message.Type.BeaconPropose:
                message = BeaconPropose.load(data)
            case Message.Type.BeaconTimeout:
                message = BeaconTimeout.load(data)
            case Message.Type.BeaconVote:
                message = BeaconVote.load(data)
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
            case _:
                raise ValueError(f"unknown message type {type_}")

        return cls(type_=type_, message=message)

    def __str__(self):
        return f"Frame(type={self.type}, message={self.message})"

    def __repr__(self):
        return self.__str__()