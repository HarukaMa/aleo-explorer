import struct
from abc import ABCMeta, abstractmethod
from enum import IntEnum, EnumMeta
from types import NoneType

import aleo

from thirdparty import bech32


# Metaclass Helper

class ABCEnumMeta(ABCMeta, EnumMeta):
    # https://stackoverflow.com/questions/56131308/create-an-abstract-enum-class/56135108#56135108
    def __new__(mcls, *args, **kw):
        abstract_enum_cls = super().__new__(mcls, *args, **kw)
        # Only check abstractions if members were defined.
        if abstract_enum_cls._member_map_:
            try:  # Handle existence of undefined abstract methods.
                absmethods = list(abstract_enum_cls.__abstractmethods__)
                if absmethods:
                    missing = ', '.join(f'{method!r}' for method in absmethods)
                    plural = 's' if len(absmethods) > 1 else ''
                    raise TypeError(
                        f"cannot instantiate abstract class {abstract_enum_cls.__name__!r}"
                        f" with abstract method{plural} {missing}")
            except AttributeError:
                pass
        return abstract_enum_cls


# Traits (kind of)

class Deserialize(metaclass=ABCMeta):

    @abstractmethod
    def load(self, data: bytearray):
        raise NotImplementedError


class Serialize(metaclass=ABCMeta):

    @abstractmethod
    def dump(self) -> bytes:
        raise NotImplementedError


class Sized(metaclass=ABCMeta):
    @property
    @abstractmethod
    def size(self):
        raise NotImplementedError


class Int(Sized, Serialize, Deserialize, int, metaclass=ABCMeta):

    def __new__(cls, value=0):
        return int.__new__(cls, value)

    @abstractmethod
    def __init__(self, _):
        raise NotImplementedError


# Basic types

class Bech32m:

    def __init__(self, data, prefix):
        if not isinstance(data, bytes):
            raise TypeError("can only initialize type with bytes")
        if not isinstance(prefix, str):
            raise TypeError("only str prefix is supported")
        self.data = data
        self.prefix = prefix

    def __str__(self):
        return bech32.bech32_encode(self.prefix, bech32.convertbits(list(self.data), 8, 5), bech32.Encoding.BECH32M)

    def __repr__(self):
        return str(self)


class u8(Int):
    size = 1

    def __init__(self, value=0):
        if not isinstance(value, int):
            raise TypeError("value must be int")
        if value < 0 or value > 255:
            raise ValueError("value must be between 0 and 255")

    def dump(self) -> bytes:
        return struct.pack("<B", self)

    @classmethod
    def load(cls, data: bytearray):
        self = cls(struct.unpack("<B", data[:1])[0])
        del data[0]
        return self


class u16(Int):
    size = 2

    def __init__(self, value=0):
        if not isinstance(value, int):
            raise TypeError("value must be int")
        if value < 0 or value > 65535:
            raise ValueError("value must be between 0 and 65535")

    def dump(self) -> bytes:
        return struct.pack("<H", self)

    @classmethod
    def load(cls, data: bytearray):
        self = cls(struct.unpack("<H", data[:2])[0])
        del data[:2]
        return self


class u32(Int):
    size = 4

    def __init__(self, value=0):
        if not isinstance(value, int):
            raise TypeError("value must be int")
        if value < 0 or value > 4294967295:
            raise ValueError("value must be between 0 and 4294967295")

    def dump(self) -> bytes:
        return struct.pack("<I", self)

    @classmethod
    def load(cls, data: bytearray):
        self = cls(struct.unpack("<I", data[:4])[0])
        del data[:4]
        return self


class u64(Int):
    size = 8

    def __init__(self, value=0):
        if not isinstance(value, int):
            raise TypeError("value must be int")
        if value < 0 or value > 18446744073709551615:
            raise ValueError("value must be between 0 and 18446744073709551615")

    def dump(self) -> bytes:
        return struct.pack("<Q", self)

    @classmethod
    def load(cls, data: bytearray):
        self = cls(struct.unpack("<Q", data[:8])[0])
        del data[:8]
        return self


class u128(Int):
    size = 16

    def __init__(self, value=0):
        if not isinstance(value, int):
            raise TypeError("value must be int")
        if value < 0 or value > 2 ** 128 - 1:
            raise ValueError("value must be between 0 and 2 ** 128 - 1")

    def dump(self) -> bytes:
        return struct.pack("<QQ", self >> 64, self & 0xFFFF_FFFF_FFFF_FFFF)

    @classmethod
    def load(cls, data: bytearray):
        lo, hi = struct.unpack("<QQ", data[:16])
        self = cls((hi << 64) | lo)
        del data[:16]
        return self


class i64(Int):
    size = 8

    def __init__(self, value=0):
        if not isinstance(value, int):
            raise TypeError("value must be int")
        if value < -9223372036854775808 or value > 9223372036854775807:
            raise ValueError("value must be between -9223372036854775808 and 9223372036854775807")

    def dump(self) -> bytes:
        return struct.pack("<q", self)

    @classmethod
    def load(cls, data: bytearray):
        self = cls(struct.unpack("<q", data[:8])[0])
        del data[:8]
        return self


class bool_(Int):
    # Really don't want to make a proper bool, reusing Int is good enough for most usages

    size = 1

    def __init__(self, value=False):
        if not isinstance(value, bool):
            raise TypeError("value must be bool")

    def dump(self) -> bytes:
        return struct.pack("<B", self)

    @classmethod
    def load(cls, data: bytearray):
        value = struct.unpack("<B", data[:1])[0]
        if value == 0:
            value = False
        elif value == 1:
            value = True
        else:
            breakpoint()
            raise ValueError("invalid value for bool")
        self = cls(value)
        del data[:1]
        return self


# Generic types

class Generic(metaclass=ABCMeta):
    @abstractmethod
    def __init__(self, types):
        self.types = types

    def __class_getitem__(cls, item):
        if not isinstance(item, tuple):
            item = item,
        # Unfortunately we have sized vec, so we can't have this check anymore
        # if not all(isinstance(x, type) or isinstance(x, Generic) for x in item):
        #     raise TypeError("expected type or generic types as generic types")
        return cls(item)


class Vec(Generic, Serialize, Deserialize):

    def __init__(self, types):
        if len(types) != 2:
            raise TypeError("expected 2 type for Vec")
        self.type = types[0]
        if isinstance(types[1], int):
            self.size = types[1]
        elif issubclass(types[1], Int):
            self.size_type = types[1]
        else:
            raise TypeError("expected int or Int as size type")
        super().__init__(types)

    def __call__(self):
        self._list = []
        return self

    def dump(self) -> bytes:
        res = b""
        if hasattr(self, "size_type"):
            res += self.size_type.dump(self.size)
        for item in self._list:
            res += self.type.dump(item)
        return res

    def load(self, data: bytearray):
        if not issubclass(self.type, Deserialize):
            raise TypeError(f"{self.type.__name__} must be Deserialize")
        if not isinstance(data, bytearray):
            raise TypeError("data must be bytearray")
        if hasattr(self, "size_type"):
            # noinspection PyTypeChecker
            if len(data) < self.size_type.size:
                raise ValueError("data is too short")
            # noinspection PyArgumentList
            self.size = self.size_type.load(data)
        self._list = []
        for i in range(self.size):
            # noinspection PyArgumentList
            self._list.append(self.type.load(data))
        return self


# snarkVM types

class Locator(Sized, Serialize, Deserialize, metaclass=ABCMeta):
    size = 32

    def __init__(self):
        if not isinstance(self._locator_prefix, str):
            raise TypeError("locator_prefix must be str")
        if len(self._locator_prefix) != 2:
            raise ValueError("locator_prefix must be 2 bytes")
        self._locator_data = None
        self._bech32m = None

    @property
    @abstractmethod
    def _locator_prefix(self):
        raise NotImplementedError

    @property
    def data(self):
        return self._locator_data

    @data.setter
    def data(self, value):
        if not isinstance(value, bytes):
            raise TypeError("data must be bytes")
        self._locator_data = value
        self._bech32m = Bech32m(value, self._locator_prefix)

    def dump(self) -> bytes:
        return self._locator_data

    @classmethod
    def load(cls, data: bytearray):
        if not isinstance(data, bytearray):
            raise TypeError("data must be bytearray")
        if len(data) < cls.size:
            raise ValueError("incorrect length")
        self = cls()
        self.data = bytes(data[:self.size])
        del data[:self.size]
        return self

    def __str__(self):
        return str(self._bech32m)

    def __repr__(self):
        return self.__class__.__name__ + "(" + str(self) + ")"

    def __eq__(self, other):
        if not isinstance(other, Locator):
            return False
        return self.data == other.data


class Object(Sized, Serialize, Deserialize, metaclass=ABCMeta):
    def __init__(self):
        if not isinstance(self._object_prefix, str):
            raise TypeError("object_prefix must be str")
        if len(self._object_prefix) != 4:
            raise ValueError("object_prefix must be 4 bytes")
        self._data = None
        self._bech32m = None

    @property
    @abstractmethod
    def _object_prefix(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def size(self):
        raise NotImplementedError

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, value):
        if not isinstance(value, bytes):
            raise TypeError("data must be bytes")
        if len(value) != self.size:
            raise ValueError("data must be %d bytes" % self.size)
        self._data = value
        self._bech32m = Bech32m(value, self._object_prefix)

    def dump(self) -> bytes:
        return self._data

    @classmethod
    def load(cls, data: bytearray):
        if not isinstance(data, bytearray):
            raise TypeError("data must be bytearray")
        self = cls()
        if len(data) < self.size:
            raise ValueError("incorrect length")
        self.data = bytes(data[:self.size])
        del data[:self.size]
        return self

    def __str__(self):
        return str(self._bech32m)

    def __repr__(self):
        return self.__class__.__name__ + "(" + str(self) + ")"

    def __eq__(self, other):
        if not isinstance(other, Object):
            return False
        return self.data == other.data


class LedgerRoot(Locator):
    _locator_prefix = "al"

    def __init__(self):
        Locator.__init__(self)


class TransactionsRoot(Locator):
    _locator_prefix = "ht"

    def __init__(self):
        Locator.__init__(self)


class PoSWNonce(Locator):
    _locator_prefix = "hn"

    def __init__(self):
        Locator.__init__(self)


class PoSWProof(Object):
    _object_prefix = "hzkp"
    size = 691

    def __init__(self):
        Object.__init__(self)


class DeprecatedPoSWProof(Object):
    _object_prefix = "hzkp"
    size = 771

    def __init__(self):
        Object.__init__(self)


class TransactionID(Locator):
    _locator_prefix = "at"

    def __init__(self):
        Locator.__init__(self)


class BlockHash(Locator):
    _locator_prefix = "ab"

    def __init__(self):
        Locator.__init__(self)


class InnerCircuitID(Locator):
    _locator_prefix = "ic"
    size = 48

    def __init__(self):
        Locator.__init__(self)


class TransitionID(Locator):
    _locator_prefix = "as"

    def __init__(self):
        Locator.__init__(self)


class SerialNumber(Locator):
    _locator_prefix = "sn"

    def __init__(self):
        Locator.__init__(self)


class Commitment(Locator):
    _locator_prefix = "cm"

    def __init__(self):
        Locator.__init__(self)


class RecordCiphertext(Object):
    _object_prefix = "recd"
    size = 288

    def __init__(self):
        Object.__init__(self)


class OuterProof(Object):
    _object_prefix = "ozkp"
    size = 289

    def __init__(self):
        Object.__init__(self)


class RecordViewKey(Object):
    _object_prefix = "rcvk"
    size = 32

    def __init__(self):
        Object.__init__(self)


class AleoAmount(i64):
    pass


class Event(Serialize, Deserialize):
    class Type(IntEnum):
        Custom = 0
        RecordViewKey = 1
        Operation = 2

        def __str__(self):
            return self.name

        def __repr__(self):
            return self.__class__.__name__ + "." + self.name

    def __init__(self):
        self.event = None
        self.type = None

    def dump(self) -> bytes:
        return self.type.to_bytes(1, "little") + self.event.dump()

    @classmethod
    def load(cls, data: bytearray):
        if not isinstance(data, bytearray):
            raise TypeError("data must be bytearray")
        if len(data) < 1:
            raise ValueError("incorrect length")
        self = cls()
        self.type = Event.Type(data[0])
        del data[0]
        match self.type:
            case Event.Type.Custom:
                self.event = CustomEvent.load(data)
            case Event.Type.RecordViewKey:
                self.event = RecordViewKeyEvent.load(data)
            case _:
                raise ValueError("unknown event type")
        return self


class CustomEvent(Serialize, Deserialize):

    def __init__(self):
        self.bytes = None

    @classmethod
    def init(cls, *, bytes_: Vec[u8, u16]):
        if not isinstance(bytes_, Vec) or bytes_.types[0] != u8:
            raise TypeError("bytes must be Vec[u8]")
        self = cls()
        self.bytes = bytes_
        return self

    def dump(self) -> bytes:
        return self.bytes.dump()

    @classmethod
    def load(cls, data: bytearray):
        if not isinstance(data, bytearray):
            raise TypeError("data must be bytearray")
        self = cls()
        # noinspection PyArgumentList
        self.bytes = Vec[u8].load(data)
        return self


class RecordViewKeyEvent(Serialize, Deserialize):

    def __init__(self):
        self.index = None
        self.record_view_key = None

    @classmethod
    def init(cls, *, index: u8, record_view_key: RecordViewKey):
        if not isinstance(index, u8):
            raise TypeError("index must be u8")
        if not isinstance(record_view_key, RecordViewKey):
            raise TypeError("record_view_key must be RecordViewKey")
        self = cls()
        self.index = index
        self.record_view_key = record_view_key
        return self

    def dump(self) -> bytes:
        return self.index.dump() + self.record_view_key.dump()

    @classmethod
    def load(cls, data: bytearray):
        if not isinstance(data, bytearray):
            raise TypeError("data must be bytearray")
        self = cls()
        self.index = u8.load(data)
        self.record_view_key = RecordViewKey.load(data)
        return self


class Transition(Serialize, Deserialize):
    def __init__(self):
        self.transition_id = None
        self.serial_numbers = None
        self.ciphertexts = None
        self.value_balance = None
        self.events = None
        self.proof = None

    @classmethod
    def init(cls, *, transition_id: TransitionID, serial_numbers: Vec[SerialNumber, 2],
             ciphertexts: Vec[RecordCiphertext, 2], value_balance: AleoAmount, events: Vec[Event, u16],
             proof: OuterProof):
        if not isinstance(transition_id, TransitionID):
            raise TypeError("transition_id must be TransitionID")
        if not isinstance(serial_numbers, Vec) or serial_numbers.types[0] != SerialNumber:
            raise TypeError("serial_numbers must be Vec[SerialNumber]")
        if not isinstance(ciphertexts, Vec) or ciphertexts.types[0] != RecordCiphertext:
            raise TypeError("ciphertexts must be Vec[RecordCiphertext]")
        if not isinstance(value_balance, AleoAmount):
            raise TypeError("value_balance must be AleoAmount")
        if not isinstance(events, Vec) or events.types[0] != Event:
            raise TypeError("events must be Vec[Event]")
        if not isinstance(proof, OuterProof):
            raise TypeError("proof must be OuterProof")
        self = cls()
        self.transition_id = transition_id
        self.serial_numbers = serial_numbers
        self.ciphertexts = ciphertexts
        self.value_balance = value_balance
        self.events = events
        self.proof = proof
        return self

    def dump(self) -> bytes:
        return self.transition_id.dump() + self.serial_numbers.dump() + self.ciphertexts.dump() + \
               self.value_balance.dump() + self.events.dump() + self.proof.dump()

    # noinspection PyArgumentList
    @classmethod
    def load(cls, data: bytearray):
        if not isinstance(data, bytearray):
            raise TypeError("data must be bytearray")
        if len(data) < 1:
            raise ValueError("incorrect length")
        self = cls()
        self.transition_id = TransitionID.load(data)
        self.serial_numbers = Vec[SerialNumber, 2].load(data)
        self.ciphertexts = Vec[RecordCiphertext, 2].load(data)
        self.value_balance = AleoAmount.load(data)
        self.events = Vec[Event, u16].load(data)
        self.proof = OuterProof.load(data)
        return self


class Transaction(Serialize, Deserialize):

    def __init__(self):
        self.transaction_id = None
        self.inner_circuit_id = None
        self.ledger_root = None
        self.transitions = None

    @classmethod
    def init(cls, *, inner_circuit_id: InnerCircuitID, ledger_root: LedgerRoot,
             transitions: Vec[Transition, u16]):
        if not isinstance(inner_circuit_id, InnerCircuitID):
            raise TypeError("inner_circuit_id must be InnerCircuitID")
        if not isinstance(ledger_root, LedgerRoot):
            raise TypeError("ledger_root must be LedgerRoot")
        if not isinstance(transitions, Vec) or transitions.types[0] != Transition:
            raise TypeError("transitions must be Vec[Transition]")
        self = cls()
        self.inner_circuit_id = inner_circuit_id
        self.ledger_root = ledger_root
        self.transitions = transitions
        return self

    def dump(self) -> bytes:
        return self.inner_circuit_id.dump() + self.ledger_root.dump() + self.transitions.dump()

    # noinspection PyArgumentList
    @classmethod
    def load(cls, data: bytearray):
        if not isinstance(data, bytearray):
            raise TypeError("data must be bytearray")
        self = cls()
        self.inner_circuit_id = InnerCircuitID.load(data)
        self.ledger_root = LedgerRoot.load(data)
        self.transitions = Vec[Transition, u16].load(data)
        self.transaction_id = TransactionID.load(bytearray(aleo.get_transaction_id(self.dump())))
        return self


class Transactions(Serialize, Deserialize):
    def __init__(self):
        self.transactions = None

    @classmethod
    def init(cls, *,
             transactions: Vec[
                 Transaction, u16]):  # merkle tree is not transmitted over network, and we don't care about it
        if not isinstance(transactions, Vec) or transactions.types[0] != Transaction:
            raise TypeError("expected Vec[Transaction] for transactions")
        self = cls()
        self.transactions = transactions
        return self

    def dump(self) -> bytes:
        return self.transactions.dump()

    # noinspection PyArgumentList
    @classmethod
    def load(cls, data: bytearray):
        if not isinstance(data, bytearray):
            raise TypeError("data must be bytearray")
        self = cls()
        self.transactions = Vec[Transaction, u16].load(data)
        return self


class Block(Serialize, Deserialize):

    def __init__(self):
        self.block_hash = None
        self.previous_block_hash = None
        self.header = None
        self.transactions = None

    @classmethod
    def init(cls, *, block_hash: BlockHash, previous_block_hash: BlockHash, header: "BlockHeader",
             transactions: Transactions):
        self = cls()
        self.block_hash = block_hash
        self.previous_block_hash = previous_block_hash
        self.header = header
        self.transactions = transactions
        return self

    def dump(self) -> bytes:
        return self.block_hash.dump() + self.previous_block_hash.dump() + self.header.dump() + self.transactions.dump()

    @classmethod
    def load(cls, data: bytearray):
        if not isinstance(data, bytearray):
            raise TypeError("data must be bytearray")
        self = cls()
        self.block_hash = BlockHash.load(data)
        self.previous_block_hash = BlockHash.load(data)
        self.header = BlockHeader.load(data)
        self.transactions = Transactions.load(data)
        return self


class BlockHeader(Serialize, Deserialize):

    def __init__(self):
        self.previous_ledger_root = None
        self.transactions_root = None
        self.metadata = None
        self.nonce = None
        self.proof = None

    @classmethod
    def init(cls, *, previous_ledger_root: LedgerRoot, transactions_root: TransactionsRoot,
             metadata: "BlockHeaderMetadata", nonce: PoSWNonce, proof: PoSWProof | DeprecatedPoSWProof):
        if not isinstance(previous_ledger_root, LedgerRoot):
            raise TypeError("previous_ledger_root must be LedgerRoot")
        if not isinstance(transactions_root, TransactionsRoot):
            raise TypeError("transactions_root must be TransactionsRoot")
        if not isinstance(metadata, BlockHeaderMetadata):
            raise TypeError("metadata must be BlockHeaderMetadata")
        if not isinstance(nonce, PoSWNonce):
            raise TypeError("nonce must be PoSWNonce")
        if not isinstance(proof, (PoSWProof, DeprecatedPoSWProof)):
            raise TypeError("proof must be PoSWProof or DeprecatedPoSWProof")
        self = cls()
        self.previous_ledger_root = previous_ledger_root
        self.transactions_root = transactions_root
        self.metadata = metadata
        self.nonce = nonce
        if metadata.height < 100000 and isinstance(proof, PoSWProof):
            raise ValueError("proof must be DeprecatedPoSWProof")
        if metadata.height >= 100000 and isinstance(proof, DeprecatedPoSWProof):
            raise ValueError("proof must be PoSWProof")
        self.proof = proof
        return self

    def dump(self) -> bytes:
        return self.previous_ledger_root.dump() + self.transactions_root.dump() + self.metadata.dump() + self.nonce.dump() + self.proof.dump()

    @classmethod
    def load(cls, data: bytearray):
        self = cls()
        if not isinstance(data, bytearray):
            raise TypeError("data must be bytearray")
        self.previous_ledger_root = LedgerRoot.load(data)
        self.transactions_root = TransactionsRoot.load(data)
        self.metadata = BlockHeaderMetadata.load(data)
        self.nonce = PoSWNonce.load(data)
        if self.metadata.height < 100000:
            self.proof = DeprecatedPoSWProof.load(data)
        else:
            self.proof = PoSWProof.load(data)
            del data[:80]
        return self

    def __eq__(self, other):
        if not isinstance(other, BlockHeader):
            return False
        return self.previous_ledger_root == other.previous_ledger_root and \
               self.transactions_root == other.transactions_root and \
               self.metadata == other.metadata and \
               self.nonce == other.nonce and \
               self.proof == other.proof


class BlockHeaderMetadata(Serialize, Deserialize):

    def __init__(self):
        self.height = None
        self.timestamp = None
        self.difficulty_target = None
        self.cumulative_weight = None

    @classmethod
    def init(cls, *, height: u32, timestamp: i64, difficulty_target: u64, cumulative_weight: u128):
        if not isinstance(height, u32):
            raise TypeError("height must be u32")
        if not isinstance(timestamp, i64):
            raise TypeError("timestamp must be i64")
        if not isinstance(difficulty_target, u64):
            raise TypeError("difficulty_target must be u64")
        if not isinstance(cumulative_weight, u128):
            raise TypeError("cumulative_weight must be u128")
        self = cls()
        self.height = height
        self.timestamp = timestamp
        self.difficulty_target = difficulty_target
        self.cumulative_weight = cumulative_weight
        return self

    def dump(self) -> bytes:
        return self.height.dump() + self.timestamp.dump() + self.difficulty_target.dump() + self.cumulative_weight.dump()

    @classmethod
    def load(cls, data: bytearray):
        self = cls()
        if not isinstance(data, bytearray):
            raise TypeError("data must be bytearray")
        self.height = u32.load(data)
        self.timestamp = i64.load(data)
        self.difficulty_target = u64.load(data)
        self.cumulative_weight = u128.load(data)
        return self

    def __eq__(self, other):
        if not isinstance(other, BlockHeaderMetadata):
            return False
        return self.height == other.height and \
               self.timestamp == other.timestamp and \
               self.difficulty_target == other.difficulty_target and \
               self.cumulative_weight == other.cumulative_weight


# snarkOS types


class IntEnumSerialize(Serialize, Deserialize, IntEnum, metaclass=ABCEnumMeta):

    def dump(self) -> bytes:
        return struct.pack("<I", self.value)

    @classmethod
    def load(cls, data: bytearray):
        if not isinstance(data, bytearray):
            raise TypeError("data must be bytearray")
        if len(data) < 4:
            raise ValueError("incorrect length")
        self = cls(struct.unpack("<I", data[:4])[0])
        del data[:4]
        return self


class NodeType(IntEnumSerialize):
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


class Status(IntEnumSerialize):
    Ready = 0
    Mining = 1
    Peering = 2
    Syncing = 3
    ShuttingDown = 4

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.name


class BlockLocators(Serialize, Deserialize):

    def __init__(self):
        self.block_locators = None

    @classmethod
    def init(cls, *, block_locators: dict[u32, (BlockHash, BlockHeader | NoneType)]):
        if not isinstance(block_locators, dict):
            raise TypeError("block_locators must be dict")
        self = cls()
        self.block_locators = block_locators
        return self

    def dump(self) -> bytes:
        res = u32(len(self.block_locators)).dump()
        for height, (block_hash, header) in self.block_locators.items():
            res += height.dump() + block_hash.dump()
            if header is None:
                res += bool_().dump()
            else:
                res += bool_(True).dump()
                res += header.dump()
        return res

    @classmethod
    def load(cls, data: bytearray):
        if not isinstance(data, bytearray):
            raise TypeError("data must be bytearray")
        num_locators = u32.load(data)
        if num_locators == 0 or num_locators > 96:
            raise ValueError("incorrect number of locators")
        self = cls()
        self.block_locators = {}
        for _ in range(num_locators):
            height = u32.load(data)
            block_hash = BlockHash.load(data)
            header_exists = bool_.load(data)
            if header_exists:
                header = BlockHeader.load(data)
            else:
                header = None
            self.block_locators[height] = (block_hash, header)
        return self


class Message(Serialize, Deserialize, metaclass=ABCMeta):
    class Type(IntEnumSerialize):
        BlockRequest = 0
        BlockResponse = 1
        ChallengeRequest = 2
        ChallengeResponse = 3
        Disconnect = 4
        PeerRequest = 5
        PeerResponse = 6
        Ping = 7
        Pong = 8
        UnconfirmedBlock = 9
        UnconfirmedTransaction = 10
        PoolRegister = 11
        PoolRequest = 12
        PoolResponse = 13
        NewBlockTemplate = 100
        PoolBlock = 101

        def __str__(self):
            return self.name

        def __repr__(self):
            return self.__class__.__name__ + "." + self.name

    @property
    @abstractmethod
    def type(self):
        raise NotImplementedError


class ChallengeRequest(Message):
    type = Message.Type.ChallengeRequest

    def __init__(self):
        self.version = None
        self.fork_depth = None
        self.node_type = None
        self.peer_status = None
        self.listener_port = None
        self.peer_nonce = None
        self.peer_cumulative_weight = None

    @classmethod
    def init(cls, *, version: u32, fork_depth: u32, node_type: NodeType, peer_status: Status,
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
        self = cls()
        self.version = version
        self.fork_depth = fork_depth
        self.node_type = node_type
        self.peer_status = peer_status
        self.listener_port = listener_port
        self.peer_nonce = peer_nonce
        self.peer_cumulative_weight = peer_cumulative_weight
        return self

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
        self = cls()
        if not isinstance(data, bytearray):
            raise TypeError("data must be bytearray")
        if len(data) != 42:
            raise ValueError("incorrect length")
        self.version = u32.load(data)
        self.fork_depth = u32.load(data)
        self.node_type = NodeType.load(data)
        self.peer_status = Status.load(data)
        self.listener_port = u16.load(data)
        self.peer_nonce = u64.load(data)
        self.peer_cumulative_weight = u128.load(data)
        return self

    def __str__(self):
        return "ChallengeRequest(version={}, fork_depth={}, node_type={}, peer_status={}, listener_port={}, peer_nonce={}, peer_cumulative_weight={})".format(
            self.version, self.fork_depth, self.node_type, self.peer_status, self.listener_port, self.peer_nonce,
            self.peer_cumulative_weight)

    def __repr__(self):
        return self.__str__()


class ChallengeResponse(Message):
    type = Message.Type.ChallengeResponse

    def __init__(self):
        self.block_header = None

    @classmethod
    def init(cls, *, block_header: BlockHeader):
        if not isinstance(block_header, BlockHeader):
            raise TypeError("block_header must be BlockHeader")
        self = cls()
        self.block_header = block_header
        return self

    def dump(self) -> bytes:
        return self.block_header.dump()

    @classmethod
    def load(cls, data: bytearray):
        self = cls()
        if not isinstance(data, bytearray):
            raise TypeError("data must be bytearray")
        self.block_header = BlockHeader.load(data)
        return self


class Ping(Message):
    type = Message.Type.Ping

    def __init__(self):
        self.version = None
        self.fork_depth = None
        self.node_type = None
        self.status = None
        self.block_hash = None
        self.block_header = None

    @classmethod
    def init(cls, *, version: u32, fork_depth: u32, node_type: NodeType, status: Status,
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
        self = cls()
        self.version = version
        self.fork_depth = fork_depth
        self.node_type = node_type
        self.status = status
        self.block_hash = block_hash
        self.block_header = block_header
        return self

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
        self = cls()
        self.version = u32.load(data)
        self.fork_depth = u32.load(data)
        self.node_type = NodeType.load(data)
        self.status = Status.load(data)
        self.block_hash = BlockHash.load(data)
        self.block_header = BlockHeader.load(data)
        return self


class Pong(Message):
    type = Message.Type.Pong

    def __init__(self):
        self.is_fork = None
        self.block_locators = None

    @classmethod
    def init(cls, *, is_fork: bool_ | NoneType, block_locators: BlockLocators):
        if not isinstance(is_fork, bool_ | NoneType):
            raise TypeError("is_fork must be bool_ | None")
        if not isinstance(block_locators, BlockLocators):
            raise TypeError("block_locators must be BlockLocators")
        self = cls()
        self.is_fork = is_fork
        self.block_locators = block_locators
        return self

    def dump(self) -> bytes:
        match self.is_fork:
            case None:
                res = u8()
            case True:
                res = u8(1)
            case False:
                res = u8(2)
            case _:
                raise ValueError("is_fork is not bool_ | None")
        return b"".join([
            res.dump(),
            self.block_locators.dump(),
        ])

    @classmethod
    def load(cls, data: bytearray):
        if not isinstance(data, bytearray):
            raise TypeError("data must be bytearray")
        self = cls()
        fork_flag = u8.load(data)
        match fork_flag:
            case 0:
                self.is_fork = None
            case 1:
                self.is_fork = True
            case 2:
                self.is_fork = False
            case _:
                raise ValueError("fork_flag is not 0, 1, or 2")
        # deferred Data type ignored
        del data[:8]
        self.block_locators = BlockLocators.load(data)
        return self


class UnconfirmedBlock(Message):
    type = Message.Type.UnconfirmedBlock

    def __init__(self):
        self.block_height = None
        self.block_hash = None
        self.block = None

    @classmethod
    def init(cls, *, block_height: u32, block_hash: BlockHash, block: Block):
        if not isinstance(block_height, u32):
            raise TypeError("block_height must be u32")
        if not isinstance(block_hash, BlockHash):
            raise TypeError("block_hash must be BlockHash")
        if not isinstance(block, Block):
            raise TypeError("block must be Block")
        self = cls()
        self.block_height = block_height
        self.block_hash = block_hash
        self.block = block
        return self

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
        self = cls()
        self.block_height = u32.load(data)
        self.block_hash = BlockHash.load(data)
        # deferred Data type ignored
        del data[:8]
        self.block = Block.load(data)
        return self


class Frame(Serialize, Deserialize):

    def __init__(self):
        self.type = None
        self.message = None

    @classmethod
    def init(cls, *, type_: Message.Type, message: Message):
        if not isinstance(type_, Message.Type):
            raise TypeError("type must be Message.Type")
        if not isinstance(message, Message):
            raise TypeError("message must be Message")
        self = cls()
        self.type = type_
        self.message = message
        return self

    def dump(self) -> bytes:
        return self.type.to_bytes(2, "little") + self.message.dump()

    @classmethod
    def load(cls, data: bytearray):
        self = cls()
        if not isinstance(data, bytearray):
            raise TypeError("data must be bytearray")
        if len(data) < 2:
            raise ValueError("missing message id")
        self.type = Message.Type(struct.unpack("<H", data[:2])[0])
        del data[:2]
        match self.type:
            case Message.Type.ChallengeRequest:
                self.message = ChallengeRequest.load(data)
            case Message.Type.ChallengeResponse:
                self.message = ChallengeResponse.load(data)
            case Message.Type.Ping:
                self.message = Ping.load(data)
            case Message.Type.Pong:
                self.message = Pong.load(data)
            case Message.Type.UnconfirmedBlock:
                self.message = UnconfirmedBlock.load(data)
            case _:
                raise ValueError("unknown message type")

        return self

    def __str__(self):
        return f"Frame(type={self.type}, message={self.message})"

    def __repr__(self):
        return self.__str__()
