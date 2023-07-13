from enum import auto, Enum
from types import NoneType
from typing import Literal as TLiteral

from .vm_basic import *


class StringType(Serializable):

    def __init__(self, *, string: str):
        self.string = string

    def dump(self) -> bytes:
        bytes_ = self.string.encode("utf-8")
        if len(bytes_) > 255:
            raise ValueError("string too long")
        return u16(len(bytes_)).dump() + bytes_

    @classmethod
    def load(cls, data: BytesIO):
        length = u16.load(data)
        string = data.read(length).decode("utf-8")
        return cls(string=string)

    @classmethod
    def loads(cls, data: str):
        return cls(string=data)

    def __str__(self):
        return self.string

class Literal(Serializable): # enum

    class Type(IntEnumu16):
        Address = 0
        Boolean = 1
        Field = 2
        Group = 3
        I8 = 4
        I16 = 5
        I32 = 6
        I64 = 7
        I128 = 8
        U8 = 9
        U16 = 10
        U32 = 11
        U64 = 12
        U128 = 13
        Scalar = 14
        String = 15

    primitive_type_map = {
        Type.Address: Address,
        Type.Boolean: bool_,
        Type.Field: Field,
        Type.Group: Group,
        Type.I8: i8,
        Type.I16: i16,
        Type.I32: i32,
        Type.I64: i64,
        Type.I128: i128,
        Type.U8: u8,
        Type.U16: u16,
        Type.U32: u32,
        Type.U64: u64,
        Type.U128: u128,
        Type.Scalar: Scalar,
        Type.String: StringType,
    }

    reverse_primitive_type_map = {
        Address: Type.Address,
        bool_: Type.Boolean,
        Field: Type.Field,
        Group: Type.Group,
        i8: Type.I8,
        i16: Type.I16,
        i32: Type.I32,
        i64: Type.I64,
        i128: Type.I128,
        u8: Type.U8,
        u16: Type.U16,
        u32: Type.U32,
        u64: Type.U64,
        u128: Type.U128,
        Scalar: Type.Scalar,
        StringType: Type.String,
    }

    def __init__(self, *, type_: Type, primitive: Serializable):
        self.type = type_
        self.primitive = primitive

    def dump(self) -> bytes:
        return self.type.dump() + self.primitive.dump()

    @classmethod
    def load(cls, data: BytesIO):
        type_ = cls.Type.load(data)
        primitive = cls.primitive_type_map[type_].load(data)
        return cls(type_=type_, primitive=primitive)

    @classmethod
    def loads(cls, type_: Type, data: str):
        return cls.primitive_type_map[type_].loads(data)

    def __str__(self):
        import disasm.aleo
        return disasm.aleo.disasm_literal(self)

    def __eq__(self, other: object):
        if not isinstance(other, Literal):
            return False
        if not isinstance(self.primitive, Comparable):
            return False
        return self.type == other.type and self.primitive == other.primitive

    def __gt__(self, other: Self):
        if not isinstance(self.primitive, Comparable):
            return False
        return self.type == other.type and self.primitive > other.primitive

    def __ge__(self, other: Self):
        if not isinstance(self.primitive, Comparable):
            return False
        return self.type == other.type and self.primitive >= other.primitive


class Identifier(Serializable):

    def __init__(self, *, value: str):
        if value == "":
            raise ValueError("identifier cannot be empty")
        if not all(map(lambda c: 0x30 <= c <= 0x39 or 0x41 <= c <= 0x5a or 0x61 <= 0x7a, map(ord, value))):
            raise ValueError(f"identifier '{value}' must consist of letters, digits, and underscores")
        if value[0].isdigit():
            raise ValueError("identifier must start with a letter")
        max_bytes = 31 # 253 - 1 bits
        if len(value) > max_bytes:
            raise ValueError(f"identifier '{value}' must be at most {max_bytes} bytes")
        self.data = value

    def dump(self) -> bytes:
        return len(self.data).to_bytes(1, "little") + self.data.encode("ascii")

    @classmethod
    def load(cls, data: BytesIO):
        if data.tell() >= data.getbuffer().nbytes:
            raise ValueError("incorrect length")
        length = data.read(1)[0]
        if data.tell() + length > data.getbuffer().nbytes:
            raise ValueError("incorrect length")
        value = data.read(length).decode("ascii") # let the exception propagate
        return cls(value=value)

    @classmethod
    def loads(cls, data: str):
        return cls(value=data)

    def __str__(self):
        return self.data

    def __eq__(self, other: object):
        if isinstance(other, str):
            return self.data == other
        if isinstance(other, Identifier):
            return self.data == other.data
        return False

    def __hash__(self):
        return hash(self.data)

class ProgramID(Serializable):

    def __init__(self, *, name: Identifier, network: Identifier):
        self.name = name
        self.network = network

    def dump(self) -> bytes:
        return self.name.dump() + self.network.dump()

    @classmethod
    def load(cls, data: BytesIO):
        name = Identifier.load(data)
        network = Identifier.load(data)
        return cls(name=name, network=network)

    @classmethod
    def loads(cls, data: str):
        (name, network) = data.split(".")
        return cls(name=Identifier(value=name), network=Identifier(value=network))

    def __str__(self):
        return f"{self.name}.{self.network}"

    def __eq__(self, other: object):
        if isinstance(other, str):
            return str(self) == other
        if isinstance(other, ProgramID):
            return self.name == other.name and self.network == other.network
        return False


class Import(Serializable):

    def __init__(self, *, program_id: ProgramID):
        self.program_id = program_id

    def dump(self) -> bytes:
        return self.program_id.dump()

    @classmethod
    def load(cls, data: BytesIO):
        program_id = ProgramID.load(data)
        return cls(program_id=program_id)


class VarInt(int, Serializable):

    def __new__(cls, value: int):
        if value > 0xffffffffffffffff:
            raise ValueError("value is too big")
        return int.__new__(cls, value)

    def __init__(self, _): # type: ignore[reportInconsistentConstructor]
        pass

    def dump(self) -> bytes:
        if 0 <= self <= 0xfc:
            return self.to_bytes(1, "little")
        elif 0xfd <= self <= 0xffff:
            return b"\xfd" + self.to_bytes(2, "little")
        elif 0x10000 <= self <= 0xffffffff:
            return b"\xfe" + self.to_bytes(4, "little")
        elif 0x100000000 <= self <= 0xffffffffffffffff:
            return b"\xff" + self.to_bytes(8, "little")
        else:
            raise ValueError("unreachable")

    @classmethod
    def load(cls, data: BytesIO):
        if data.tell() >= data.getbuffer().nbytes:
            raise ValueError("data is too short")
        value = data.read(1)[0]
        if value == 0xfd:
            if data.tell() + 2 > data.getbuffer().nbytes:
                raise ValueError("data is too short")
            value = u16.load(data)
        elif value == 0xfe:
            if data.tell() + 4 > data.getbuffer().nbytes:
                raise ValueError("data is too short")
            value = u32.load(data)
        elif value == 0xff:
            if data.tell() + 8 > data.getbuffer().nbytes:
                raise ValueError("data is too short")
            value = u64.load(data)
        else:
            value = u8(value)
        return cls(value)


class Register(EnumBaseSerialize, Serialize, RustEnum):

    class Type(IntEnumu8):
        Locator = 0
        Member = 1

    type: Type
    locator: VarInt

    @classmethod
    def load(cls, data: BytesIO):
        type_ = cls.Type.load(data)
        if type_ == cls.Type.Locator:
            return LocatorRegister.load(data)
        elif type_ == cls.Type.Member:
            return MemberRegister.load(data)
        else:
            raise ValueError(f"Invalid register type {type_}")


class LocatorRegister(Register):
    type = Register.Type.Locator

    def __init__(self, *, locator: VarInt):
        self.locator = locator

    def dump(self) -> bytes:
        return self.type.dump() + self.locator.dump()

    @classmethod
    def load(cls, data: BytesIO):
        locator = VarInt.load(data)
        return cls(locator=locator)


class MemberRegister(Register):
    type = Register.Type.Member

    def __init__(self, *, locator: VarInt, identifiers: Vec[Identifier, u16]):
        self.locator = locator
        self.identifiers = identifiers

    def dump(self) -> bytes:
        return self.type.dump() + self.locator.dump() + self.identifiers.dump()

    @classmethod
    def load(cls, data: BytesIO):
        locator = VarInt.load(data)
        identifiers = Vec[Identifier, u16].load(data)
        return cls(locator=locator, identifiers=identifiers)


class Operand(EnumBaseSerialize, Serialize, RustEnum):

    class Type(IntEnumu8):
        Literal = 0
        Register = 1
        ProgramID = 2
        Caller = 3
        BlockHeight = 4

    @classmethod
    def load(cls, data: BytesIO):
        if data.tell() >= data.getbuffer().nbytes:
            raise ValueError("incorrect length")
        type_ = cls.Type.load(data)
        if type_ == cls.Type.Literal:
            return LiteralOperand.load(data)
        elif type_ == cls.Type.Register:
            return RegisterOperand.load(data)
        elif type_ == cls.Type.ProgramID:
            return ProgramIDOperand.load(data)
        elif type_ == cls.Type.Caller:
            return CallerOperand.load(data)
        elif type_ == cls.Type.BlockHeight:
            return BlockHeightOperand.load(data)
        else:
            raise ValueError("unknown operand type")

class LiteralOperand(Operand):
    type = Operand.Type.Literal

    def __init__(self, *, literal: Literal):
        self.literal = literal

    def dump(self) -> bytes:
        return self.type.dump() + self.literal.dump()

    @classmethod
    def load(cls, data: BytesIO):
        return cls(literal=Literal.load(data))


class RegisterOperand(Operand):
    type = Operand.Type.Register

    def __init__(self, *, register: Register):
        self.register = register

    def dump(self) -> bytes:
        return self.type.dump() + self.register.dump()

    @classmethod
    def load(cls, data: BytesIO):
        return cls(register=Register.load(data))


class ProgramIDOperand(Operand):
    type = Operand.Type.ProgramID

    def __init__(self, *, program_id: ProgramID):
        self.program_id = program_id

    def dump(self) -> bytes:
        return self.type.dump() + self.program_id.dump()

    @classmethod
    def load(cls, data: BytesIO):
        return cls(program_id=ProgramID.load(data))


class CallerOperand(Operand):
    type = Operand.Type.Caller

    def __init__(self):
        pass

    def dump(self) -> bytes:
        return self.type.dump()

    @classmethod
    def load(cls, data: BytesIO):
        return cls()

class BlockHeightOperand(Operand):
    type = Operand.Type.BlockHeight

    def __init__(self):
        pass

    def dump(self) -> bytes:
        return self.type.dump()

    @classmethod
    def load(cls, data: BytesIO):
        return cls()

N = TypeVar("N", bound=int)

@access_generic_type
class Literals(Serializable, Generic[N]):
    types: tuple[N]

    def __init__(self, *, operands: Vec[Operand, N], destination: Register):
        self.num_operands = self.types[0]
        self.operands = operands
        self.destination = destination

    def dump(self) -> bytes:
        res = b""
        for i in range(self.num_operands):
            res += self.operands[i].dump()
        res += self.destination.dump()
        return res

    @classmethod
    def load(cls, data: BytesIO, *, types: Optional[tuple[N]] = None):
        if types is None:
            raise ValueError("types must be specified")
        num_operands = types[0]
        operands: list[Operand] = []
        for _ in range(num_operands):
            operands.append(Operand.load(data))
        destination = Register.load(data)
        return cls(operands=Vec[Operand, TLiteral[num_operands]](operands), destination=destination)


@access_generic_type
class AssertInstruction(Serializable, Generic[N]):
    types: tuple[N]

    def __init__(self, *, operands: Vec[Operand, 2]):
        self.operands = operands

    def dump(self) -> bytes:
        return self.operands.dump()

    @classmethod
    def load(cls, data: BytesIO, *, types: Optional[tuple[N]] = None):
        return cls(operands=Vec[Operand, 2].load(data))


class Locator(Serializable):

    def __init__(self, *, id_: ProgramID, resource: Identifier):
        self.id = id_
        self.resource = resource

    def dump(self) -> bytes:
        return self.id.dump() + self.resource.dump()

    @classmethod
    def load(cls, data: BytesIO):
        id_ = ProgramID.load(data)
        resource = Identifier.load(data)
        return cls(id_=id_, resource=resource)

    def __str__(self):
        return f"{self.id}/{self.resource}"


class CallOperator(EnumBaseSerialize, Serialize, RustEnum):

    class Type(IntEnumu8):
        Locator = 0
        Resource = 1

    @classmethod
    def load(cls, data: BytesIO):
        if data.tell() >= data.getbuffer().nbytes:
            raise ValueError("incorrect length")
        type_ = cls.Type.load(data)
        if type_ == cls.Type.Locator:
            return LocatorCallOperator.load(data)
        elif type_ == cls.Type.Resource:
            return ResourceCallOperator.load(data)
        else:
            raise ValueError("unknown call operator type")


class LocatorCallOperator(CallOperator):
    type = CallOperator.Type.Locator

    def __init__(self, *, locator: Locator):
        self.locator = locator

    def dump(self) -> bytes:
        return self.type.dump() + self.locator.dump()

    @classmethod
    def load(cls, data: BytesIO):
        return cls(locator=Locator.load(data))


class ResourceCallOperator(CallOperator):
    type = CallOperator.Type.Resource

    def __init__(self, *, resource: Identifier):
        self.resource = resource

    def dump(self) -> bytes:
        return self.type.dump() + self.resource.dump()

    @classmethod
    def load(cls, data: BytesIO):
        return cls(resource=Identifier.load(data))


class CallInstruction(Serializable):

    def __init__(self, *, operator: CallOperator, operands: Vec[Operand, u8], destinations: Vec[Register, u8]):
        self.operator = operator
        self.operands = operands
        self.destinations = destinations

    def dump(self) -> bytes:
        return self.operator.dump() + self.operands.dump() + self.destinations.dump()

    @classmethod
    def load(cls, data: BytesIO):
        operator = CallOperator.load(data)
        operands = Vec[Operand, u8].load(data)
        destinations = Vec[Register, u8].load(data)
        return cls(operator=operator, operands=operands, destinations=destinations)

class LiteralType(IntEnumu8):
    Address = 0
    Boolean = 1
    Field = 2
    Group = 3
    I8 = 4
    I16 = 5
    I32 = 6
    I64 = 7
    I128 = 8
    U8 = 9
    U16 = 10
    U32 = 11
    U64 = 12
    U128 = 13
    Scalar = 14
    String = 15

    @property
    def primitive_type(self):
        return {
            self.Address: Address,
            self.Boolean: bool_,
            self.Field: Field,
            self.Group: Group,
            self.I8: i8,
            self.I16: i16,
            self.I32: i32,
            self.I64: i64,
            self.I128: i128,
            self.U8: u8,
            self.U16: u16,
            self.U32: u32,
            self.U64: u64,
            self.U128: u128,
            self.Scalar: Scalar,
            self.String: StringType,
        }[self]

    def __str__(self):
        return {
            self.Address: "address",
            self.Boolean: "bool",
            self.Field: "field",
            self.Group: "group",
            self.I8: "i8",
            self.I16: "i16",
            self.I32: "i32",
            self.I64: "i64",
            self.I128: "i128",
            self.U8: "u8",
            self.U16: "u16",
            self.U32: "u32",
            self.U64: "u64",
            self.U128: "u128",
            self.Scalar: "scalar",
            self.String: "string",
        }[self]


class PlaintextType(EnumBaseSerialize, Serialize, RustEnum):

    class Type(IntEnumu8):
        Literal = 0
        Struct = 1

    type: Type

    @classmethod
    def load(cls, data: BytesIO):
        if data.tell() >= data.getbuffer().nbytes:
            raise ValueError("incorrect length")
        type_ = cls.Type.load(data)
        if type_ == cls.Type.Literal:
            return LiteralPlaintextType.load(data)
        if type_ == cls.Type.Struct:
            return StructPlaintextType.load(data)
        raise ValueError("unknown type")


class LiteralPlaintextType(PlaintextType):
    type = PlaintextType.Type.Literal

    def __init__(self, *, literal_type: LiteralType):
        self.literal_type = literal_type

    def dump(self) -> bytes:
        return self.type.dump() + self.literal_type.dump()

    @classmethod
    def load(cls, data: BytesIO):
        literal_type = LiteralType.load(data)
        return cls(literal_type=literal_type)

    def __str__(self):
        return str(self.literal_type)


class StructPlaintextType(PlaintextType):
    type = PlaintextType.Type.Struct

    def __init__(self, *, struct_: Identifier):
        self.struct = struct_

    def dump(self) -> bytes:
        return self.type.dump() + self.struct.dump()

    @classmethod
    def load(cls, data: BytesIO):
        struct_ = Identifier.load(data)
        return cls(struct_=struct_)

    def __str__(self):
        return str(self.struct)

class RegisterType(EnumBaseSerialize, Serialize, RustEnum):

    class Type(IntEnumu8):
        Plaintext = 0
        Record = 1
        ExternalRecord = 2

    @classmethod
    def load(cls, data: BytesIO):
        type_ = cls.Type.load(data)
        if type_ == cls.Type.Plaintext:
            return PlaintextRegisterType.load(data)
        elif type_ == cls.Type.Record:
            return RecordRegisterType.load(data)
        elif type_ == cls.Type.ExternalRecord:
            return ExternalRecordRegisterType.load(data)
        else:
            raise ValueError(f"Invalid register type {type_}")


class PlaintextRegisterType(RegisterType):
    type = RegisterType.Type.Plaintext

    def __init__(self, *, plaintext_type: PlaintextType):
        self.plaintext_type = plaintext_type

    def dump(self) -> bytes:
        return self.type.dump() + self.plaintext_type.dump()

    @classmethod
    def load(cls, data: BytesIO):
        plaintext_type = PlaintextType.load(data)
        return cls(plaintext_type=plaintext_type)


class RecordRegisterType(RegisterType):
    type = RegisterType.Type.Record

    def __init__(self, *, identifier: Identifier):
        self.identifier = identifier

    def dump(self) -> bytes:
        return self.type.dump() + self.identifier.dump()

    @classmethod
    def load(cls, data: BytesIO):
        identifier = Identifier.load(data)
        return cls(identifier=identifier)


class ExternalRecordRegisterType(RegisterType):
    type = RegisterType.Type.ExternalRecord

    def __init__(self, *, locator: Locator):
        self.locator = locator

    def dump(self) -> bytes:
        return self.type.dump() + self.locator.dump()

    @classmethod
    def load(cls, data: BytesIO):
        locator = Locator.load(data)
        return cls(locator=locator)

class CastType(EnumBaseSerialize, Serialize, RustEnum):

    class Type(IntEnumu8):
        GroupXCoordinate = 0
        GroupYCoordinate = 1
        RegisterType = 2

    @classmethod
    def load(cls, data: BytesIO):
        type_ = cls.Type.load(data)
        if type_ == cls.Type.GroupXCoordinate:
            return GroupXCoordinateCastType.load(data)
        elif type_ == cls.Type.GroupYCoordinate:
            return GroupYCoordinateCastType.load(data)
        elif type_ == cls.Type.RegisterType:
            return RegisterTypeCastType.load(data)
        else:
            raise ValueError(f"Invalid cast type {type_}")

class GroupXCoordinateCastType(CastType):
    type = CastType.Type.GroupXCoordinate

    def dump(self) -> bytes:
        return self.type.dump()

    @classmethod
    def load(cls, data: BytesIO):
        return cls()

class GroupYCoordinateCastType(CastType):
    type = CastType.Type.GroupYCoordinate

    def dump(self) -> bytes:
        return self.type.dump()

    @classmethod
    def load(cls, data: BytesIO):
        return cls()

class RegisterTypeCastType(CastType):
    type = CastType.Type.RegisterType

    def __init__(self, *, register_type: RegisterType):
        self.register_type = register_type

    def dump(self) -> bytes:
        return self.type.dump() + self.register_type.dump()

    @classmethod
    def load(cls, data: BytesIO):
        register_type = RegisterType.load(data)
        return cls(register_type=register_type)


class CastInstruction(Serializable):

    def __init__(self, *, operands: Vec[Operand, u8], destination: Register, cast_type: CastType):
        self.operands = operands
        self.destination = destination
        self.cast_type = cast_type

    def dump(self) -> bytes:
        return self.operands.dump() + self.destination.dump() + self.cast_type.dump()

    @classmethod
    def load(cls, data: BytesIO):
        operands = Vec[Operand, u8].load(data)
        destination = Register.load(data)
        cast_Type = CastType.load(data)
        return cls(operands=operands, destination=destination, cast_type=cast_Type)

E = TypeVar('E', bound=Enum)

@access_generic_type
class CommitInstruction(Serializable, Generic[E]):
    types: tuple[E]

    class Type(Enum):
        CommitBHP256 = 0
        CommitBHP512 = 1
        CommitBHP768 = 2
        CommitBHP1024 = 3
        CommitPED64 = 4
        CommitPED128 = 5

    def __init__(self, *, operands: Vec[Operand, 2], destination: Register, destination_type: LiteralType):
        self.type = self.types[0]
        self.operands = operands
        self.destination = destination
        self.destination_type = destination_type

    def dump(self) -> bytes:
        return self.operands.dump() + self.destination.dump() + self.destination_type.dump()

    @classmethod
    def load(cls, data: BytesIO, *, types: Optional[tuple[E]] = None):
        if not types:
            raise ValueError("expected types")
        operands = Vec[Operand, 2].load(data)
        destination = Register.load(data)
        destination_type = LiteralType.load(data)
        return cls(operands=operands, destination=destination, destination_type=destination_type)


@access_generic_type
class HashInstruction(Serializable, Generic[E]):
    types: tuple[E]

    class Type(Enum):
        HashBHP256 = 0
        HashBHP512 = 1
        HashBHP768 = 2
        HashBHP1024 = 3
        HashPED64 = 4
        HashPED128 = 5
        HashPSD2 = 6
        HashPSD4 = 7
        HashPSD8 = 8
        HashManyPSD2 = 9
        HashManyPSD4 = 10
        HashManyPSD8 = 11

    # shortcut here so check doesn't work
    def __init__(self, *, operands: Vec[Operand | NoneType, 2], destination: Register, destination_type: LiteralType):
        self.type = self.types[0]
        self.operands = operands
        self.destination = destination
        self.destination_type = destination_type

    @classmethod
    def num_operands(cls, type_: Type):
        if type_ in [cls.Type.HashManyPSD2, cls.Type.HashManyPSD4, cls.Type.HashManyPSD8]:
            return 2
        return 1

    def dump(self) -> bytes:
        return self.operands.dump() + self.destination.dump() + self.destination_type.dump()

    @classmethod
    def load(cls, data: BytesIO, *, types: Optional[tuple[E]] = None):
        if types is None:
            raise ValueError("expected types")
        if not isinstance(types[0], cls.Type):
            raise ValueError("expected types to be of type HashInstruction.Type")
        operands = Vec[Operand, cls.num_operands(types[0])].load(data)
        destination = Register.load(data)
        destination_type = LiteralType.load(data)
        return cls(operands=operands, destination=destination, destination_type=destination_type)


class Instruction(Serializable):

    class Type(IntEnumu16):

        @staticmethod
        def _generate_next_value_(name: str, start: int, count: int, last_values: list[int]):
            return count

        Abs = auto()
        AbsWrapped = auto()
        Add = auto()
        AddWrapped = auto()
        And = auto()
        AssertEq = auto()
        AssertNeq = auto()
        Call = auto()
        Cast = auto()
        CommitBHP256 = auto()
        CommitBHP512 = auto()
        CommitBHP768 = auto()
        CommitBHP1024 = auto()
        CommitPED64 = auto()
        CommitPED128 = auto()
        Div = auto()
        DivWrapped = auto()
        Double = auto()
        GreaterThan = auto()
        GreaterThanOrEqual = auto()
        HashBHP256 = auto()
        HashBHP512 = auto()
        HashBHP768 = auto()
        HashBHP1024 = auto()
        HashPED64 = auto()
        HashPED128 = auto()
        HashPSD2 = auto()
        HashPSD4 = auto()
        HashPSD8 = auto()
        HashManyPSD2 = auto()
        HashManyPSD4 = auto()
        HashManyPSD8 = auto()
        Inv = auto()
        IsEq = auto()
        IsNeq = auto()
        LessThan = auto()
        LessThanOrEqual = auto()
        Modulo = auto()
        Mul = auto()
        MulWrapped = auto()
        Nand = auto()
        Neg = auto()
        Nor = auto()
        Not = auto()
        Or = auto()
        Pow = auto()
        PowWrapped = auto()
        Rem = auto()
        RemWrapped = auto()
        Shl = auto()
        ShlWrapped = auto()
        Shr = auto()
        ShrWrapped = auto()
        Square = auto()
        SquareRoot = auto()
        Sub = auto()
        SubWrapped = auto()
        Ternary = auto()
        Xor = auto()

    type: Type

    # Some types are not implemented as Literals originally,
    # but binary wise they have the same behavior (operands, destination)
    type_map = {
        Type.Abs: Literals[1],
        Type.AbsWrapped: Literals[1],
        Type.Add: Literals[2],
        Type.AddWrapped: Literals[2],
        Type.And: Literals[2],
        Type.AssertEq: AssertInstruction[0],
        Type.AssertNeq: AssertInstruction[1],
        Type.Call: CallInstruction,
        Type.Cast: CastInstruction,
        Type.CommitBHP256: CommitInstruction[CommitInstruction.Type.CommitBHP256],
        Type.CommitBHP512: CommitInstruction[CommitInstruction.Type.CommitBHP512],
        Type.CommitBHP768: CommitInstruction[CommitInstruction.Type.CommitBHP768],
        Type.CommitBHP1024: CommitInstruction[CommitInstruction.Type.CommitBHP1024],
        Type.CommitPED64: CommitInstruction[CommitInstruction.Type.CommitPED64],
        Type.CommitPED128: CommitInstruction[CommitInstruction.Type.CommitPED128],
        Type.Div: Literals[2],
        Type.DivWrapped: Literals[2],
        Type.Double: Literals[1],
        Type.GreaterThan: Literals[2],
        Type.GreaterThanOrEqual: Literals[2],
        Type.HashBHP256: HashInstruction[HashInstruction.Type.HashBHP256],
        Type.HashBHP512: HashInstruction[HashInstruction.Type.HashBHP512],
        Type.HashBHP768: HashInstruction[HashInstruction.Type.HashBHP768],
        Type.HashBHP1024: HashInstruction[HashInstruction.Type.HashBHP1024],
        Type.HashPED64: HashInstruction[HashInstruction.Type.HashPED64],
        Type.HashPED128: HashInstruction[HashInstruction.Type.HashPED128],
        Type.HashPSD2: HashInstruction[HashInstruction.Type.HashPSD2],
        Type.HashPSD4: HashInstruction[HashInstruction.Type.HashPSD4],
        Type.HashPSD8: HashInstruction[HashInstruction.Type.HashPSD8],
        Type.HashManyPSD2: HashInstruction[HashInstruction.Type.HashManyPSD2],
        Type.HashManyPSD4: HashInstruction[HashInstruction.Type.HashManyPSD4],
        Type.HashManyPSD8: HashInstruction[HashInstruction.Type.HashManyPSD8],
        Type.Inv: Literals[1],
        Type.IsEq: Literals[2],
        Type.IsNeq: Literals[2],
        Type.LessThan: Literals[2],
        Type.LessThanOrEqual: Literals[2],
        Type.Modulo: Literals[2],
        Type.Mul: Literals[2],
        Type.MulWrapped: Literals[2],
        Type.Nand: Literals[2],
        Type.Neg: Literals[1],
        Type.Nor: Literals[2],
        Type.Not: Literals[1],
        Type.Or: Literals[2],
        Type.Pow: Literals[2],
        Type.PowWrapped: Literals[2],
        Type.Rem: Literals[2],
        Type.RemWrapped: Literals[2],
        Type.Shl: Literals[2],
        Type.ShlWrapped: Literals[2],
        Type.Shr: Literals[2],
        Type.ShrWrapped: Literals[2],
        Type.Square: Literals[1],
        Type.SquareRoot: Literals[1],
        Type.Sub: Literals[2],
        Type.SubWrapped: Literals[2],
        Type.Ternary: Literals[3],
        Type.Xor: Literals[2],
    }

    # used by feature hash
    feature_map = {
        Type.Abs: "U",
        Type.AbsWrapped: "U",
        Type.Add: "B",
        Type.AddWrapped: "B",
        Type.And: "B",
        Type.AssertEq: "B",
        Type.AssertNeq: "B",
        Type.Call: "C",
        Type.Cast: "X",
        Type.CommitBHP256: "M",
        Type.CommitBHP512: "M",
        Type.CommitBHP768: "M",
        Type.CommitBHP1024: "M",
        Type.CommitPED64: "M",
        Type.CommitPED128: "M",
        Type.Div: "B",
        Type.DivWrapped: "B",
        Type.Double: "U",
        Type.GreaterThan: "P",
        Type.GreaterThanOrEqual: "P",
        Type.HashBHP256: "H",
        Type.HashBHP512: "H",
        Type.HashBHP768: "H",
        Type.HashBHP1024: "H",
        Type.HashPED64: "H",
        Type.HashPED128: "H",
        Type.HashPSD2: "H",
        Type.HashPSD4: "H",
        Type.HashPSD8: "H",
        Type.HashManyPSD2: "H",
        Type.HashManyPSD4: "H",
        Type.HashManyPSD8: "H",
        Type.Inv: "U",
        Type.IsEq: "P",
        Type.IsNeq: "P",
        Type.LessThan: "P",
        Type.LessThanOrEqual: "P",
        Type.Modulo: "B",
        Type.Mul: "B",
        Type.MulWrapped: "B",
        Type.Nand: "B",
        Type.Neg: "U",
        Type.Nor: "B",
        Type.Not: "U",
        Type.Or: "B",
        Type.Pow: "B",
        Type.PowWrapped: "B",
        Type.Rem: "B",
        Type.RemWrapped: "B",
        Type.Shl: "B",
        Type.ShlWrapped: "B",
        Type.Shr: "B",
        Type.ShrWrapped: "B",
        Type.Square: "U",
        Type.SquareRoot: "U",
        Type.Sub: "B",
        Type.SubWrapped: "B",
        Type.Ternary: "T",
        Type.Xor: "B",
    }

    fee_map = {
        Type.Abs: 2_000,
        Type.AbsWrapped: 2_000,
        Type.Add: 2_000,
        Type.AddWrapped: 2_000,
        Type.And: 2_000,
        Type.AssertEq: 2_000,
        Type.AssertNeq: 2_000,
        Type.Call: -1,
        Type.Cast: 2_000,
        Type.CommitBHP256: 200_000,
        Type.CommitBHP512: 200_000,
        Type.CommitBHP768: 200_000,
        Type.CommitBHP1024: 200_000,
        Type.CommitPED64: 100_000,
        Type.CommitPED128: 100_000,
        Type.Div: 10_000,
        Type.DivWrapped: 2_000,
        Type.Double: 2_000,
        Type.GreaterThan: 2_000,
        Type.GreaterThanOrEqual: 2_000,
        Type.HashBHP256: 200_000,
        Type.HashBHP512: 100_000,
        Type.HashBHP768: 100_000,
        Type.HashBHP1024: 100_000,
        Type.HashPED64: 20_000,
        Type.HashPED128: 30_000,
        Type.HashPSD2: {
            "high": 600_000,
            "low": 60_000,
        },
        Type.HashPSD4: {
            "high": 700_000,
            "low": 100_000,
        },
        Type.HashPSD8: {
            "high": 800_000,
            "low": 200_000,
        },
        Type.HashManyPSD2: -1,
        Type.HashManyPSD4: -1,
        Type.HashManyPSD8: -1,
        Type.Inv: 10_000,
        Type.IsEq: 2_000,
        Type.IsNeq: 2_000,
        Type.LessThan: 2_000,
        Type.LessThanOrEqual: 2_000,
        Type.Modulo: 2_000,
        Type.Mul: 150_000,
        Type.MulWrapped: 2_000,
        Type.Nand: 2_000,
        Type.Neg: 2_000,
        Type.Nor: 2_000,
        Type.Not: 2_000,
        Type.Or: 2_000,
        Type.Pow: 20_000,
        Type.PowWrapped: 2_000,
        Type.Rem: 2_000,
        Type.RemWrapped: 2_000,
        Type.Shl: 2_000,
        Type.ShlWrapped: 2_000,
        Type.Shr: 2_000,
        Type.ShrWrapped: 2_000,
        Type.Square: 2_000,
        Type.SquareRoot: 120_000,
        Type.Sub: 10_000,
        Type.SubWrapped: 2_000,
        Type.Ternary: 2_000,
        Type.Xor: 2_000,
    }

    def a(self, b: Vec[u8, u8]):
        reveal_type(b)

    def __init__(self, *, type_: Type, literals: Literals):
        self.type = type_
        reveal_type(literals)
        self.literals = literals

    def dump(self) -> bytes:
        return self.type.dump() + self.literals.dump()

    @classmethod
    def load(cls, data: BytesIO):
        type_ = cls.Type.load(data)
        instruction_type = cls.type_map[type_]
        literals: Literals | AssertInstruction | CallInstruction | CastInstruction | CommitInstruction | HashInstruction = instruction_type.load(data)
        reveal_type(literals)
        return cls(type_=type_, literals=literals)

    @property
    def cost(self) -> int:
        if self.type in (self.Type.HashPSD2, self.Type.HashPSD4, self.Type.HashPSD8):
            if not isinstance(self, HashInstruction):
                raise ValueError(f"expected HashInstruction, got {self}")
            inst: HashInstruction = self
            # need to redesign the fee map to go fully static
            fee_dict: dict[str, int] = Instruction.fee_map[self.type] # type: ignore[reportGeneralTypeIssues]
            if inst.destination_type in (LiteralType.Address, LiteralType.Group):
                return fee_dict["high"]
            else:
                return fee_dict["low"]
        cost = Instruction.fee_map[self.type]
        if cost == -1:
            raise ValueError(f"instruction {self.type} is not supported in finalize")
        return cost # type: ignore[reportGeneralTypeIssues]
