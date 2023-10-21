from enum import auto, EnumType

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
        Signature = 15
        String = 16

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
        Type.Signature: Signature,
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
        Signature: Type.Signature,
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
        return cls(type_=type_, primitive=cls.primitive_type_map[type_].loads(data))

    def __str__(self):
        import disasm.aleo
        return disasm.aleo.disasm_literal(self)

    def __eq__(self, other: object):
        if not isinstance(other, Literal):
            return False
        if not isinstance(self.primitive, Equal):
            return False
        return self.type == other.type and self.primitive == other.primitive

    def __gt__(self, other: Self):
        if not isinstance(self.primitive, Compare):
            return False
        return self.type == other.type and self.primitive > other.primitive

    def __ge__(self, other: Self):
        if not isinstance(self.primitive, Compare):
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
        length = data.read(1)[0]
        value = data.read(length).decode("ascii") # let the exception propagate
        return cls(value=value)

    @classmethod
    def loads(cls, data: str):
        return cls(value=data)

    def __str__(self):
        return self.data

    def __repr__(self):
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
        value = data.read(1)[0]
        if value == 0xfd:
            value = u16.load(data)
        elif value == 0xfe:
            value = u32.load(data)
        elif value == 0xff:
            value = u64.load(data)
        else:
            value = u8(value)
        return cls(value)


class Register(EnumBaseSerialize, Serialize, RustEnum):

    class Type(IntEnumu8):
        Locator = 0
        Access = 1

    type: Type
    locator: VarInt

    @classmethod
    def load(cls, data: BytesIO):
        type_ = cls.Type.load(data)
        if type_ == cls.Type.Locator:
            return LocatorRegister.load(data)
        elif type_ == cls.Type.Access:
            return AccessRegister.load(data)
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


class Access(EnumBaseSerialize, Serialize, RustEnum):

    class Type(IntEnumu8):
        Member = 0
        Index = 1

    @classmethod
    def load(cls, data: BytesIO):
        type_ = cls.Type.load(data)
        if type_ == cls.Type.Member:
            return MemberAccess.load(data)
        elif type_ == cls.Type.Index:
            return IndexAccess.load(data)
        else:
            raise ValueError("unknown access type")

class MemberAccess(Access):
    type = Access.Type.Member

    def __init__(self, *, identifier: Identifier):
        self.identifier = identifier

    def dump(self) -> bytes:
        return self.type.dump() + self.identifier.dump()

    @classmethod
    def load(cls, data: BytesIO):
        identifier = Identifier.load(data)
        return cls(identifier=identifier)


class IndexAccess(Access):
    type = Access.Type.Index

    def __init__(self, *, index: u32):
        self.index = index

    def dump(self) -> bytes:
        return self.type.dump() + self.index.dump()

    @classmethod
    def load(cls, data: BytesIO):
        index = u32.load(data)
        return cls(index=index)


class AccessRegister(Register):
    type = Register.Type.Access

    def __init__(self, *, locator: VarInt, accesses: Vec[Access, u16]):
        self.locator = locator
        self.accesses = accesses

    def dump(self) -> bytes:
        return self.type.dump() + self.locator.dump() + self.accesses.dump()

    @classmethod
    def load(cls, data: BytesIO):
        locator = VarInt.load(data)
        accesses = Vec[Access, u16].load(data)
        return cls(locator=locator, accesses=accesses)


class Operand(EnumBaseSerialize, Serialize, RustEnum):

    class Type(IntEnumu8):
        Literal = 0
        Register = 1
        ProgramID = 2
        Signer = 3
        Caller = 4
        BlockHeight = 5

    @classmethod
    def load(cls, data: BytesIO):
        type_ = cls.Type.load(data)
        if type_ == cls.Type.Literal:
            return LiteralOperand.load(data)
        elif type_ == cls.Type.Register:
            return RegisterOperand.load(data)
        elif type_ == cls.Type.ProgramID:
            return ProgramIDOperand.load(data)
        elif type_ == cls.Type.Signer:
            return SignerOperand.load(data)
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


class SignerOperand(Operand):
    type = Operand.Type.Signer

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



N = TypeVar("N", bound=FixedSize)

@access_generic_type
class Literals(Serializable, Generic[N]):
    types: tuple[N]

    def __init__(self, *, operands: list[Operand], destination: Register):
        self.num_operands = self.types[0]
        if len(operands) != self.num_operands:
            raise ValueError("incorrect number of operands")
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
        return cls(operands=operands, destination=destination)


class Variant(int):
    def __class_getitem__(cls, item: int):
        return cls(item)

V = TypeVar("V", bound=Variant)

@access_generic_type
class AssertInstruction(Serializable, Generic[V]):
    types: tuple[V]
    variant: V

    def __init__(self, *, operands: tuple[Operand, Operand]):
        self.variant = self.types[0]
        self.operands = operands

    def dump(self) -> bytes:
        return b"".join(operand.dump() for operand in self.operands)

    @classmethod
    def load(cls, data: BytesIO, *, types: Optional[tuple[N]] = None):
        if types is None:
            raise ValueError("expected types")
        op1 = Operand.load(data)
        op2 = Operand.load(data)
        return cls(operands=(op1, op2))


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
    Signature = 15
    String = 16

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
            self.Signature: Signature,
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
            self.Signature: "signature",
            self.String: "string",
        }[self]


class PlaintextType(EnumBaseSerialize, Serialize, RustEnum):

    class Type(IntEnumu8):
        Literal = 0
        Struct = 1
        Array = 2

    type: Type

    @classmethod
    def load(cls, data: BytesIO):
        type_ = cls.Type.load(data)
        if type_ == cls.Type.Literal:
            return LiteralPlaintextType.load(data)
        if type_ == cls.Type.Struct:
            return StructPlaintextType.load(data)
        if type_ == cls.Type.Array:
            return ArrayPlaintextType.load(data)
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

class ArrayType(Serializable):

    def __init__(self, *, element_type: PlaintextType, length: u32):
        self.element_type = element_type
        self.length = length

    def dump(self) -> bytes:
        res = b""
        if isinstance(self.element_type, (LiteralPlaintextType, StructPlaintextType)):
            res += self.element_type.dump()
        e = self.element_type
        lengths: list[u32] = []
        for _ in range(32):
            if isinstance(e, ArrayPlaintextType):
                lengths.append(e.array_type.length)
                e = e.array_type.element_type
            else:
                break
        res += Vec[u32, u8](lengths).dump()
        return res

    @classmethod
    def load(cls, data: BytesIO):
        plaintext_type = PlaintextType.load(data)
        if isinstance(plaintext_type, ArrayPlaintextType):
            raise ValueError("invalid data")
        lengths = Vec[u32, u8].load(data)
        if not 0 < len(lengths) <= 32:
            raise ValueError("invalid data")
        lengths = reversed(list(lengths))
        array = ArrayType(
            element_type=plaintext_type,
            length=next(lengths),
        )
        while lengths:
            array = ArrayType(
                element_type=ArrayPlaintextType(array_type=array),
                length=next(lengths),
            )
        return array

class ArrayPlaintextType(PlaintextType):
    type = PlaintextType.Type.Array

    def __init__(self, *, array_type: ArrayType):
        self.array_type = array_type

    def dump(self) -> bytes:
        return self.type.dump() + self.array_type.dump()

    @classmethod
    def load(cls, data: BytesIO):
        array_type = ArrayType.load(data)
        return cls(array_type=array_type)


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
        Plaintext = 2
        Record = 3
        ExternalRecord = 4

    @classmethod
    def load(cls, data: BytesIO):
        type_ = cls.Type.load(data)
        if type_ == cls.Type.GroupXCoordinate:
            return GroupXCoordinateCastType.load(data)
        elif type_ == cls.Type.GroupYCoordinate:
            return GroupYCoordinateCastType.load(data)
        elif type_ == cls.Type.Plaintext:
            return PlaintextCastType.load(data)
        elif type_ == cls.Type.Record:
            return RecordCastType.load(data)
        elif type_ == cls.Type.ExternalRecord:
            return ExternalRecordCastType.load(data)
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

class PlaintextCastType(CastType):
    type = CastType.Type.Plaintext

    def __init__(self, *, plaintext_type: PlaintextType):
        self.plaintext_type = plaintext_type

    def dump(self) -> bytes:
        return self.type.dump() + self.plaintext_type.dump()

    @classmethod
    def load(cls, data: BytesIO):
        plaintext_type = PlaintextType.load(data)
        return cls(plaintext_type=plaintext_type)

class RecordCastType(CastType):
    type = CastType.Type.Record

    def __init__(self, *, identifier: Identifier):
        self.identifier = identifier

    def dump(self) -> bytes:
        return self.type.dump() + self.identifier.dump()

    @classmethod
    def load(cls, data: BytesIO):
        identifier = Identifier.load(data)
        return cls(identifier=identifier)

class ExternalRecordCastType(CastType):
    type = CastType.Type.ExternalRecord

    def __init__(self, *, locator: Locator):
        self.locator = locator

    def dump(self) -> bytes:
        return self.type.dump() + self.locator.dump()

    @classmethod
    def load(cls, data: BytesIO):
        locator = Locator.load(data)
        return cls(locator=locator)


@access_generic_type
class CastInstruction(Serializable, Generic[V]):
    types: tuple[V]

    class Type(IntEnum):
        Cast = 0
        CastLossy = 1

    def __init__(self, *, operands: Vec[Operand, u8], destination: Register, cast_type: CastType):
        self.type = self.types[0]
        self.operands = operands
        self.destination = destination
        self.cast_type = cast_type

    def dump(self) -> bytes:
        return self.operands.dump() + self.destination.dump() + self.cast_type.dump()

    @classmethod
    def load(cls, data: BytesIO, *, types: Optional[tuple[V]] = None):
        if types is None:
            raise ValueError("expected types")
        operands = Vec[Operand, u8].load(data)
        destination = Register.load(data)
        cast_Type = CastType.load(data)
        return cls(operands=operands, destination=destination, cast_type=cast_Type)


class EnumTypeValue(EnumType):
    def __class_getitem__(cls, item: EnumType):
        return item

@access_generic_type
class CommitInstruction(Serializable, Generic[V]):
    types: tuple[V]

    class Type(IntEnum):
        CommitBHP256 = 0
        CommitBHP512 = 1
        CommitBHP768 = 2
        CommitBHP1024 = 3
        CommitPED64 = 4
        CommitPED128 = 5

    def __init__(self, *, operands: tuple[Operand, Operand], destination: Register, destination_type: LiteralType):
        self.type = self.types[0]
        self.operands = operands
        self.destination = destination
        self.destination_type = destination_type

    def dump(self) -> bytes:
        return b"".join(o.dump() for o in self.operands) + self.destination.dump() + self.destination_type.dump()

    @classmethod
    def load(cls, data: BytesIO, *, types: Optional[tuple[V]] = None):
        if not types:
            raise ValueError("expected types")
        op1 = Operand.load(data)
        op2 = Operand.load(data)
        destination = Register.load(data)
        destination_type = LiteralType.load(data)
        return cls(operands=(op1, op2), destination=destination, destination_type=destination_type)


@access_generic_type
class HashInstruction(Serializable, Generic[V]):
    types: tuple[V]

    class Type(IntEnum):

        @staticmethod
        def _generate_next_value_(name: str, start: int, count: int, last_values: list[int]):
            return count

        HashBHP256 = auto()
        HashBHP512 = auto()
        HashBHP768 = auto()
        HashBHP1024 = auto()
        HashKeccak256 = auto()
        HashKeccak384 = auto()
        HashKeccak512 = auto()
        HashPED64 = auto()
        HashPED128 = auto()
        HashPSD2 = auto()
        HashPSD4 = auto()
        HashPSD8 = auto()
        HashSha3_256 = auto()
        HashSha3_384 = auto()
        HashSha3_512 = auto()
        HashManyPSD2 = auto()
        HashManyPSD4 = auto()
        HashManyPSD8 = auto()

    # shortcut here so check doesn't work
    def __init__(self, *, operands: tuple[Operand, Optional[Operand]], destination: Register, destination_type: PlaintextType):
        self.type = self.types[0]
        self.operands = operands
        self.destination = destination
        self.destination_type = destination_type

    @classmethod
    def num_operands(cls, type_: Type, **_: Any) -> int:
        if type_ in [cls.Type.HashManyPSD2, cls.Type.HashManyPSD4, cls.Type.HashManyPSD8]:
            return 2
        return 1

    def dump(self) -> bytes:
        return b"".join(op.dump() for op in self.operands if op) + self.destination.dump() + self.destination_type.dump()

    @classmethod
    def load(cls, data: BytesIO, *, types: Optional[tuple[V]] = None):
        if types is None:
            raise ValueError("expected types")
        size = cls.num_operands(cls.Type(types[0]))
        op1 = Operand.load(data)
        if size == 2:
            op2 = Operand.load(data)
        else:
            op2 = None
        destination = Register.load(data)
        destination_type = PlaintextType.load(data)
        return cls(operands=(op1, op2), destination=destination, destination_type=destination_type)

class AsyncInstruction(Serializable):

    def __init__(self, *, function_name: Identifier, operands: Vec[Operand, u8], destination: Register):
        self.function_name = function_name
        self.operands = operands
        self.destination = destination

    def dump(self) -> bytes:
        return self.function_name.dump() + self.operands.dump() + self.destination.dump()

    @classmethod
    def load(cls, data: BytesIO):
        function_name = Identifier.load(data)
        operands = Vec[Operand, u8].load(data)
        destination = Register.load(data)
        return cls(function_name=function_name, operands=operands, destination=destination)

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
        Async = auto()
        Call = auto()
        Cast = auto()
        CastLossy = auto()
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
        HashKeccak256 = auto()
        HashKeccak384 = auto()
        HashKeccak512 = auto()
        HashPED64 = auto()
        HashPED128 = auto()
        HashPSD2 = auto()
        HashPSD4 = auto()
        HashPSD8 = auto()
        HashSha3_256 = auto()
        HashSha3_384 = auto()
        HashSha3_512 = auto()
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
        SignVerify = auto()
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
        Type.Abs: Literals[FixedSize[1]],
        Type.AbsWrapped: Literals[FixedSize[1]],
        Type.Add: Literals[FixedSize[2]],
        Type.AddWrapped: Literals[FixedSize[2]],
        Type.And: Literals[FixedSize[2]],
        Type.AssertEq: AssertInstruction[Variant[0]],
        Type.AssertNeq: AssertInstruction[Variant[1]],
        Type.Async: AsyncInstruction,
        Type.Call: CallInstruction,
        Type.Cast: CastInstruction[Variant[CastInstruction.Type.Cast]],
        Type.CastLossy: CastInstruction[Variant[CastInstruction.Type.CastLossy]],
        Type.CommitBHP256: CommitInstruction[Variant[CommitInstruction.Type.CommitBHP256]],
        Type.CommitBHP512: CommitInstruction[Variant[CommitInstruction.Type.CommitBHP512]],
        Type.CommitBHP768: CommitInstruction[Variant[CommitInstruction.Type.CommitBHP768]],
        Type.CommitBHP1024: CommitInstruction[Variant[CommitInstruction.Type.CommitBHP1024]],
        Type.CommitPED64: CommitInstruction[Variant[CommitInstruction.Type.CommitPED64]],
        Type.CommitPED128: CommitInstruction[Variant[CommitInstruction.Type.CommitPED128]],
        Type.Div: Literals[FixedSize[2]],
        Type.DivWrapped: Literals[FixedSize[2]],
        Type.Double: Literals[FixedSize[1]],
        Type.GreaterThan: Literals[FixedSize[2]],
        Type.GreaterThanOrEqual: Literals[FixedSize[2]],
        Type.HashBHP256: HashInstruction[Variant[HashInstruction.Type.HashBHP256]],
        Type.HashBHP512: HashInstruction[Variant[HashInstruction.Type.HashBHP512]],
        Type.HashBHP768: HashInstruction[Variant[HashInstruction.Type.HashBHP768]],
        Type.HashBHP1024: HashInstruction[Variant[HashInstruction.Type.HashBHP1024]],
        Type.HashKeccak256: HashInstruction[Variant[HashInstruction.Type.HashKeccak256]],
        Type.HashKeccak384: HashInstruction[Variant[HashInstruction.Type.HashKeccak384]],
        Type.HashKeccak512: HashInstruction[Variant[HashInstruction.Type.HashKeccak512]],
        Type.HashPED64: HashInstruction[Variant[HashInstruction.Type.HashPED64]],
        Type.HashPED128: HashInstruction[Variant[HashInstruction.Type.HashPED128]],
        Type.HashPSD2: HashInstruction[Variant[HashInstruction.Type.HashPSD2]],
        Type.HashPSD4: HashInstruction[Variant[HashInstruction.Type.HashPSD4]],
        Type.HashPSD8: HashInstruction[Variant[HashInstruction.Type.HashPSD8]],
        Type.HashSha3_256: HashInstruction[Variant[HashInstruction.Type.HashSha3_256]],
        Type.HashSha3_384: HashInstruction[Variant[HashInstruction.Type.HashSha3_384]],
        Type.HashSha3_512: HashInstruction[Variant[HashInstruction.Type.HashSha3_512]],
        Type.HashManyPSD2: HashInstruction[Variant[HashInstruction.Type.HashManyPSD2]],
        Type.HashManyPSD4: HashInstruction[Variant[HashInstruction.Type.HashManyPSD4]],
        Type.HashManyPSD8: HashInstruction[Variant[HashInstruction.Type.HashManyPSD8]],
        Type.Inv: Literals[FixedSize[1]],
        Type.IsEq: Literals[FixedSize[2]],
        Type.IsNeq: Literals[FixedSize[2]],
        Type.LessThan: Literals[FixedSize[2]],
        Type.LessThanOrEqual: Literals[FixedSize[2]],
        Type.Modulo: Literals[FixedSize[2]],
        Type.Mul: Literals[FixedSize[2]],
        Type.MulWrapped: Literals[FixedSize[2]],
        Type.Nand: Literals[FixedSize[2]],
        Type.Neg: Literals[FixedSize[1]],
        Type.Nor: Literals[FixedSize[2]],
        Type.Not: Literals[FixedSize[1]],
        Type.Or: Literals[FixedSize[2]],
        Type.Pow: Literals[FixedSize[2]],
        Type.PowWrapped: Literals[FixedSize[2]],
        Type.Rem: Literals[FixedSize[2]],
        Type.RemWrapped: Literals[FixedSize[2]],
        Type.Shl: Literals[FixedSize[2]],
        Type.ShlWrapped: Literals[FixedSize[2]],
        Type.Shr: Literals[FixedSize[2]],
        Type.ShrWrapped: Literals[FixedSize[2]],
        Type.SignVerify: Literals[FixedSize[3]],
        Type.Square: Literals[FixedSize[1]],
        Type.SquareRoot: Literals[FixedSize[1]],
        Type.Sub: Literals[FixedSize[2]],
        Type.SubWrapped: Literals[FixedSize[2]],
        Type.Ternary: Literals[FixedSize[3]],
        Type.Xor: Literals[FixedSize[2]],
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
        Type.CastLossy: "X",
        Type.CommitBHP256: "M",
        Type.CommitBHP512: "M",
        Type.CommitBHP768: "M",
        Type.CommitBHP1024: "M",
        Type.CommitPED64: "M",
        Type.CommitPED128: "M",
        Type.Async: "F",
        Type.Div: "B",
        Type.DivWrapped: "B",
        Type.Double: "U",
        Type.GreaterThan: "P",
        Type.GreaterThanOrEqual: "P",
        Type.HashBHP256: "H",
        Type.HashBHP512: "H",
        Type.HashBHP768: "H",
        Type.HashBHP1024: "H",
        Type.HashKeccak256: "H",
        Type.HashKeccak384: "H",
        Type.HashKeccak512: "H",
        Type.HashPED64: "H",
        Type.HashPED128: "H",
        Type.HashPSD2: "H",
        Type.HashPSD4: "H",
        Type.HashPSD8: "H",
        Type.HashSha3_256: "H",
        Type.HashSha3_384: "H",
        Type.HashSha3_512: "H",
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
        Type.SignVerify: "S",
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
        Type.Async: -1,
        Type.Call: -1,
        Type.Cast: 2_000,
        Type.CastLossy: 2_000,
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
        Type.HashBHP256: 100_000,
        Type.HashBHP512: 100_000,
        Type.HashBHP768: 100_000,
        Type.HashBHP1024: 100_000,
        Type.HashKeccak256: 100_000,
        Type.HashKeccak384: 100_000,
        Type.HashKeccak512: 100_000,
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
        Type.HashSha3_256: 100_000,
        Type.HashSha3_384: 100_000,
        Type.HashSha3_512: 100_000,
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
        Type.SignVerify: 250_000,
        Type.Square: 2_000,
        Type.SquareRoot: 120_000,
        Type.Sub: 10_000,
        Type.SubWrapped: 2_000,
        Type.Ternary: 2_000,
        Type.Xor: 2_000,
    }

    def __init__(self, *, type_: Type, literals: Literals[Any] | AssertInstruction[Any] | CallInstruction | CastInstruction[Any] | CommitInstruction[Any] | HashInstruction[Any] | AsyncInstruction):
        self.type = type_
        self.literals = literals

    def dump(self) -> bytes:
        return self.type.dump() + self.literals.dump()

    @classmethod
    def load(cls, data: BytesIO):
        type_ = cls.Type.load(data)
        instruction_type = cls.type_map[type_]
        literals = instruction_type.load(data)
        return cls(type_=type_, literals=literals)

    @property
    def cost(self) -> int:
        if self.type in (self.Type.HashPSD2, self.Type.HashPSD4, self.Type.HashPSD8):
            if not isinstance(self.literals, HashInstruction):
                raise ValueError(f"expected HashInstruction, got {self}")
            inst = self.literals
            # need to redesign the fee map to go fully static
            fee_dict: dict[str, int] = Instruction.fee_map[self.type] # type: ignore[reportGeneralTypeIssues]
            if not isinstance(inst.destination_type, LiteralPlaintextType):
                raise ValueError(f"expected LiteralPlaintextType, got {inst.destination_type}")
            literal_type = inst.destination_type.literal_type
            if literal_type in (LiteralType.Address, LiteralType.Group):
                return fee_dict["high"]
            else:
                return fee_dict["low"]
        cost = Instruction.fee_map[self.type]
        if cost == -1:
            raise ValueError(f"instruction {self.type} is not supported in finalize")
        return cost # type: ignore[reportGeneralTypeIssues]
