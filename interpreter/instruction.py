from interpreter.environment import Registers
from interpreter.utils import load_plaintext_from_operand, store_plaintext_to_register, FinalizeState
from node.types import *

def execute_instruction(instruction: Instruction, program: Program, registers: Registers, finalize_state: FinalizeState):
    literals = instruction.literals
    if isinstance(literals, Literals):
        num_operands = int(literals.num_operands)
        operands = literals.operands[:num_operands]
        destination = literals.destination
        instruction_ops[instruction.type.value](operands, destination, registers, finalize_state)
    elif isinstance(literals, CastInstruction):
        operands = literals.operands
        destination = literals.destination
        cast_type = literals.cast_type
        CastOp(operands, destination, cast_type, program, registers, finalize_state)
    elif isinstance(literals, CallInstruction):
        raise NotImplementedError
    elif isinstance(literals, AssertInstruction):
        variant = literals.variant
        if variant == 0:
            AssertEq(literals.operands, registers, finalize_state)
        elif variant == 1:
            AssertNeq(literals.operands, registers, finalize_state)
    elif isinstance(literals, HashInstruction):
        type_ = literals.type
        if type_ == HashInstruction.Type.HashBHP256:
            HashBHP256(literals.operands, literals.destination, literals.destination_type, registers, finalize_state)
        else:
            raise NotImplementedError
    else:
        raise NotImplementedError

def Abs(operands: [Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def AbsWrapped(operands: [Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def Add(operands: [Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    allowed_types = [
        Field,
        Group,
        i8,
        i16,
        i32,
        i64,
        i128,
        u8,
        u16,
        u32,
        u64,
        u128,
        Scalar,
    ]
    op1 = load_plaintext_from_operand(operands[0], registers, finalize_state)
    op2 = load_plaintext_from_operand(operands[1], registers, finalize_state)
    if not (isinstance(op1, LiteralPlaintext) and isinstance(op2, LiteralPlaintext)):
        raise TypeError("operands must be literals")
    op1_type = Literal.primitive_type_map[op1.literal.type]
    op2_type = Literal.primitive_type_map[op2.literal.type]
    if not (op1_type in allowed_types and op2_type == op1_type):
        raise TypeError("invalid operand types")
    res = LiteralPlaintext(
        literal=Literal(
            type_=Literal.reverse_primitive_type_map[op1_type],
            primitive=op1.literal.primitive + op2.literal.primitive
        )
    )
    store_plaintext_to_register(res, destination, registers)

def AddWrapped(operands: [Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    op1 = load_plaintext_from_operand(operands[0], registers, finalize_state)
    op2 = load_plaintext_from_operand(operands[1], registers, finalize_state)
    res = LiteralPlaintext(
        literal=Literal(
            type_=op1.literal.type,
            primitive=op1.literal.primitive.add_wrapped(op2.literal.primitive),
        )
    )
    store_plaintext_to_register(res, destination, registers)

def And(operands: [Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    allowed_types = [bool_, i8, i16, i32, i64, i128, u8, u16, u32, u64, u128]
    op1 = load_plaintext_from_operand(operands[0], registers, finalize_state)
    op2 = load_plaintext_from_operand(operands[1], registers, finalize_state)
    if not (isinstance(op1, LiteralPlaintext) and isinstance(op2, LiteralPlaintext)):
        raise TypeError("operands must be literals")
    op1_type = Literal.primitive_type_map[op1.literal.type]
    op2_type = Literal.primitive_type_map[op2.literal.type]
    if not (op1_type in allowed_types and op2_type == op1_type):
        raise TypeError("invalid operand types")
    res = LiteralPlaintext(
        literal=Literal(
            type_=Literal.reverse_primitive_type_map[op1_type],
            primitive=op1.literal.primitive & op2.literal.primitive
        )
    )
    store_plaintext_to_register(res, destination, registers)

def AssertEq(operands: [Operand], registers: Registers, finalize_state: FinalizeState):
    if len(operands) != 2:
        raise RuntimeError("invalid number of operands")
    op1 = load_plaintext_from_operand(operands[0], registers, finalize_state)
    op2 = load_plaintext_from_operand(operands[1], registers, finalize_state)
    if op1 != op2:
        raise AssertionError("assertion failed: {} != {}".format(op1, op2))

def AssertNeq(operands: [Operand], registers: Registers, finalize_state: FinalizeState):
    if len(operands) != 2:
        raise RuntimeError("invalid number of operands")
    op1 = load_plaintext_from_operand(operands[0], registers, finalize_state)
    op2 = load_plaintext_from_operand(operands[1], registers, finalize_state)
    if op1 == op2:
        raise AssertionError("assertion failed: {} == {}".format(op1, op2))

def CallOp(operands: [Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def CastOp(operands: [Operand], destination: Register, cast_type: CastType, program: Program, registers: Registers, finalize_state: FinalizeState):

    def verify_struct_type(struct_plaintext: StructPlaintext, verify_struct_definition: Struct):
        if len(struct_plaintext.members) != len(verify_struct_definition.members):
            raise RuntimeError("invalid number of members")
        for i, (identifier, member_type) in enumerate(verify_struct_definition.members):
            identifier: Identifier
            member_type: PlaintextType
            member_name, member_value = struct_plaintext.members[i]
            member_name: Identifier
            member_value: Plaintext
            if member_name != identifier:
                raise RuntimeError("invalid member name")
            if member_value.type.value != member_type.type.value:
                raise RuntimeError("invalid member type")
            if member_value.type == Plaintext.Type.Literal:
                member_value: LiteralPlaintext
                member_type: LiteralPlaintextType
                if member_value.literal.type.value != member_type.literal_type.value:
                    raise RuntimeError("invalid member type")
            elif member_value.type == Plaintext.Type.Struct:
                member_value: StructPlaintext
                member_type: StructPlaintextType
                sub_struct_definition = program.structs[member_type.struct]
                verify_struct_type(member_value, sub_struct_definition)
    if not isinstance(cast_type, RegisterTypeCastType):
        raise NotImplementedError
    register_type: RegisterType = cast_type.register_type
    if register_type == RegisterType.Type.Plaintext:
        raise RuntimeError("invalid register type")
    register_type: PlaintextRegisterType
    plaintext_type: PlaintextType = register_type.plaintext_type
    if plaintext_type.type != PlaintextType.Type.Struct:
        plaintext_type: LiteralPlaintextType
        if not plaintext_type.literal_type in [LiteralType.Address, LiteralType.Field, LiteralType.Group]:
            raise AssertionError("snarkOS doesn't support casting from this type yet")
        raise NotImplementedError
    plaintext_type: StructPlaintextType
    struct_identifier = plaintext_type.struct
    struct_definition = program.structs[struct_identifier]
    if len(struct_definition.members) != len(operands):
        raise RuntimeError("invalid number of operands")
    members = []
    for i, (name, _) in enumerate(struct_definition.members):
        name: Identifier
        members.append(Tuple[Identifier, Plaintext]((name, load_plaintext_from_operand(operands[i], registers, finalize_state))))
    struct_plaintext = StructPlaintext(members=Vec[Tuple[Identifier, Plaintext], u8](members))
    verify_struct_type(struct_plaintext, struct_definition)
    store_plaintext_to_register(struct_plaintext, destination, registers)

def CommitBHP256(operands: [Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def CommitBHP512(operands: [Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def CommitBHP768(operands: [Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def CommitBHP1024(operands: [Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def CommitPED64(operands: [Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def CommitPED128(operands: [Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def Div(operands: [Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    op1 = load_plaintext_from_operand(operands[0], registers, finalize_state)
    op2 = load_plaintext_from_operand(operands[1], registers, finalize_state)
    if not (isinstance(op1, LiteralPlaintext) and isinstance(op2, LiteralPlaintext)):
        raise TypeError("operands must be literals")
    if op1.literal.type != op2.literal.type:
        raise TypeError("invalid operand types")
    res = LiteralPlaintext(
        literal=Literal(
            type_=op1.literal.type,
            primitive=op1.literal.primitive // op2.literal.primitive,
        )
    )
    store_plaintext_to_register(res, destination, registers)

def DivWrapped(operands: [Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    op1 = load_plaintext_from_operand(operands[0], registers, finalize_state)
    op2 = load_plaintext_from_operand(operands[1], registers, finalize_state)
    res = LiteralPlaintext(
        literal=Literal(
            type_=op1.literal.type,
            primitive=op1.literal.primitive.div_wrapped(op2.literal.primitive),
        )
    )
    store_plaintext_to_register(res, destination, registers)

def Double(operands: [Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def GreaterThan(operands: [Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    op1 = load_plaintext_from_operand(operands[0], registers, finalize_state)
    op2 = load_plaintext_from_operand(operands[1], registers, finalize_state)
    if op1 > op2:
        res = LiteralPlaintext(
            literal=Literal(
                type_=Literal.Type.Boolean,
                primitive=bool_(True),
            )
        )
    else:
        res = LiteralPlaintext(
            literal=Literal(
                type_=Literal.Type.Boolean,
                primitive=bool_(False),
            )
        )
    store_plaintext_to_register(res, destination, registers)

def GreaterThanOrEqual(operands: [Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    op1 = load_plaintext_from_operand(operands[0], registers, finalize_state)
    op2 = load_plaintext_from_operand(operands[1], registers, finalize_state)
    if op1 >= op2:
        res = LiteralPlaintext(
            literal=Literal(
                type_=Literal.Type.Boolean,
                primitive=bool_(True),
            )
        )
    else:
        res = LiteralPlaintext(
            literal=Literal(
                type_=Literal.Type.Boolean,
                primitive=bool_(False),
            )
        )
    store_plaintext_to_register(res, destination, registers)

def HashBHP256(operands: [Operand], destination: Register, destination_type: LiteralType, registers: Registers, finalize_state: FinalizeState):
    op = load_plaintext_from_operand(operands[0], registers, finalize_state)
    value_type = destination_type.primitive_type
    value = value_type.load(BytesIO(bytes(aleo.hash_ops(PlaintextValue(plaintext=op).dump(), "bhp256", destination_type.dump()))))
    res = LiteralPlaintext(
        literal=Literal(
            type_=Literal.Type(destination_type.value),
            primitive=value,
        )
    )
    store_plaintext_to_register(res, destination, registers)

def HashBHP512(operands: [Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def HashBHP768(operands: [Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def HashBHP1024(operands: [Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def HashPED64(operands: [Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def HashPED128(operands: [Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def HashPSD2(operands: [Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def HashPSD4(operands: [Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def HashPSD8(operands: [Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def HashManyPSD2(operands: [Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def HashManyPSD4(operands: [Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def HashManyPSD8(operands: [Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def Inv(operands: [Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def IsEq(operands: [Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    allowed_types = [
        Address,
        bool_,
        Field,
        Group,
        i8,
        i16,
        i32,
        i64,
        i128,
        u8,
        u16,
        u32,
        u64,
        u128,
        Scalar,
        Struct,
    ]
    op1 = load_plaintext_from_operand(operands[0], registers, finalize_state)
    op2 = load_plaintext_from_operand(operands[1], registers, finalize_state)
    # loosely check the types, we don't really expect to run into bad types here
    if op1.type != op2.type:
        raise TypeError("invalid operand types")
    res = LiteralPlaintext(
        literal=Literal(
            type_=Literal.Type.Boolean,
            primitive=bool_(op1 == op2),
        )
    )
    store_plaintext_to_register(res, destination, registers)


def IsNeq(operands: [Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    allowed_types = [
        Address,
        bool_,
        Field,
        Group,
        i8,
        i16,
        i32,
        i64,
        i128,
        u8,
        u16,
        u32,
        u64,
        u128,
        Scalar,
        Struct,
    ]
    op1 = load_plaintext_from_operand(operands[0], registers, finalize_state)
    op2 = load_plaintext_from_operand(operands[1], registers, finalize_state)
    # loosely check the types, we don't really expect to run into bad types here
    if op1.type != op2.type:
        raise TypeError("invalid operand types")
    res = LiteralPlaintext(
        literal=Literal(
            type_=Literal.Type.Boolean,
            primitive=bool_(op1 != op2),
        )
    )
    store_plaintext_to_register(res, destination, registers)

def LessThan(operands: [Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    op1 = load_plaintext_from_operand(operands[0], registers, finalize_state)
    op2 = load_plaintext_from_operand(operands[1], registers, finalize_state)
    if op1 < op2:
        res = LiteralPlaintext(
            literal=Literal(
                type_=Literal.Type.Boolean,
                primitive=bool_(True),
            )
        )
    else:
        res = LiteralPlaintext(
            literal=Literal(
                type_=Literal.Type.Boolean,
                primitive=bool_(False),
            )
        )
    store_plaintext_to_register(res, destination, registers)

def LessThanOrEqual(operands: [Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    op1 = load_plaintext_from_operand(operands[0], registers, finalize_state)
    op2 = load_plaintext_from_operand(operands[1], registers, finalize_state)
    if op1 <= op2:
        res = LiteralPlaintext(
            literal=Literal(
                type_=Literal.Type.Boolean,
                primitive=bool_(True),
            )
        )
    else:
        res = LiteralPlaintext(
            literal=Literal(
                type_=Literal.Type.Boolean,
                primitive=bool_(False),
            )
        )
    store_plaintext_to_register(res, destination, registers)
def Modulo(operands: [Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def Mul(operands: [Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    op1 = load_plaintext_from_operand(operands[0], registers, finalize_state)
    op2 = load_plaintext_from_operand(operands[1], registers, finalize_state)
    if not (isinstance(op1, LiteralPlaintext) and isinstance(op2, LiteralPlaintext)):
        raise TypeError("operands must be literals")
    if op1.literal.type != op2.literal.type:
        raise TypeError("invalid operand types")
    res = LiteralPlaintext(
        literal=Literal(
            type_=op1.literal.type,
            primitive=op1.literal.primitive * op2.literal.primitive,
        )
    )
    store_plaintext_to_register(res, destination, registers)

def MulWrapped(operands: [Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    op1 = load_plaintext_from_operand(operands[0], registers, finalize_state)
    op2 = load_plaintext_from_operand(operands[1], registers, finalize_state)
    res = LiteralPlaintext(
        literal=Literal(
            type_=op1.literal.type,
            primitive=op1.literal.primitive.mul_wrapped(op2.literal.primitive),
        )
    )
    store_plaintext_to_register(res, destination, registers)

def Nand(operands: [Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def Neg(operands: [Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def Nor(operands: [Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def Not(operands: [Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    op = load_plaintext_from_operand(operands[0], registers, finalize_state)
    res = LiteralPlaintext(
        literal=Literal(
            type_=Literal.Type.Boolean,
            primitive=~op.literal.primitive,
        )
    )
    store_plaintext_to_register(res, destination, registers)

def Or(operands: [Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    op1 = load_plaintext_from_operand(operands[0], registers, finalize_state)
    op2 = load_plaintext_from_operand(operands[1], registers, finalize_state)
    op1_type = Literal.primitive_type_map[op1.literal.type]
    res = LiteralPlaintext(
        literal=Literal(
            type_=Literal.reverse_primitive_type_map[op1_type],
            primitive=op1.literal.primitive | op2.literal.primitive
        )
    )
    store_plaintext_to_register(res, destination, registers)

def Pow(operands: [Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def PowWrapped(operands: [Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def Rem(operands: [Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def RemWrapped(operands: [Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def Shl(operands: [Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    allowed_value_types = [i8, i16, i32, i64, i128, u8, u16, u32, u64, u128]
    allowed_magnitude_types = [u8, u16, u32]
    op1 = load_plaintext_from_operand(operands[0], registers, finalize_state)
    op2 = load_plaintext_from_operand(operands[1], registers, finalize_state)
    if not (isinstance(op1, LiteralPlaintext) and isinstance(op2, LiteralPlaintext)):
        raise TypeError("operands must be literals")
    op1_type = Literal.primitive_type_map[op1.literal.type]
    op2_type = Literal.primitive_type_map[op2.literal.type]
    if op1_type not in allowed_value_types:
        raise TypeError("invalid operand types")
    if op2_type not in allowed_magnitude_types:
        raise TypeError("invalid operand types")
    res = LiteralPlaintext(
        literal=Literal(
            type_=op1.literal.type,
            primitive=op1.literal.primitive << op2.literal.primitive,
        )
    )
    store_plaintext_to_register(res, destination, registers)

def ShlWrapped(operands: [Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def Shr(operands: [Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    allowed_value_types = [i8, i16, i32, i64, i128, u8, u16, u32, u64, u128]
    allowed_magnitude_types = [u8, u16, u32]
    op1 = load_plaintext_from_operand(operands[0], registers, finalize_state)
    op2 = load_plaintext_from_operand(operands[1], registers, finalize_state)
    if not (isinstance(op1, LiteralPlaintext) and isinstance(op2, LiteralPlaintext)):
        raise TypeError("operands must be literals")
    op1_type = Literal.primitive_type_map[op1.literal.type]
    op2_type = Literal.primitive_type_map[op2.literal.type]
    if op1_type not in allowed_value_types or op2_type not in allowed_magnitude_types:
        raise TypeError("invalid operand types")
    res = LiteralPlaintext(
        literal=Literal(
            type_=op1.literal.type,
            primitive=op1.literal.primitive >> op2.literal.primitive,
        )
    )
    store_plaintext_to_register(res, destination, registers)

def ShrWrapped(operands: [Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def Square(operands: [Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def SquareRoot(operands: [Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def Sub(operands: [Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    allowed_types = [
        Field,
        Group,
        i8,
        i16,
        i32,
        i64,
        i128,
        u8,
        u16,
        u32,
        u64,
        u128,
        Scalar,
    ]
    op1 = load_plaintext_from_operand(operands[0], registers, finalize_state)
    op2 = load_plaintext_from_operand(operands[1], registers, finalize_state)
    if not (isinstance(op1, LiteralPlaintext) and isinstance(op2, LiteralPlaintext)):
        raise TypeError("operands must be literals")
    op1_type = Literal.primitive_type_map[op1.literal.type]
    op2_type = Literal.primitive_type_map[op2.literal.type]
    if not (op1_type in allowed_types and op2_type == op1_type):
        raise TypeError("invalid operand types")
    res = LiteralPlaintext(
        literal=Literal(
            type_=Literal.reverse_primitive_type_map[op1_type],
            primitive=op1.literal.primitive - op2.literal.primitive
        )
    )
    store_plaintext_to_register(res, destination, registers)

def SubWrapped(operands: [Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    op1 = load_plaintext_from_operand(operands[0], registers, finalize_state)
    op2 = load_plaintext_from_operand(operands[1], registers, finalize_state)
    res = LiteralPlaintext(
        literal=Literal(
            type_=op1.literal.type,
            primitive=op1.literal.primitive.sub_wrapped(op2.literal.primitive),
        )
    )
    store_plaintext_to_register(res, destination, registers)

def Ternary(operands: [Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    op1 = load_plaintext_from_operand(operands[0], registers, finalize_state)
    op2 = load_plaintext_from_operand(operands[1], registers, finalize_state)
    op3 = load_plaintext_from_operand(operands[2], registers, finalize_state)
    if not isinstance(op1, LiteralPlaintext):
        raise TypeError("condition must be a literal")
    if op1.literal.type != Literal.Type.Boolean:
        raise TypeError("condition must be a boolean")
    if op1.literal.primitive == bool_(True):
        store_plaintext_to_register(op2, destination, registers)
    else:
        store_plaintext_to_register(op3, destination, registers)

def Xor(operands: [Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError



instruction_ops = {
    0: Abs,
    1: AbsWrapped,
    2: Add,
    3: AddWrapped,
    4: And,
    5: AssertEq,
    6: AssertNeq,
    7: CallOp,
    8: CastOp,
    9: CommitBHP256,
    10: CommitBHP512,
    11: CommitBHP768,
    12: CommitBHP1024,
    13: CommitPED64,
    14: CommitPED128,
    15: Div,
    16: DivWrapped,
    17: Double,
    18: GreaterThan,
    19: GreaterThanOrEqual,
    20: HashBHP256,
    21: HashBHP512,
    22: HashBHP768,
    23: HashBHP1024,
    24: HashPED64,
    25: HashPED128,
    26: HashPSD2,
    27: HashPSD4,
    28: HashPSD8,
    29: HashManyPSD2,
    30: HashManyPSD4,
    31: HashManyPSD8,
    32: Inv,
    33: IsEq,
    34: IsNeq,
    35: LessThan,
    36: LessThanOrEqual,
    37: Modulo,
    38: Mul,
    39: MulWrapped,
    40: Nand,
    41: Neg,
    42: Nor,
    43: Not,
    44: Or,
    45: Pow,
    46: PowWrapped,
    47: Rem,
    48: RemWrapped,
    49: Shl,
    50: ShlWrapped,
    51: Shr,
    52: ShrWrapped,
    53: Square,
    54: SquareRoot,
    55: Sub,
    56: SubWrapped,
    57: Ternary,
    58: Xor
}
