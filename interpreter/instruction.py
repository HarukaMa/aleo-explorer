from interpreter.environment import Registers
from interpreter.utils import load_plaintext_from_operand, store_plaintext_to_register
from node.types import *

def execute_instruction(instruction: Instruction, program: Program, registers: Registers):
    literals = instruction.literals
    if isinstance(literals, Literals):
        num_operands = int(literals.num_operands)
        operands = literals.operands[:num_operands]
        destination = literals.destination
        instruction_ops[instruction.type.value](operands, destination, registers)
    elif isinstance(literals, Cast):
        operands = literals.operands
        destination = literals.destination
        register_type = literals.register_type
        CastOp(operands, destination, register_type, program, registers)
    elif isinstance(literals, Call):
        raise NotImplementedError
    elif isinstance(literals, AssertInstruction):
        variant = literals.variant
        if variant == 0:
            AssertEq(literals.operands, registers)
        elif variant == 1:
            AssertNeq(literals.operands, registers)
    else:
        raise NotImplementedError

def Abs(operands: [Operand], destination: Register, registers: Registers):
    raise NotImplementedError

def AbsWrapped(operands: [Operand], destination: Register, registers: Registers):
    raise NotImplementedError

def Add(operands: [Operand], destination: Register, registers: Registers):
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
    op1 = load_plaintext_from_operand(operands[0], registers)
    op2 = load_plaintext_from_operand(operands[1], registers)
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

def AddWrapped(operands: [Operand], destination: Register, registers: Registers):
    raise NotImplementedError

def And(operands: [Operand], destination: Register, registers: Registers):
    raise NotImplementedError

def AssertEq(operands: [Operand], registers: Registers):
    if len(operands) != 2:
        raise RuntimeError("invalid number of operands")
    op1 = load_plaintext_from_operand(operands[0], registers)
    op2 = load_plaintext_from_operand(operands[1], registers)
    if op1 != op2:
        raise RuntimeError("assertion failed")

def AssertNeq(operands: [Operand], registers: Registers):
    if len(operands) != 2:
        raise RuntimeError("invalid number of operands")
    op1 = load_plaintext_from_operand(operands[0], registers)
    op2 = load_plaintext_from_operand(operands[1], registers)
    if op1 == op2:
        raise RuntimeError("assertion failed")

def CallOp(operands: [Operand], destination: Register, registers: Registers):
    raise NotImplementedError

def CastOp(operands: [Operand], destination: Register, register_type: RegisterType, program: Program, registers: Registers):

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
    if register_type == RegisterType.Type.Plaintext:
        raise RuntimeError("invalid register type")
    register_type: PlaintextRegisterType
    plaintext_type: PlaintextType = register_type.plaintext_type
    if plaintext_type.type != PlaintextType.Type.Struct:
        raise RuntimeError("invalid plaintext type")
    plaintext_type: StructPlaintextType
    struct_identifier = plaintext_type.struct
    struct_definition = program.structs[struct_identifier]
    if len(struct_definition.members) != len(operands):
        raise RuntimeError("invalid number of operands")
    members = []
    for i, (name, _) in enumerate(struct_definition.members):
        name: Identifier
        members.append(Tuple[Identifier, Plaintext]((name, load_plaintext_from_operand(operands[i], registers))))
    struct_plaintext = StructPlaintext(members=Vec[Tuple[Identifier, Plaintext], u8](members))
    verify_struct_type(struct_plaintext, struct_definition)
    store_plaintext_to_register(struct_plaintext, destination, registers)

def CommitBHP256(operands: [Operand], destination: Register, registers: Registers):
    raise NotImplementedError

def CommitBHP512(operands: [Operand], destination: Register, registers: Registers):
    raise NotImplementedError

def CommitBHP768(operands: [Operand], destination: Register, registers: Registers):
    raise NotImplementedError

def CommitBHP1024(operands: [Operand], destination: Register, registers: Registers):
    raise NotImplementedError

def CommitPED64(operands: [Operand], destination: Register, registers: Registers):
    raise NotImplementedError

def CommitPED128(operands: [Operand], destination: Register, registers: Registers):
    raise NotImplementedError

def Div(operands: [Operand], destination: Register, registers: Registers):
    raise NotImplementedError

def DivWrapped(operands: [Operand], destination: Register, registers: Registers):
    raise NotImplementedError

def Double(operands: [Operand], destination: Register, registers: Registers):
    raise NotImplementedError

def GreaterThan(operands: [Operand], destination: Register, registers: Registers):
    raise NotImplementedError

def GreaterThanOrEqual(operands: [Operand], destination: Register, registers: Registers):
    raise NotImplementedError

def HashBHP256(operands: [Operand], destination: Register, registers: Registers):
    op = load_plaintext_from_operand(operands[0], registers)
    res = LiteralPlaintext(
        literal=Literal(
            type_=Literal.Type.Field,
            primitive=Field.load(bytearray(aleo.hash_bhp256(PlaintextValue(plaintext=op).dump())))
        )
    )
    store_plaintext_to_register(res, destination, registers)

def HashBHP512(operands: [Operand], destination: Register, registers: Registers):
    raise NotImplementedError

def HashBHP768(operands: [Operand], destination: Register, registers: Registers):
    raise NotImplementedError

def HashBHP1024(operands: [Operand], destination: Register, registers: Registers):
    raise NotImplementedError

def HashPED64(operands: [Operand], destination: Register, registers: Registers):
    raise NotImplementedError

def HashPED128(operands: [Operand], destination: Register, registers: Registers):
    raise NotImplementedError

def HashPSD2(operands: [Operand], destination: Register, registers: Registers):
    raise NotImplementedError

def HashPSD4(operands: [Operand], destination: Register, registers: Registers):
    raise NotImplementedError

def HashPSD8(operands: [Operand], destination: Register, registers: Registers):
    raise NotImplementedError

def Inv(operands: [Operand], destination: Register, registers: Registers):
    raise NotImplementedError

def IsEq(operands: [Operand], destination: Register, registers: Registers):
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
    op1 = load_plaintext_from_operand(operands[0], registers)
    op2 = load_plaintext_from_operand(operands[1], registers)
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


def IsNeq(operands: [Operand], destination: Register, registers: Registers):
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
    op1 = load_plaintext_from_operand(operands[0], registers)
    op2 = load_plaintext_from_operand(operands[1], registers)
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

def LessThan(operands: [Operand], destination: Register, registers: Registers):
    raise NotImplementedError

def LessThanOrEqual(operands: [Operand], destination: Register, registers: Registers):
    raise NotImplementedError

def Modulo(operands: [Operand], destination: Register, registers: Registers):
    raise NotImplementedError

def Mul(operands: [Operand], destination: Register, registers: Registers):
    raise NotImplementedError

def MulWrapped(operands: [Operand], destination: Register, registers: Registers):
    raise NotImplementedError

def Nand(operands: [Operand], destination: Register, registers: Registers):
    raise NotImplementedError

def Neg(operands: [Operand], destination: Register, registers: Registers):
    raise NotImplementedError

def Nor(operands: [Operand], destination: Register, registers: Registers):
    raise NotImplementedError

def Not(operands: [Operand], destination: Register, registers: Registers):
    raise NotImplementedError

def Or(operands: [Operand], destination: Register, registers: Registers):
    raise NotImplementedError

def Pow(operands: [Operand], destination: Register, registers: Registers):
    raise NotImplementedError

def PowWrapped(operands: [Operand], destination: Register, registers: Registers):
    raise NotImplementedError

def Rem(operands: [Operand], destination: Register, registers: Registers):
    raise NotImplementedError

def RemWrapped(operands: [Operand], destination: Register, registers: Registers):
    raise NotImplementedError

def Shl(operands: [Operand], destination: Register, registers: Registers):
    raise NotImplementedError

def ShlWrapped(operands: [Operand], destination: Register, registers: Registers):
    raise NotImplementedError

def Shr(operands: [Operand], destination: Register, registers: Registers):
    raise NotImplementedError

def ShrWrapped(operands: [Operand], destination: Register, registers: Registers):
    raise NotImplementedError

def Square(operands: [Operand], destination: Register, registers: Registers):
    raise NotImplementedError

def SquareRoot(operands: [Operand], destination: Register, registers: Registers):
    raise NotImplementedError

def Sub(operands: [Operand], destination: Register, registers: Registers):
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
    op1 = load_plaintext_from_operand(operands[0], registers)
    op2 = load_plaintext_from_operand(operands[1], registers)
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

def SubWrapped(operands: [Operand], destination: Register, registers: Registers):
    raise NotImplementedError

def Ternary(operands: [Operand], destination: Register, registers: Registers):
    op1 = load_plaintext_from_operand(operands[0], registers)
    op2 = load_plaintext_from_operand(operands[1], registers)
    op3 = load_plaintext_from_operand(operands[2], registers)
    if not isinstance(op1, LiteralPlaintext):
        raise TypeError("condition must be a literal")
    if op1.literal.type != Literal.Type.Boolean:
        raise TypeError("condition must be a boolean")
    if op1.literal.primitive == bool_(True):
        store_plaintext_to_register(op2, destination, registers)
    else:
        store_plaintext_to_register(op3, destination, registers)

def Xor(operands: [Operand], destination: Register, registers: Registers):
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
    29: Inv,
    30: IsEq,
    31: IsNeq,
    32: LessThan,
    33: LessThanOrEqual,
    34: Modulo,
    35: Mul,
    36: MulWrapped,
    37: Nand,
    38: Neg,
    39: Nor,
    40: Not,
    41: Or,
    42: Pow,
    43: PowWrapped,
    44: Rem,
    45: RemWrapped,
    46: Shl,
    47: ShlWrapped,
    48: Shr,
    49: ShrWrapped,
    50: Square,
    51: SquareRoot,
    52: Sub,
    53: SubWrapped,
    54: Ternary,
    55: Xor
}
