from typing import cast

from aleo_types import *
from interpreter.environment import Registers
from interpreter.utils import load_plaintext_from_operand, store_plaintext_to_register, FinalizeState

IT = Instruction.Type
HT = HashInstruction.Type
CmT = CommitInstruction.Type
CsT = CastType.Type

def execute_instruction(instruction: Instruction, program: Program, registers: Registers, finalize_state: FinalizeState):
    literals = instruction.literals
    if isinstance(literals, Literals):
        num_operands = literals.num_operands
        operands = literals.operands[:num_operands]
        destination = literals.destination
        literal_ops[instruction.type](operands, destination, registers, finalize_state)
    elif isinstance(literals, CastInstruction):
        operands = literals.operands
        destination = literals.destination
        cast_type = literals.cast_type
        cast_op(operands, destination, cast_type, program, registers, finalize_state)
    elif isinstance(literals, CallInstruction):
        raise NotImplementedError
    elif isinstance(literals, AssertInstruction):
        variant = literals.variant
        if variant == 0:
            assert_eq(literals.operands, registers, finalize_state)
        elif variant == 1:
            assert_neq(literals.operands, registers, finalize_state)
        else:
            raise NotImplementedError
    elif isinstance(literals, HashInstruction):
        type_ = literals.type
        hash_op(literals.operands, literals.destination, literals.destination_type, registers, finalize_state, type_)
    elif isinstance(literals, CommitInstruction):
        type_ = literals.type
        commit_op(literals.operands, literals.destination, literals.destination_type, registers, finalize_state, type_)
    else:
        raise NotImplementedError


def abs_(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    op = load_plaintext_from_operand(operands[0], registers, finalize_state)
    if not isinstance(op, LiteralPlaintext):
        raise TypeError("operand must be literal")
    if not isinstance(op.literal.primitive, Abs):
        raise TypeError("operand must be absolute")
    # noinspection PyTypeChecker
    res = LiteralPlaintext(
        literal=Literal(
            type_=op.literal.type,
            primitive=abs(op.literal.primitive),
        )
    )
    store_plaintext_to_register(res, destination, registers)

def abs_wrapped(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    op = load_plaintext_from_operand(operands[0], registers, finalize_state)
    if not isinstance(op, LiteralPlaintext):
        raise TypeError("operand must be literal")
    if not isinstance(op.literal.primitive, AbsWrapped):
        raise TypeError("operand must be absolute")
    # noinspection PyTypeChecker
    res = LiteralPlaintext(
        literal=Literal(
            type_=op.literal.type,
            primitive=op.literal.primitive.abs_wrapped(),
        )
    )
    store_plaintext_to_register(res, destination, registers)

def add(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    op1 = load_plaintext_from_operand(operands[0], registers, finalize_state)
    op2 = load_plaintext_from_operand(operands[1], registers, finalize_state)
    if not (isinstance(op1, LiteralPlaintext) and isinstance(op2, LiteralPlaintext)):
        raise TypeError("operands must be literals")
    op1_primitive = op1.literal.primitive
    op2_primitive = op2.literal.primitive
    if not isinstance(op1_primitive, Add):
        raise TypeError("operands must be addable")
    # noinspection PyTypeChecker
    res = LiteralPlaintext(
        literal=Literal(
            type_=op1.literal.type,
            primitive=op1_primitive + op2_primitive,
        )
    )
    store_plaintext_to_register(res, destination, registers)

def add_wrapped(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    op1 = load_plaintext_from_operand(operands[0], registers, finalize_state)
    op2 = load_plaintext_from_operand(operands[1], registers, finalize_state)
    if not (isinstance(op1, LiteralPlaintext) and isinstance(op2, LiteralPlaintext)):
        raise TypeError("operands must be literals")
    if not isinstance(op1.literal.primitive, AddWrapped):
        raise TypeError("operands must be addable")
    # noinspection PyTypeChecker
    res = LiteralPlaintext(
        literal=Literal(
            type_=op1.literal.type,
            primitive=op1.literal.primitive.add_wrapped(op2.literal.primitive),
        )
    )
    store_plaintext_to_register(res, destination, registers)

def and_(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    op1 = load_plaintext_from_operand(operands[0], registers, finalize_state)
    op2 = load_plaintext_from_operand(operands[1], registers, finalize_state)
    if not (isinstance(op1, LiteralPlaintext) and isinstance(op2, LiteralPlaintext)):
        raise TypeError("operands must be literals")
    if not isinstance(op1.literal.primitive, And):
        raise TypeError("operands must be andable")
    # noinspection PyTypeChecker
    res = LiteralPlaintext(
        literal=Literal(
            type_=op1.literal.type,
            primitive=op1.literal.primitive & op2.literal.primitive,
        )
    )
    store_plaintext_to_register(res, destination, registers)

def assert_eq(operands: tuple[Operand, Operand], registers: Registers, finalize_state: FinalizeState):
    op1 = load_plaintext_from_operand(operands[0], registers, finalize_state)
    op2 = load_plaintext_from_operand(operands[1], registers, finalize_state)
    if op1 != op2:
        raise AssertionError("assertion failed: {} != {}".format(op1, op2))

def assert_neq(operands: tuple[Operand, Operand], registers: Registers, finalize_state: FinalizeState):
    op1 = load_plaintext_from_operand(operands[0], registers, finalize_state)
    op2 = load_plaintext_from_operand(operands[1], registers, finalize_state)
    if op1 == op2:
        raise AssertionError("assertion failed: {} == {}".format(op1, op2))

def call_op(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def cast_op(operands: list[Operand], destination: Register, cast_type: CastType, program: Program, registers: Registers, finalize_state: FinalizeState):

    def verify_struct_type(struct_plaintext: StructPlaintext, verify_struct_definition: Struct):
        if len(struct_plaintext.members) != len(verify_struct_definition.members):
            raise RuntimeError("invalid number of members")
        for i, (identifier, member_type) in enumerate(verify_struct_definition.members):
            member_name, member_value = struct_plaintext.members[i]
            if member_name != identifier:
                raise RuntimeError("invalid member name")
            if member_value.type.value != member_type.type.value:
                raise RuntimeError("invalid member type")
            if isinstance(member_value, LiteralPlaintext):
                member_type = cast(LiteralPlaintextType, member_type)
                # noinspection PyUnresolvedReferences
                if member_value.literal.type.value != member_type.literal_type.value:
                    raise RuntimeError("invalid member type")
            elif isinstance(member_value, StructPlaintext):
                member_type = cast(StructPlaintextType, member_type)
                sub_struct_definition = program.structs[member_type.struct]
                verify_struct_type(member_value, sub_struct_definition)

    if not isinstance(cast_type, PlaintextCastType):
        raise NotImplementedError
    plaintext_type = cast_type.plaintext_type
    if isinstance(plaintext_type, LiteralPlaintextType):
        plaintext = load_plaintext_from_operand(operands[0], registers, finalize_state)
        if not isinstance(plaintext, LiteralPlaintext):
            raise TypeError("operand must be a literal")
        if not plaintext.literal.type in [Literal.Type.Address, Literal.Type.Field, Literal.Type.Group]:
            raise AssertionError("snarkOS doesn't support casting from this type yet")
        primitive = plaintext.literal.primitive
        if not isinstance(primitive, Cast):
            raise TypeError("operand must be castable")
        literal_type = plaintext_type.literal_type
        res = LiteralPlaintext(
            literal=Literal(
                type_=Literal.Type(literal_type.value),
                primitive=primitive.cast(literal_type, lossy=False),
            )
        )
        store_plaintext_to_register(res, destination, registers)
    elif isinstance(plaintext_type, StructPlaintextType):
        struct_identifier = plaintext_type.struct
        struct_definition = program.structs[struct_identifier]
        if len(struct_definition.members) != len(operands):
            raise RuntimeError("invalid number of operands")
        members: list[Tuple[Identifier, Plaintext]] = []
        for i, (name, _) in enumerate(struct_definition.members):
            name: Identifier
            members.append(Tuple[Identifier, Plaintext]((name, load_plaintext_from_operand(operands[i], registers, finalize_state))))
        struct_plaintext = StructPlaintext(members=Vec[Tuple[Identifier, Plaintext], u8](members))
        verify_struct_type(struct_plaintext, struct_definition)
        store_plaintext_to_register(struct_plaintext, destination, registers)
    elif isinstance(plaintext_type, ArrayPlaintextType):
        array: list[Plaintext] = []
        for operand in operands:
            array.append(load_plaintext_from_operand(operand, registers, finalize_state))
        store_plaintext_to_register(ArrayPlaintext(elements=Vec[Plaintext, u32](array)), destination, registers)
    else:
        raise NotImplementedError

def commit_op(operands: tuple[Operand, Operand], destination: Register, destination_type: LiteralType, registers: Registers, finalize_state: FinalizeState, commit_type: CmT):
    op1 = load_plaintext_from_operand(operands[0], registers, finalize_state)
    op2 = load_plaintext_from_operand(operands[1], registers, finalize_state)
    if not isinstance(op2, LiteralPlaintext):
        raise TypeError("operand must be literal")
    if op2.literal.type != Literal.Type.Scalar:
        raise TypeError("invalid operand types")
    value_type = destination_type.primitive_type
    value = value_type.load(BytesIO(aleo_explorer_rust.commit_ops(PlaintextValue(plaintext=op1).dump(), op2.literal.primitive, commit_ops[commit_type], destination_type)))

    res = LiteralPlaintext(
        literal=Literal(
            type_=Literal.Type(destination_type.value),
            primitive=value,
        )
    )
    store_plaintext_to_register(res, destination, registers)

def div(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    op1 = load_plaintext_from_operand(operands[0], registers, finalize_state)
    op2 = load_plaintext_from_operand(operands[1], registers, finalize_state)
    if not (isinstance(op1, LiteralPlaintext) and isinstance(op2, LiteralPlaintext)):
        raise TypeError("operands must be literals")
    if not isinstance(op1.literal.primitive, Div):
        raise TypeError("operands must be divs")
    # noinspection PyTypeChecker
    res = LiteralPlaintext(
        literal=Literal(
            type_=op1.literal.type,
            primitive=op1.literal.primitive // op2.literal.primitive,
        )
    )
    store_plaintext_to_register(res, destination, registers)

def div_wrapped(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    op1 = load_plaintext_from_operand(operands[0], registers, finalize_state)
    op2 = load_plaintext_from_operand(operands[1], registers, finalize_state)
    if not (isinstance(op1, LiteralPlaintext) and isinstance(op2, LiteralPlaintext)):
        raise TypeError("operands must be literals")
    if not isinstance(op1.literal.primitive, DivWrapped):
        raise TypeError("operands must be dividable")
    # noinspection PyTypeChecker
    res = LiteralPlaintext(
        literal=Literal(
            type_=op1.literal.type,
            primitive=op1.literal.primitive.div_wrapped(op2.literal.primitive),
        )
    )
    store_plaintext_to_register(res, destination, registers)

def double(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    op = load_plaintext_from_operand(operands[0], registers, finalize_state)
    if not isinstance(op, LiteralPlaintext):
        raise TypeError("operand must be literal")
    if not isinstance(op.literal.primitive, Double):
        raise TypeError("operand must be doubleable")
    # noinspection PyTypeChecker
    res = LiteralPlaintext(
        literal=Literal(
            type_=op.literal.type,
            primitive=op.literal.primitive.double(),
        )
    )
    store_plaintext_to_register(res, destination, registers)

def greater_than(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    op1 = load_plaintext_from_operand(operands[0], registers, finalize_state)
    op2 = load_plaintext_from_operand(operands[1], registers, finalize_state)
    if not isinstance(op1, LiteralPlaintext) or not isinstance(op2, LiteralPlaintext):
        raise TypeError("operands must be literals")
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
                primitive=bool_(),
            )
        )
    store_plaintext_to_register(res, destination, registers)

def greater_than_or_equal(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    op1 = load_plaintext_from_operand(operands[0], registers, finalize_state)
    op2 = load_plaintext_from_operand(operands[1], registers, finalize_state)
    if not isinstance(op1, LiteralPlaintext) or not isinstance(op2, LiteralPlaintext):
        raise TypeError("operands must be literals")
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
                primitive=bool_(),
            )
        )
    store_plaintext_to_register(res, destination, registers)

def hash_op(operands: tuple[Operand, Optional[Operand]], destination: Register, destination_type: PlaintextType, registers: Registers, finalize_state: FinalizeState, hash_type: HT):
    op = load_plaintext_from_operand(operands[0], registers, finalize_state)
    if not isinstance(destination_type, LiteralPlaintextType):
        raise TypeError("destination type must be literal")
    value_type = destination_type.literal_type.primitive_type
    value = value_type.load(BytesIO(aleo_explorer_rust.hash_ops(PlaintextValue(plaintext=op).dump(), hash_ops[hash_type], destination_type.literal_type)))
    res = LiteralPlaintext(
        literal=Literal(
            type_=Literal.Type(destination_type.literal_type.value),
            primitive=value,
        )
    )
    store_plaintext_to_register(res, destination, registers)

def hash_many_psd2(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def hash_many_psd4(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def hash_many_psd8(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def inv(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    op = load_plaintext_from_operand(operands[0], registers, finalize_state)
    if not isinstance(op, LiteralPlaintext):
        raise TypeError("operand must be literal")
    if not isinstance(op.literal.primitive, Inv):
        raise TypeError("operand must be invertible")
    # noinspection PyTypeChecker
    res = LiteralPlaintext(
        literal=Literal(
            type_=op.literal.type,
            primitive=op.literal.primitive.inv(),
        )
    )
    store_plaintext_to_register(res, destination, registers)

def is_eq(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
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


def is_neq(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
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

def less_than(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    op1 = load_plaintext_from_operand(operands[0], registers, finalize_state)
    op2 = load_plaintext_from_operand(operands[1], registers, finalize_state)
    if not (isinstance(op1, LiteralPlaintext) and isinstance(op2, LiteralPlaintext)):
        raise TypeError("operands must be literals")
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

def less_than_or_equal(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    op1 = load_plaintext_from_operand(operands[0], registers, finalize_state)
    op2 = load_plaintext_from_operand(operands[1], registers, finalize_state)
    if not (isinstance(op1, LiteralPlaintext) and isinstance(op2, LiteralPlaintext)):
        raise TypeError("operands must be literals")
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
def modulo(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    op1 = load_plaintext_from_operand(operands[0], registers, finalize_state)
    op2 = load_plaintext_from_operand(operands[1], registers, finalize_state)
    if not (isinstance(op1, LiteralPlaintext) and isinstance(op2, LiteralPlaintext)):
        raise TypeError("operands must be literals")
    if not (isinstance(op1.literal.primitive, Mod) and isinstance(op2.literal.primitive, Mod)):
        raise TypeError("operands must be modulos")
    # noinspection PyTypeChecker
    res = LiteralPlaintext(
        literal=Literal(
            type_=op1.literal.type,
            primitive=op1.literal.primitive % op2.literal.primitive,
        )
    )
    store_plaintext_to_register(res, destination, registers)

def mul(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    op1 = load_plaintext_from_operand(operands[0], registers, finalize_state)
    op2 = load_plaintext_from_operand(operands[1], registers, finalize_state)
    if not (isinstance(op1, LiteralPlaintext) and isinstance(op2, LiteralPlaintext)):
        raise TypeError("operands must be literals")
    if not isinstance(op1.literal.primitive, Mul):
        raise TypeError("operands must be multiplicative")
    if (op1.literal.type == Literal.Type.Group and op2.literal.type != Literal.Type.Scalar) or (op1.literal.type == Literal.Type.Scalar and op2.literal.type != Literal.Type.Group):
        raise TypeError("invalid operand types")
    # noinspection PyTypeChecker
    res = LiteralPlaintext(
        literal=Literal(
            type_=op1.literal.type,
            primitive=op1.literal.primitive * op2.literal.primitive,
        )
    )
    store_plaintext_to_register(res, destination, registers)

def mul_wrapped(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    op1 = load_plaintext_from_operand(operands[0], registers, finalize_state)
    op2 = load_plaintext_from_operand(operands[1], registers, finalize_state)
    if not (isinstance(op1, LiteralPlaintext) and isinstance(op2, LiteralPlaintext)):
        raise TypeError("operands must be literals")
    if not isinstance(op1.literal.primitive, MulWrapped):
        raise TypeError("operands must be multiplicative")
    # noinspection PyTypeChecker
    res = LiteralPlaintext(
        literal=Literal(
            type_=op1.literal.type,
            primitive=op1.literal.primitive.mul_wrapped(op2.literal.primitive),
        )
    )
    store_plaintext_to_register(res, destination, registers)

def nand(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    op1 = load_plaintext_from_operand(operands[0], registers, finalize_state)
    op2 = load_plaintext_from_operand(operands[1], registers, finalize_state)
    if not isinstance(op1, LiteralPlaintext) or not isinstance(op2, LiteralPlaintext):
        raise TypeError("operands must be literals")
    if not isinstance(op1.literal.primitive, Nand):
        raise TypeError("operands must be nandable")
    # noinspection PyTypeChecker
    res = LiteralPlaintext(
        literal=Literal(
            type_=Literal.Type.Boolean,
            primitive=op1.literal.primitive.nand(op2.literal.primitive),
        )
    )
    store_plaintext_to_register(res, destination, registers)

def neg(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    op = load_plaintext_from_operand(operands[0], registers, finalize_state)
    if not isinstance(op, LiteralPlaintext):
        raise TypeError("operand must be literal")
    if not isinstance(op.literal.primitive, Neg):
        raise TypeError("operand must be negatable")
    # noinspection PyTypeChecker
    res = LiteralPlaintext(
        literal=Literal(
            type_=op.literal.type,
            primitive=-op.literal.primitive,
        )
    )
    store_plaintext_to_register(res, destination, registers)

def nor(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    op1 = load_plaintext_from_operand(operands[0], registers, finalize_state)
    op2 = load_plaintext_from_operand(operands[1], registers, finalize_state)
    if not isinstance(op1, LiteralPlaintext) or not isinstance(op2, LiteralPlaintext):
        raise TypeError("operands must be literals")
    if not isinstance(op1.literal.primitive, Nor):
        raise TypeError("operands must be norable")
    # noinspection PyTypeChecker
    res = LiteralPlaintext(
        literal=Literal(
            type_=op1.literal.type,
            primitive=op1.literal.primitive.nor(op2.literal.primitive),
        )
    )
    store_plaintext_to_register(res, destination, registers)

def not_(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    op = load_plaintext_from_operand(operands[0], registers, finalize_state)
    if not isinstance(op, LiteralPlaintext):
        raise TypeError("operand must be literal")
    if not isinstance(op.literal.primitive, Not):
        raise TypeError("operand must be invertable")
    # noinspection PyTypeChecker
    res = LiteralPlaintext(
        literal=Literal(
            type_=op.literal.type,
            primitive=~op.literal.primitive,
        )
    )
    store_plaintext_to_register(res, destination, registers)

def or_(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    op1 = load_plaintext_from_operand(operands[0], registers, finalize_state)
    op2 = load_plaintext_from_operand(operands[1], registers, finalize_state)
    if not (isinstance(op1, LiteralPlaintext) and isinstance(op2, LiteralPlaintext)):
        raise TypeError("operands must be literals")
    if not isinstance(op1.literal.primitive, Or):
        raise TypeError("operands must be bitwise or")
    # noinspection PyTypeChecker
    res = LiteralPlaintext(
        literal=Literal(
            type_=op1.literal.type,
            primitive=op1.literal.primitive | op2.literal.primitive
        )
    )
    store_plaintext_to_register(res, destination, registers)

def pow_(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    op1 = load_plaintext_from_operand(operands[0], registers, finalize_state)
    op2 = load_plaintext_from_operand(operands[1], registers, finalize_state)
    if not (isinstance(op1, LiteralPlaintext) and isinstance(op2, LiteralPlaintext)):
        raise TypeError("operands must be literals")
    if not isinstance(op1.literal.primitive, Pow):
        raise TypeError("operands must be exponentiable")
    # noinspection PyTypeChecker
    res = LiteralPlaintext(
        literal=Literal(
            type_=op1.literal.type,
            primitive=op1.literal.primitive ** op2.literal.primitive,
        )
    )
    store_plaintext_to_register(res, destination, registers)

def pow_wrapped(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    op1 = load_plaintext_from_operand(operands[0], registers, finalize_state)
    op2 = load_plaintext_from_operand(operands[1], registers, finalize_state)
    if not (isinstance(op1, LiteralPlaintext) and isinstance(op2, LiteralPlaintext)):
        raise TypeError("operands must be literals")
    if not isinstance(op1.literal.primitive, PowWrapped):
        raise TypeError("operands must be exponentiable")
    # noinspection PyTypeChecker
    res = LiteralPlaintext(
        literal=Literal(
            type_=op1.literal.type,
            primitive=op1.literal.primitive.pow_wrapped(op2.literal.primitive),
        )
    )
    store_plaintext_to_register(res, destination, registers)

def rem(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    op1 = load_plaintext_from_operand(operands[0], registers, finalize_state)
    op2 = load_plaintext_from_operand(operands[1], registers, finalize_state)
    if not (isinstance(op1, LiteralPlaintext) and isinstance(op2, LiteralPlaintext)):
        raise TypeError("operands must be literals")
    if not isinstance(op1.literal.primitive, Rem):
        raise TypeError("operands must be remainder")
    # noinspection PyTypeChecker
    res = LiteralPlaintext(
        literal=Literal(
            type_=op1.literal.type,
            primitive=op1.literal.primitive % op2.literal.primitive,
        )
    )
    store_plaintext_to_register(res, destination, registers)

def rem_wrapped(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    op1 = load_plaintext_from_operand(operands[0], registers, finalize_state)
    op2 = load_plaintext_from_operand(operands[1], registers, finalize_state)
    if not (isinstance(op1, LiteralPlaintext) and isinstance(op2, LiteralPlaintext)):
        raise TypeError("operands must be literals")
    if not isinstance(op1.literal.primitive, RemWrapped):
        raise TypeError("operands must be remainder")
    # noinspection PyTypeChecker
    res = LiteralPlaintext(
        literal=Literal(
            type_=op1.literal.type,
            primitive=op1.literal.primitive.rem_wrapped(op2.literal.primitive),
        )
    )
    store_plaintext_to_register(res, destination, registers)

def shl(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    op1 = load_plaintext_from_operand(operands[0], registers, finalize_state)
    op2 = load_plaintext_from_operand(operands[1], registers, finalize_state)
    if not (isinstance(op1, LiteralPlaintext) and isinstance(op2, LiteralPlaintext)):
        raise TypeError("operands must be literals")
    if not isinstance(op1.literal.primitive, Shl):
        raise TypeError("operands must be shift left")
    # noinspection PyTypeChecker
    res = LiteralPlaintext(
        literal=Literal(
            type_=op1.literal.type,
            primitive=op1.literal.primitive << op2.literal.primitive,
        )
    )
    store_plaintext_to_register(res, destination, registers)

def shl_wrapped(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    op1 = load_plaintext_from_operand(operands[0], registers, finalize_state)
    op2 = load_plaintext_from_operand(operands[1], registers, finalize_state)
    if not (isinstance(op1, LiteralPlaintext) and isinstance(op2, LiteralPlaintext)):
        raise TypeError("operands must be literals")
    if not isinstance(op1.literal.primitive, ShlWrapped):
        raise TypeError("operands must be shift left")
    # noinspection PyTypeChecker
    res = LiteralPlaintext(
        literal=Literal(
            type_=op1.literal.type,
            primitive=op1.literal.primitive.shl_wrapped(op2.literal.primitive),
        )
    )
    store_plaintext_to_register(res, destination, registers)

def shr(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    op1 = load_plaintext_from_operand(operands[0], registers, finalize_state)
    op2 = load_plaintext_from_operand(operands[1], registers, finalize_state)
    if not (isinstance(op1, LiteralPlaintext) and isinstance(op2, LiteralPlaintext)):
        raise TypeError("operands must be literals")
    if not isinstance(op1.literal.primitive, Shr):
        raise TypeError("operands must be shift right")
    # noinspection PyTypeChecker
    res = LiteralPlaintext(
        literal=Literal(
            type_=op1.literal.type,
            primitive=op1.literal.primitive >> op2.literal.primitive,
        )
    )
    store_plaintext_to_register(res, destination, registers)

def shr_wrapped(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    op1 = load_plaintext_from_operand(operands[0], registers, finalize_state)
    op2 = load_plaintext_from_operand(operands[1], registers, finalize_state)
    if not (isinstance(op1, LiteralPlaintext) and isinstance(op2, LiteralPlaintext)):
        raise TypeError("operands must be literals")
    if not isinstance(op1.literal.primitive, ShrWrapped):
        raise TypeError("operands must be shift right")
    # noinspection PyTypeChecker
    res = LiteralPlaintext(
        literal=Literal(
            type_=op1.literal.type,
            primitive=op1.literal.primitive.shr_wrapped(op2.literal.primitive),
        )
    )
    store_plaintext_to_register(res, destination, registers)


def square(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    op = load_plaintext_from_operand(operands[0], registers, finalize_state)
    if not isinstance(op, LiteralPlaintext):
        raise TypeError("operand must be literal")
    if not isinstance(op.literal.primitive, Square):
        raise TypeError("operand must be squareable")
    # noinspection PyTypeChecker
    res = LiteralPlaintext(
        literal=Literal(
            type_=op.literal.type,
            primitive=op.literal.primitive.square(),
        )
    )
    store_plaintext_to_register(res, destination, registers)

def square_root(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    op = load_plaintext_from_operand(operands[0], registers, finalize_state)
    if not isinstance(op, LiteralPlaintext):
        raise TypeError("operand must be literal")
    if not isinstance(op.literal.primitive, Sqrt):
        raise TypeError("operand must be square rootable")
    # noinspection PyTypeChecker
    res = LiteralPlaintext(
        literal=Literal(
            type_=op.literal.type,
            primitive=op.literal.primitive.sqrt(),
        )
    )
    store_plaintext_to_register(res, destination, registers)

def sub(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    op1 = load_plaintext_from_operand(operands[0], registers, finalize_state)
    op2 = load_plaintext_from_operand(operands[1], registers, finalize_state)
    if not (isinstance(op1, LiteralPlaintext) and isinstance(op2, LiteralPlaintext)):
        raise TypeError("operands must be literals")
    if not isinstance(op1.literal.primitive, Sub):
        raise TypeError("operands must be subtractable")
    # noinspection PyTypeChecker
    res = LiteralPlaintext(
        literal=Literal(
            type_=op1.literal.type,
            primitive=op1.literal.primitive - op2.literal.primitive
        )
    )
    store_plaintext_to_register(res, destination, registers)

def sub_wrapped(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    op1 = load_plaintext_from_operand(operands[0], registers, finalize_state)
    op2 = load_plaintext_from_operand(operands[1], registers, finalize_state)
    if not (isinstance(op1, LiteralPlaintext) and isinstance(op2, LiteralPlaintext)):
        raise TypeError("operands must be literals")
    if not isinstance(op1.literal.primitive, SubWrapped):
        raise TypeError("operands must be subtractable")
    # noinspection PyTypeChecker
    res = LiteralPlaintext(
        literal=Literal(
            type_=op1.literal.type,
            primitive=op1.literal.primitive.sub_wrapped(op2.literal.primitive),
        )
    )
    store_plaintext_to_register(res, destination, registers)

def ternary(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    op1 = load_plaintext_from_operand(operands[0], registers, finalize_state)
    op2 = load_plaintext_from_operand(operands[1], registers, finalize_state)
    op3 = load_plaintext_from_operand(operands[2], registers, finalize_state)
    if not isinstance(op1, LiteralPlaintext):
        raise TypeError("condition must be a literal")
    if op1.literal.type != Literal.Type.Boolean:
        raise TypeError("condition must be a boolean")
    if op1.literal.primitive:
        store_plaintext_to_register(op2, destination, registers)
    else:
        store_plaintext_to_register(op3, destination, registers)

def xor(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    op1 = load_plaintext_from_operand(operands[0], registers, finalize_state)
    op2 = load_plaintext_from_operand(operands[1], registers, finalize_state)
    if not isinstance(op1, LiteralPlaintext) or not isinstance(op2, LiteralPlaintext):
        raise TypeError("operands must be literals")
    if not isinstance(op1.literal.primitive, Xor):
        raise TypeError("operands must be bitwise xor")
    # noinspection PyTypeChecker
    res = LiteralPlaintext(
        literal=Literal(
            type_=op1.literal.type,
            primitive=op1.literal.primitive ^ op2.literal.primitive,
        )
    )
    store_plaintext_to_register(res, destination, registers)



literal_ops = {
    IT.Abs: abs_,
    IT.AbsWrapped: abs_wrapped,
    IT.Add: add,
    IT.AddWrapped: add_wrapped,
    IT.And: and_,
    IT.Div: div,
    IT.DivWrapped: div_wrapped,
    IT.Double: double,
    IT.GreaterThan: greater_than,
    IT.GreaterThanOrEqual: greater_than_or_equal,
    IT.Inv: inv,
    IT.IsEq: is_eq,
    IT.IsNeq: is_neq,
    IT.LessThan: less_than,
    IT.LessThanOrEqual: less_than_or_equal,
    IT.Modulo: modulo,
    IT.Mul: mul,
    IT.MulWrapped: mul_wrapped,
    IT.Nand: nand,
    IT.Neg: neg,
    IT.Nor: nor,
    IT.Not: not_,
    IT.Or: or_,
    IT.Pow: pow_,
    IT.PowWrapped: pow_wrapped,
    IT.Rem: rem,
    IT.RemWrapped: rem_wrapped,
    IT.Shl: shl,
    IT.ShlWrapped: shl_wrapped,
    IT.Shr: shr,
    IT.ShrWrapped: shr_wrapped,
    IT.Square: square,
    IT.SquareRoot: square_root,
    IT.Sub: sub,
    IT.SubWrapped: sub_wrapped,
    IT.Ternary: ternary,
    IT.Xor: xor,
}

hash_ops = {
    HT.HashBHP256: "bhp256",
    HT.HashBHP512: "bhp512",
    HT.HashBHP768: "bhp768",
    HT.HashBHP1024: "bhp1024",
    HT.HashKeccak256: "keccak256",
    HT.HashKeccak384: "keccak384",
    HT.HashKeccak512: "keccak512",
    HT.HashPED64: "ped64",
    HT.HashPED128: "ped128",
    HT.HashPSD2: "psd2",
    HT.HashPSD4: "psd4",
    HT.HashPSD8: "psd8",
    HT.HashSha3_256: "sha3_256",
    HT.HashSha3_384: "sha3_384",
    HT.HashSha3_512: "sha3_512",
}

commit_ops = {
    CmT.CommitBHP256: "bhp256",
    CmT.CommitBHP512: "bhp512",
    CmT.CommitBHP768: "bhp768",
    CmT.CommitBHP1024: "bhp1024",
    CmT.CommitPED64: "ped64",
    CmT.CommitPED128: "ped128",
}