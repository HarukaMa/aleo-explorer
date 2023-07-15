from typing import cast

from interpreter.environment import Registers
from interpreter.utils import load_plaintext_from_operand, store_plaintext_to_register, FinalizeState
from node.types import *

IT = Instruction.Type

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
        if type_ == HashInstruction.Type.HashBHP256:
            hash_bhp256(literals.operands, literals.destination, literals.destination_type, registers, finalize_state)
        else:
            raise NotImplementedError
    else:
        raise NotImplementedError

def abs_(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def abs_wrapped(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

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

    if not isinstance(cast_type, RegisterTypeCastType):
        raise NotImplementedError
    register_type = cast_type.register_type
    if not isinstance(register_type, PlaintextRegisterType):
        raise RuntimeError("invalid register type")
    plaintext_type = register_type.plaintext_type
    if isinstance(plaintext_type, LiteralPlaintextType):
        if not plaintext_type.literal_type in [LiteralType.Address, LiteralType.Field, LiteralType.Group]:
            raise AssertionError("snarkOS doesn't support casting from this type yet")
        raise NotImplementedError
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
    else:
        raise NotImplementedError

def commit_bhp256(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def commit_bhp512(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def commit_bhp768(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def commit_bhp1024(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def commit_ped64(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def commit_ped128(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

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
    raise NotImplementedError

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

def hash_bhp256(operands: tuple[Operand, Optional[Operand]], destination: Register, destination_type: LiteralType, registers: Registers, finalize_state: FinalizeState):
    op = load_plaintext_from_operand(operands[0], registers, finalize_state)
    value_type = destination_type.primitive_type
    value = value_type.load(BytesIO(aleo.hash_ops(PlaintextValue(plaintext=op).dump(), "bhp256", destination_type.dump())))
    res = LiteralPlaintext(
        literal=Literal(
            type_=Literal.Type(destination_type.value),
            primitive=value,
        )
    )
    store_plaintext_to_register(res, destination, registers)

def hash_bhp512(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def hash_bhp768(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def hash_bhp1024(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def hash_ped64(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def hash_ped128(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def hash_psd2(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def hash_psd4(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def hash_psd8(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def hash_many_psd2(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def hash_many_psd4(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def hash_many_psd8(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def inv(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

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
    raise NotImplementedError

def mul(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    op1 = load_plaintext_from_operand(operands[0], registers, finalize_state)
    op2 = load_plaintext_from_operand(operands[1], registers, finalize_state)
    if not (isinstance(op1, LiteralPlaintext) and isinstance(op2, LiteralPlaintext)):
        raise TypeError("operands must be literals")
    if not isinstance(op1.literal.primitive, Mul):
        raise TypeError("operands must be multiplicative")
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
    raise NotImplementedError

def neg(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def nor(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def not_(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    op = load_plaintext_from_operand(operands[0], registers, finalize_state)
    if not isinstance(op, LiteralPlaintext):
        raise TypeError("operand must be literal")
    if not isinstance(op.literal.primitive, Not):
        raise TypeError("operand must be invertable")
    # noinspection PyTypeChecker
    res = LiteralPlaintext(
        literal=Literal(
            type_=Literal.Type.Boolean,
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
    raise NotImplementedError

def pow_wrapped(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def rem(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def rem_wrapped(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

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
    raise NotImplementedError

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
    raise NotImplementedError

def square(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

def square_root(operands: list[Operand], destination: Register, registers: Registers, finalize_state: FinalizeState):
    raise NotImplementedError

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
    raise NotImplementedError


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