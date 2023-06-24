from io import StringIO

from node.types import *


class disasm_str(StringIO):

    def __init__(self):
        super().__init__()
        self.indent_level = 0

    def indent(self):
        self.indent_level += 1

    def unindent(self):
        self.indent_level -= 1

    def insert_line(self, content: str):
        self.write("    " * self.indent_level)
        self.write(content)
        self.write("\n")

    def insert(self, content: str):
        self.write(content)

    def __str__(self):
        return self.getvalue()


def plaintext_type_to_str(value: PlaintextType):
    match value.type:
        case PlaintextType.Type.Literal:
            value: LiteralPlaintextType
            return value.literal_type.name.lower()
        case PlaintextType.Type.Struct:
            value: StructPlaintextType
            return str(value.struct)


def value_type_to_mode_type_str(value: ValueType):
    mode = value.type.name.lower()
    if "record" in mode:
        mode = "private"
    match value.type:
        case ValueType.Type.Constant | ValueType.Type.Public | ValueType.Type.Private:
            # noinspection PyUnresolvedReferences
            t = plaintext_type_to_str(value.plaintext_type)
        case ValueType.Type.Record:
            value: RecordValueType
            t = str(value.identifier)
        case ValueType.Type.ExternalRecord:
            value: ExternalRecordValueType
            t = str(value.locator)
        case _:
            raise NotImplementedError
    return mode, t

def public_or_private_to_str(value: PublicOrPrivate):
    if value == PublicOrPrivate.Public:
        return "public"
    return "private"

_instruction_type_to_str_map = {
    Instruction.Type.Abs: "abs",
    Instruction.Type.AbsWrapped: "abs_wrapped",
    Instruction.Type.Add: "add",
    Instruction.Type.AddWrapped: "add_wrapped",
    Instruction.Type.And: "and",
    Instruction.Type.AssertEq: "assert_eq",
    Instruction.Type.AssertNeq: "assert_neq",
    Instruction.Type.Call: "call",
    Instruction.Type.Cast: "cast",
    Instruction.Type.CommitBHP256: "commit.bhp256",
    Instruction.Type.CommitBHP512: "commit.bhp512",
    Instruction.Type.CommitBHP768: "commit.bhp768",
    Instruction.Type.CommitBHP1024: "commit.bhp1024",
    Instruction.Type.CommitPED64: "commit.ped64",
    Instruction.Type.CommitPED128: "commit.ped128",
    Instruction.Type.Div: "div",
    Instruction.Type.DivWrapped: "div_wrapped",
    Instruction.Type.Double: "double",
    Instruction.Type.GreaterThan: "gt",
    Instruction.Type.GreaterThanOrEqual: "gte",
    Instruction.Type.HashBHP256: "hash.bhp256",
    Instruction.Type.HashBHP512: "hash.bhp512",
    Instruction.Type.HashBHP768: "hash.bhp768",
    Instruction.Type.HashBHP1024: "hash.bhp1024",
    Instruction.Type.HashPED64: "hash.ped64",
    Instruction.Type.HashPED128: "hash.ped128",
    Instruction.Type.HashPSD2: "hash.psd2",
    Instruction.Type.HashPSD4: "hash.psd4",
    Instruction.Type.HashPSD8: "hash.psd8",
    Instruction.Type.HashManyPSD2: "hash_many.psd2",
    Instruction.Type.HashManyPSD4: "hash_many.psd4",
    Instruction.Type.HashManyPSD8: "hash_many.psd8",
    Instruction.Type.Inv: "inv",
    Instruction.Type.IsEq: "is.eq",
    Instruction.Type.IsNeq: "is.neq",
    Instruction.Type.LessThan: "lt",
    Instruction.Type.LessThanOrEqual: "lte",
    Instruction.Type.Modulo: "mod",
    Instruction.Type.Mul: "mul",
    Instruction.Type.MulWrapped: "mul_wrapped",
    Instruction.Type.Nand: "nand",
    Instruction.Type.Neg: "neg",
    Instruction.Type.Nor: "nor",
    Instruction.Type.Not: "not",
    Instruction.Type.Or: "or",
    Instruction.Type.Pow: "pow",
    Instruction.Type.PowWrapped: "pow_wrapped",
    Instruction.Type.Rem: "rem",
    Instruction.Type.RemWrapped: "rem_wrapped",
    Instruction.Type.Shl: "shl",
    Instruction.Type.ShlWrapped: "shl_wrapped",
    Instruction.Type.Shr: "shr",
    Instruction.Type.ShrWrapped: "shr_wrapped",
    Instruction.Type.Square: "square",
    Instruction.Type.SquareRoot: "square_root",
    Instruction.Type.Sub: "sub",
    Instruction.Type.SubWrapped: "sub_wrapped",
    Instruction.Type.Ternary: "ternary",
    Instruction.Type.Xor: "xor",
}

def instruction_type_to_str(value: Instruction.Type):
    return _instruction_type_to_str_map[value]