from io import StringIO

from aleo_types import *


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
    if isinstance(value, LiteralPlaintextType):
        return value.literal_type.name.lower()
    elif isinstance(value, StructPlaintextType):
        return str(value.struct)
    elif isinstance(value, ArrayPlaintextType):
        return str(value.array_type)
    elif isinstance(value, ExternalStructPlaintextType):
        return str(value.locator)
    else:
        raise NotImplementedError

def finalize_type_to_str(value: FinalizeType):
    if isinstance(value, PlaintextFinalizeType):
        return f"{plaintext_type_to_str(value.plaintext_type)}.public"
    elif isinstance(value, FutureFinalizeType):
        return f"{str(value.locator)}.future"
    else:
        raise NotImplementedError


def value_type_to_mode_type_str(value: ValueType):
    mode = value.type.name.lower()
    if "record" in mode:
        mode = "private"
    if isinstance(value, ConstantValueType | PublicValueType | PrivateValueType):
        t = plaintext_type_to_str(value.plaintext_type)
    elif isinstance(value, RecordValueType):
        t = str(value.identifier)
    elif isinstance(value, ExternalRecordValueType):
        t = str(value.locator)
    else:
        raise NotImplementedError
    return mode, t

def public_or_private_to_str(value: PublicOrPrivate):
    if value == PublicOrPrivate.Public:
        return "public"
    return "private"

_instruction_type_to_str_map = {
    Instruction.Type.Abs: "abs",
    Instruction.Type.AbsWrapped: "abs.w",
    Instruction.Type.Add: "add",
    Instruction.Type.AddWrapped: "add.w",
    Instruction.Type.And: "and",
    Instruction.Type.AssertEq: "assert.eq",
    Instruction.Type.AssertNeq: "assert.neq",
    Instruction.Type.Async: "async",
    Instruction.Type.Call: "call",
    Instruction.Type.Cast: "cast",
    Instruction.Type.CastLossy: "cast.lossy",
    Instruction.Type.CommitBHP256: "commit.bhp256",
    Instruction.Type.CommitBHP512: "commit.bhp512",
    Instruction.Type.CommitBHP768: "commit.bhp768",
    Instruction.Type.CommitBHP1024: "commit.bhp1024",
    Instruction.Type.CommitPED64: "commit.ped64",
    Instruction.Type.CommitPED128: "commit.ped128",
    Instruction.Type.Div: "div",
    Instruction.Type.DivWrapped: "div.w",
    Instruction.Type.Double: "double",
    Instruction.Type.GreaterThan: "gt",
    Instruction.Type.GreaterThanOrEqual: "gte",
    Instruction.Type.HashBHP256: "hash.bhp256",
    Instruction.Type.HashBHP512: "hash.bhp512",
    Instruction.Type.HashBHP768: "hash.bhp768",
    Instruction.Type.HashBHP1024: "hash.bhp1024",
    Instruction.Type.HashKeccak256: "hash.keccak256",
    Instruction.Type.HashKeccak384: "hash.keccak384",
    Instruction.Type.HashKeccak512: "hash.keccak512",
    Instruction.Type.HashPED64: "hash.ped64",
    Instruction.Type.HashPED128: "hash.ped128",
    Instruction.Type.HashPSD2: "hash.psd2",
    Instruction.Type.HashPSD4: "hash.psd4",
    Instruction.Type.HashPSD8: "hash.psd8",
    Instruction.Type.HashSha3_256: "hash.sha3_256",
    Instruction.Type.HashSha3_384: "hash.sha3_384",
    Instruction.Type.HashSha3_512: "hash.sha3_512",
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
    Instruction.Type.MulWrapped: "mul.w",
    Instruction.Type.Nand: "nand",
    Instruction.Type.Neg: "neg",
    Instruction.Type.Nor: "nor",
    Instruction.Type.Not: "not",
    Instruction.Type.Or: "or",
    Instruction.Type.Pow: "pow",
    Instruction.Type.PowWrapped: "pow.w",
    Instruction.Type.Rem: "rem",
    Instruction.Type.RemWrapped: "rem.w",
    Instruction.Type.Shl: "shl",
    Instruction.Type.ShlWrapped: "shl.w",
    Instruction.Type.Shr: "shr",
    Instruction.Type.ShrWrapped: "shr.w",
    Instruction.Type.SignVerify: "sign_verify",
    Instruction.Type.Square: "square",
    Instruction.Type.SquareRoot: "square_root",
    Instruction.Type.Sub: "sub",
    Instruction.Type.SubWrapped: "sub.w",
    Instruction.Type.Ternary: "ternary",
    Instruction.Type.Xor: "xor",
    Instruction.Type.DeserializeBits: "deserialize.bits",
    Instruction.Type.DeserializeBitsRaw: "deserialize.bits.raw",
    Instruction.Type.ECDSAVerifyDigest: "ecdsa.verify.digest",
    Instruction.Type.ECDSAVerifyDigestEth: "ecdsa.verify.digest.eth",
    Instruction.Type.ECDSAVerifyKeccak256: "ecdsa.verify.keccak256",
    Instruction.Type.ECDSAVerifyKeccak256Raw: "ecdsa.verify.keccak256.raw",
    Instruction.Type.ECDSAVerifyKeccak256Eth: "ecdsa.verify.keccak256.eth",
    Instruction.Type.ECDSAVerifyKeccak384: "ecdsa.verify.keccak384",
    Instruction.Type.ECDSAVerifyKeccak384Raw: "ecdsa.verify.keccak384.raw",
    Instruction.Type.ECDSAVerifyKeccak384Eth: "ecdsa.verify.keccak384.eth",
    Instruction.Type.ECDSAVerifyKeccak512: "ecdsa.verify.keccak512",
    Instruction.Type.ECDSAVerifyKeccak512Raw: "ecdsa.verify.keccak512.raw",
    Instruction.Type.ECDSAVerifyKeccak512Eth: "ecdsa.verify.keccak512.eth",
    Instruction.Type.ECDSAVerifySha3_256: "ecdsa.verify.sha3_256",
    Instruction.Type.ECDSAVerifySha3_256Raw: "ecdsa.verify.sha3_256.raw",
    Instruction.Type.ECDSAVerifySha3_256Eth: "ecdsa.verify.sha3_256.eth",
    Instruction.Type.ECDSAVerifySha3_384: "ecdsa.verify.sha3_384",
    Instruction.Type.ECDSAVerifySha3_384Raw: "ecdsa.verify.sha3_384.raw",
    Instruction.Type.ECDSAVerifySha3_384Eth: "ecdsa.verify.sha3_384.eth",
    Instruction.Type.ECDSAVerifySha3_512: "ecdsa.verify.sha3_512",
    Instruction.Type.ECDSAVerifySha3_512Raw: "ecdsa.verify.sha3_512.raw",
    Instruction.Type.ECDSAVerifySha3_512Eth: "ecdsa.verify.sha3_512.eth",
    Instruction.Type.HashBHP256Raw: "hash.bhp256.raw",
    Instruction.Type.HashBHP512Raw: "hash.bhp512.raw",
    Instruction.Type.HashBHP768Raw: "hash.bhp768.raw",
    Instruction.Type.HashBHP1024Raw: "hash.bhp1024.raw",
    Instruction.Type.HashKeccak256Raw: "hash.keccak256.raw",
    Instruction.Type.HashKeccak256Native: "hash.keccak256.native",
    Instruction.Type.HashKeccak256NativeRaw: "hash.keccak256.native.raw",
    Instruction.Type.HashKeccak384Raw: "hash.keccak384.raw",
    Instruction.Type.HashKeccak384Native: "hash.keccak384.native",
    Instruction.Type.HashKeccak384NativeRaw: "hash.keccak384.native.raw",
    Instruction.Type.HashKeccak512Raw: "hash.keccak512.raw",
    Instruction.Type.HashKeccak512Native: "hash.keccak512.native",
    Instruction.Type.HashKeccak512NativeRaw: "hash.keccak512.native.raw",
    Instruction.Type.HashPED64Raw: "hash.ped64.raw",
    Instruction.Type.HashPED128Raw: "hash.ped128.raw",
    Instruction.Type.HashPSD2Raw: "hash.psd2.raw",
    Instruction.Type.HashPSD4Raw: "hash.psd4.raw",
    Instruction.Type.HashPSD8Raw: "hash.psd8.raw",
    Instruction.Type.HashSha3_256Raw: "hash.sha3_256.raw",
    Instruction.Type.HashSha3_256Native: "hash.sha3_256.native",
    Instruction.Type.HashSha3_256NativeRaw: "hash.sha3_256.native.raw",
    Instruction.Type.HashSha3_384Raw: "hash.sha3_384.raw",
    Instruction.Type.HashSha3_384Native: "hash.sha3_384.native",
    Instruction.Type.HashSha3_384NativeRaw: "hash.sha3_384.native.raw",
    Instruction.Type.HashSha3_512Raw: "hash.sha3_512.raw",
    Instruction.Type.HashSha3_512Native: "hash.sha3_512.native",
    Instruction.Type.HashSha3_512NativeRaw: "hash.sha3_512.native.raw",
    Instruction.Type.SerializeBits: "serialize.bits",
    Instruction.Type.SerializeBitsRaw: "serialize.bits.raw",
    
}

def instruction_type_to_str(value: Instruction.Type):
    return _instruction_type_to_str_map[value]