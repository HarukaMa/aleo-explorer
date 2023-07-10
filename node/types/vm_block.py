import json
import re
from hashlib import sha256, md5
from typing import Type as TType

from .vm_instruction import *


# util functions

def feature_string_from_instructions(instructions: list[Instruction]) -> str:
    s = [Instruction.feature_map[inst.type] for inst in instructions]
    if len(s) == 0:
        return ""
    res = [s[0]]
    for i in range(1, len(s)):
        if s[i] == s[i - 1]:
            continue
        res.append(s[i])
    return "".join(res)

class EvaluationDomain(Serializable):

    
    def __init__(self, *, size: u64, log_size_of_group: u32, size_as_field_element: Field, size_inv: Field,
                 group_gen: Field, group_gen_inv: Field, generator_inv: Field):
        self.size = size
        self.log_size_of_group = log_size_of_group
        self.size_as_field_element = size_as_field_element
        self.size_inv = size_inv
        self.group_gen = group_gen
        self.group_gen_inv = group_gen_inv
        self.generator_inv = generator_inv

    def dump(self) -> bytes:
        return self.size.dump() + self.log_size_of_group.dump() + self.size_as_field_element.dump() + \
               self.size_inv.dump() + self.group_gen.dump() + self.group_gen_inv.dump() + self.generator_inv.dump()

    @classmethod
    def load(cls, data: BytesIO):
        size = u64.load(data)
        log_size_of_group = u32.load(data)
        size_as_field_element = Field.load(data)
        size_inv = Field.load(data)
        group_gen = Field.load(data)
        group_gen_inv = Field.load(data)
        generator_inv = Field.load(data)
        return cls(size=size, log_size_of_group=log_size_of_group, size_as_field_element=size_as_field_element,
                   size_inv=size_inv, group_gen=group_gen, group_gen_inv=group_gen_inv, generator_inv=generator_inv)

class EvaluationsOnDomain(Serializable):

    def __init__(self, *, evaluations: Vec[Field, u64], domain: EvaluationDomain):
        self.evaluations = evaluations
        self.domain = domain

    def dump(self) -> bytes:
        return self.evaluations.dump() + self.domain.dump()

    @classmethod
    def load(cls, data: BytesIO):
        evaluations = Vec[Field, u64].load(data)
        domain = EvaluationDomain.load(data)
        return cls(evaluations=evaluations, domain=domain)


class EpochChallenge(Serializable):

    def __init__(self, *, epoch_number: u32, epoch_block_hash: BlockHash, epoch_polynomial: Vec[Field, u64],
                 epoch_polynomial_evaluations: EvaluationsOnDomain):
        self.epoch_number = epoch_number
        self.epoch_block_hash = epoch_block_hash
        self.epoch_polynomial = epoch_polynomial
        self.epoch_polynomial_evaluations = epoch_polynomial_evaluations

    def dump(self) -> bytes:
        return self.epoch_number.dump() + self.epoch_block_hash.dump() + self.epoch_polynomial.dump() + \
               self.epoch_polynomial_evaluations.dump()

    @classmethod
    def load(cls, data: BytesIO):
        epoch_number = u32.load(data)
        epoch_block_hash = BlockHash.load(data)
        epoch_polynomial = Vec[Field, u64].load(data)
        epoch_polynomial_evaluations = EvaluationsOnDomain.load(data)
        return cls(epoch_number=epoch_number, epoch_block_hash=epoch_block_hash, epoch_polynomial=epoch_polynomial,
                   epoch_polynomial_evaluations=epoch_polynomial_evaluations)


class MapKey(Serializable):

    def __init__(self, *, name: Identifier, plaintext_type: PlaintextType):
        self.name = name
        self.plaintext_type = plaintext_type

    def dump(self) -> bytes:
        return self.name.dump() + self.plaintext_type.dump()

    @classmethod
    def load(cls, data: BytesIO):
        name = Identifier.load(data)
        plaintext_type = PlaintextType.load(data)
        return cls(name=name, plaintext_type=plaintext_type)


class MapValue(Serializable):

    def __init__(self, *, name: Identifier, plaintext_type: PlaintextType):
        self.name = name
        self.plaintext_type = plaintext_type

    def dump(self) -> bytes:
        return self.name.dump() + self.plaintext_type.dump()

    @classmethod
    def load(cls, data: BytesIO):
        name = Identifier.load(data)
        plaintext_type = PlaintextType.load(data)
        return cls(name=name, plaintext_type=plaintext_type)


class Mapping(Serializable):

    def __init__(self, *, name: Identifier, key: MapKey, value: MapValue):
        self.name = name
        self.key = key
        self.value = value

    def dump(self) -> bytes:
        return self.name.dump() + self.key.dump() + self.value.dump()

    @classmethod
    def load(cls, data: BytesIO):
        name = Identifier.load(data)
        key = MapKey.load(data)
        value = MapValue.load(data)
        return cls(name=name, key=key, value=value)


class Struct(Serializable):

    def __init__(self, *, name: Identifier, members: Vec[Tuple[Identifier, PlaintextType], u16]):
        self.name = name
        self.members = members

    def dump(self) -> bytes:
        return self.name.dump() + self.members.dump()

    @classmethod
    def load(cls, data: BytesIO):
        name = Identifier.load(data)
        members = Vec[Tuple[Identifier, PlaintextType], u16].load(data)
        return cls(name=name, members=members)

    def get_member_type(self, member_name: Identifier) -> PlaintextType:
        for member in self.members:
            if member[0] == member_name:
                return member[1]
        raise ValueError("member not found")


class PublicOrPrivate(IntEnumu8):
    Public = 0
    Private = 1


class EntryType(Serializable):  # enum

    class Type(IntEnumu8):
        Constant = 0
        Public = 1
        Private = 2

    def __init__(self, *, type_: Type, plaintext_type: PlaintextType):
        self.type = type_
        self.plaintext_type = plaintext_type

    def dump(self) -> bytes:
        return self.type.dump() + self.plaintext_type.dump()

    @classmethod
    def load(cls, data: BytesIO):
        type_ = cls.Type.load(data)
        plaintext_type = PlaintextType.load(data)
        return cls(type_=type_, plaintext_type=plaintext_type)


class RecordType(Serializable):

    def __init__(self, *, name: Identifier, owner: PublicOrPrivate, entries: Vec[Tuple[Identifier, EntryType], u16]):
        self.name = name
        self.owner = owner
        self.entries = entries

    def dump(self) -> bytes:
        return self.name.dump() + self.owner.dump() + self.entries.dump()

    @classmethod
    def load(cls, data: BytesIO):
        name = Identifier.load(data)
        owner = PublicOrPrivate.load(data)
        entries = Vec[Tuple[Identifier, EntryType], u16].load(data)
        return cls(name=name, owner=owner, entries=entries)


class ClosureInput(Serializable):

    def __init__(self, *, register: Register, register_type: RegisterType):
        self.register = register
        self.register_type = register_type

    def dump(self) -> bytes:
        return self.register.dump() + self.register_type.dump()

    @classmethod
    def load(cls, data: BytesIO):
        register = Register.load(data)
        register_type = RegisterType.load(data)
        return cls(register=register, register_type=register_type)


class ClosureOutput(Serializable):

    def __init__(self, *, operand: Operand, register_type: RegisterType):
        self.operand = operand
        self.register_type = register_type

    def dump(self) -> bytes:
        return self.operand.dump() + self.register_type.dump()

    @classmethod
    def load(cls, data: BytesIO):
        operand = Operand.load(data)
        register_type = RegisterType.load(data)
        return cls(operand=operand, register_type=register_type)


class Closure(Serializable):

    def __init__(self, *, name: Identifier, inputs: Vec[ClosureInput, u16], instructions: Vec[Instruction, u32],
                 outputs: Vec[ClosureOutput, u16]):
        self.name = name
        self.inputs = inputs
        self.instructions = instructions
        self.outputs = outputs

    def dump(self) -> bytes:
        return self.name.dump() + self.inputs.dump() + self.instructions.dump() + self.outputs.dump()

    @classmethod
    def load(cls, data: BytesIO):
        name = Identifier.load(data)
        inputs = Vec[ClosureInput, u16].load(data)
        instructions = Vec[Instruction, u32].load(data)
        outputs = Vec[ClosureOutput, u16].load(data)
        return cls(name=name, inputs=inputs, instructions=instructions, outputs=outputs)

    def instruction_feature_string(self) -> str:
        return feature_string_from_instructions(self.instructions)


class FinalizeCommand(Serializable):

    def __init__(self, *, operands: Vec[Operand, u8]):
        self.operands = operands

    def dump(self) -> bytes:
        return self.operands.dump()

    @classmethod
    def load(cls, data: BytesIO):
        operands = Vec[Operand, u8].load(data)
        return cls(operands=operands)


class Command(EnumBaseSerialize, RustEnum, Serializable):
    type: "Command.Type"

    class Type(IntEnumu8):
        Instruction = 0
        Contains = 1
        Get = 2
        GetOrUse = 3
        RandChaCha = 4
        Remove = 5
        Set = 6
        BranchEq = 7
        BranchNeq = 8
        Position = 9

    fee_map = {
        Type.Instruction: 0,
        Type.Contains: 250_000,
        Type.Get: 500_000,
        Type.GetOrUse: 500_000,
        Type.RandChaCha: 500_000,
        Type.Remove: 10_000,
        Type.Set: 1_000_000,
        Type.BranchEq: 5_000,
        Type.BranchNeq: 5_000,
        Type.Position: 1_000,
    }

    @classmethod
    def load(cls, data: BytesIO):
        type_ = cls.Type.load(data)
        if type_ == cls.Type.Instruction:
            return InstructionCommand.load(data)
        elif type_ == cls.Type.Contains:
            return ContainsCommand.load(data)
        elif type_ == cls.Type.Get:
            return GetCommand.load(data)
        elif type_ == cls.Type.GetOrUse:
            return GetOrUseCommand.load(data)
        elif type_ == cls.Type.RandChaCha:
            return RandChaChaCommand.load(data)
        elif type_ == cls.Type.Remove:
            return RemoveCommand.load(data)
        elif type_ == cls.Type.Set:
            return SetCommand.load(data)
        elif type_ == cls.Type.BranchEq:
            return BranchEqCommand.load(data)
        elif type_ == cls.Type.BranchNeq:
            return BranchNeqCommand.load(data)
        elif type_ == cls.Type.Position:
            return PositionCommand.load(data)
        else:
            raise ValueError("Invalid variant")

    @property
    def cost(self) -> int:
        if isinstance(self, InstructionCommand):
            return self.instruction.cost
        return self.fee_map[self.type]

class InstructionCommand(Command):
    type = Command.Type.Instruction

    def __init__(self, *, instruction: Instruction):
        self.instruction = instruction

    def dump(self) -> bytes:
        return self.type.dump() + self.instruction.dump()

    @classmethod
    def load(cls, data: BytesIO):
        instruction = Instruction.load(data)
        return cls(instruction=instruction)

class ContainsCommand(Command):
    type = Command.Type.Contains

    def __init__(self, *, mapping: Identifier, key: Operand, destination: Register):
        self.mapping = mapping
        self.key = key
        self.destination = destination

    def dump(self) -> bytes:
        return self.type.dump() + self.mapping.dump() + self.key.dump() + self.destination.dump()

    @classmethod
    def load(cls, data: BytesIO):
        mapping = Identifier.load(data)
        key = Operand.load(data)
        destination = Register.load(data)
        return cls(mapping=mapping, key=key, destination=destination)

class GetCommand(Command):
    type = Command.Type.Get

    def __init__(self, *, mapping: Identifier, key: Operand, destination: Register):
        self.mapping = mapping
        self.key = key
        self.destination = destination

    def dump(self) -> bytes:
        return self.type.dump() + self.mapping.dump() + self.key.dump() + self.destination.dump()

    @classmethod
    def load(cls, data: BytesIO):
        mapping = Identifier.load(data)
        key = Operand.load(data)
        destination = Register.load(data)
        return cls(mapping=mapping, key=key, destination=destination)


class GetOrUseCommand(Command):
    type = Command.Type.GetOrUse

    def __init__(self, *, mapping: Identifier, key: Operand, default: Operand, destination: Register):
        self.mapping = mapping
        self.key = key
        self.default = default
        self.destination = destination

    def dump(self) -> bytes:
        return self.type.dump() + self.mapping.dump() + self.key.dump() + self.default.dump() + self.destination.dump()

    @classmethod
    def load(cls, data: BytesIO):
        mapping = Identifier.load(data)
        key = Operand.load(data)
        default = Operand.load(data)
        destination = Register.load(data)
        return cls(mapping=mapping, key=key, default=default, destination=destination)


class RandChaChaCommand(Command):
    type = Command.Type.RandChaCha

    def __init__(self, *, operands: Vec[Operand, u8], destination: Register, destination_type: LiteralType):
        self.operands = operands
        self.destination = destination
        self.destination_type = destination_type

    def dump(self) -> bytes:
        return self.type.dump() + self.operands.dump() + self.destination.dump() + self.destination_type.dump()

    @classmethod
    def load(cls, data: BytesIO):
        operands = Vec[Operand, u8].load(data)
        destination = Register.load(data)
        destination_type = LiteralType.load(data)
        return cls(operands=operands, destination=destination, destination_type=destination_type)

class RemoveCommand(Command):
    type = Command.Type.Remove

    def __init__(self, *, mapping: Identifier, key: Operand):
        self.mapping = mapping
        self.key = key

    def dump(self) -> bytes:
        return self.type.dump() + self.mapping.dump() + self.key.dump()

    @classmethod
    def load(cls, data: BytesIO):
        mapping = Identifier.load(data)
        key = Operand.load(data)
        return cls(mapping=mapping, key=key)

class SetCommand(Command):
    type = Command.Type.Set

    def __init__(self, *, mapping: Identifier, key: Operand, value: Operand):
        self.mapping = mapping
        self.key = key
        self.value = value

    def dump(self) -> bytes:
        return self.type.dump() + self.mapping.dump() + self.key.dump() + self.value.dump()

    @classmethod
    def load(cls, data: BytesIO):
        mapping = Identifier.load(data)
        key = Operand.load(data)
        value = Operand.load(data)
        return cls(mapping=mapping, key=key, value=value)

class BranchEqCommand(Command):
    type = Command.Type.BranchEq

    def __init__(self, *, first: Operand, second: Operand, position: Identifier):
        self.first = first
        self.second = second
        self.position = position

    def dump(self) -> bytes:
        return self.type.dump() + self.first.dump() + self.second.dump() + self.position.dump()

    @classmethod
    def load(cls, data: BytesIO):
        first = Operand.load(data)
        second = Operand.load(data)
        position = Identifier.load(data)
        return cls(first=first, second=second, position=position)

class BranchNeqCommand(Command):
    type = Command.Type.BranchNeq

    def __init__(self, *, first: Operand, second: Operand, position: Identifier):
        self.first = first
        self.second = second
        self.position = position

    def dump(self) -> bytes:
        return self.type.dump() + self.first.dump() + self.second.dump() + self.position.dump()

    @classmethod
    def load(cls, data: BytesIO):
        first = Operand.load(data)
        second = Operand.load(data)
        position = Identifier.load(data)
        return cls(first=first, second=second, position=position)

class PositionCommand(Command):
    type = Command.Type.Position

    def __init__(self, *, position: Identifier):
        self.position = position

    def dump(self) -> bytes:
        return self.type.dump() + self.position.dump()

    @classmethod
    def load(cls, data: BytesIO):
        position = Identifier.load(data)
        return cls(position=position)


class FinalizeInput(Serializable):

    def __init__(self, *, register: Register, plaintext_type: PlaintextType):
        self.register = register
        self.plaintext_type = plaintext_type

    def dump(self) -> bytes:
        return self.register.dump() + self.plaintext_type.dump()

    @classmethod
    def load(cls, data: BytesIO):
        register = Register.load(data)
        plaintext_type = PlaintextType.load(data)
        return cls(register=register, plaintext_type=plaintext_type)

class Finalize(Serializable):

    def __init__(self, *, name: Identifier, inputs: Vec[FinalizeInput, u16], commands: Vec[Command, u16]):
        self.name = name
        self.inputs = inputs
        self.commands = commands

    def dump(self) -> bytes:
        return self.name.dump() + self.inputs.dump() + self.commands.dump()

    @classmethod
    def load(cls, data: BytesIO):
        name = Identifier.load(data)
        inputs = Vec[FinalizeInput, u16].load(data)
        commands = Vec[Command, u16].load(data)
        return cls(name=name, inputs=inputs, commands=commands)

    @property
    def cost(self) -> int:
        return sum(command.cost for command in self.commands)


class ValueType(EnumBaseSerialize, RustEnum, Serializable):

    class Type(IntEnumu8):
        Constant = 0
        Public = 1
        Private = 2
        Record = 3
        ExternalRecord = 4

    type: Type

    @classmethod
    def load(cls, data: BytesIO):
        type_ = cls.Type.load(data)
        if type_ == cls.Type.Constant:
            return ConstantValueType.load(data)
        elif type_ == cls.Type.Public:
            return PublicValueType.load(data)
        elif type_ == cls.Type.Private:
            return PrivateValueType.load(data)
        elif type_ == cls.Type.Record:
            return RecordValueType.load(data)
        elif type_ == cls.Type.ExternalRecord:
            return ExternalRecordValueType.load(data)
        else:
            raise ValueError("Invalid variant")


class ConstantValueType(ValueType):
    type = ValueType.Type.Constant

    def __init__(self, *, plaintext_type: PlaintextType):
        self.plaintext_type = plaintext_type

    def dump(self) -> bytes:
        return self.type.dump() + self.plaintext_type.dump()

    @classmethod
    def load(cls, data: BytesIO):
        plaintext_type = PlaintextType.load(data)
        return cls(plaintext_type=plaintext_type)


class PublicValueType(ValueType):
    type = ValueType.Type.Public

    def __init__(self, *, plaintext_type: PlaintextType):
        self.plaintext_type = plaintext_type

    def dump(self) -> bytes:
        return self.type.dump() + self.plaintext_type.dump()

    @classmethod
    def load(cls, data: BytesIO):
        plaintext_type = PlaintextType.load(data)
        return cls(plaintext_type=plaintext_type)


class PrivateValueType(ValueType):
    type = ValueType.Type.Private

    def __init__(self, *, plaintext_type: PlaintextType):
        self.plaintext_type = plaintext_type

    def dump(self) -> bytes:
        return self.type.dump() + self.plaintext_type.dump()

    @classmethod
    def load(cls, data: BytesIO):
        plaintext_type = PlaintextType.load(data)
        return cls(plaintext_type=plaintext_type)


class RecordValueType(ValueType):
    type = ValueType.Type.Record

    def __init__(self, *, identifier: Identifier):
        self.identifier = identifier

    def dump(self) -> bytes:
        return self.type.dump() + self.identifier.dump()

    @classmethod
    def load(cls, data: BytesIO):
        identifier = Identifier.load(data)
        return cls(identifier=identifier)


class ExternalRecordValueType(ValueType):
    type = ValueType.Type.ExternalRecord

    def __init__(self, *, locator: Locator):
        self.locator = locator

    def dump(self) -> bytes:
        return self.type.dump() + self.locator.dump()

    @classmethod
    def load(cls, data: BytesIO):
        locator = Locator.load(data)
        return cls(locator=locator)


class FunctionInput(Serializable):

    def __init__(self, *, register: Register, value_type: ValueType):
        self.register = register
        self.value_type = value_type

    def dump(self) -> bytes:
        return self.register.dump() + self.value_type.dump()

    @classmethod
    def load(cls, data: BytesIO):
        register = Register.load(data)
        value_type = ValueType.load(data)
        return cls(register=register, value_type=value_type)


class FunctionOutput(Serializable):

    def __init__(self, *, operand: Operand, value_type: ValueType):
        self.operand = operand
        self.value_type = value_type

    def dump(self) -> bytes:
        return self.operand.dump() + self.value_type.dump()

    @classmethod
    def load(cls, data: BytesIO):
        operand = Operand.load(data)
        value_type = ValueType.load(data)
        return cls(operand=operand, value_type=value_type)


class Function(Serializable):

    def __init__(self, *, name: Identifier, inputs: Vec[FunctionInput, u16], instructions: Vec[Instruction, u32],
                 outputs: Vec[FunctionOutput, u16], finalize: Option[Tuple[FinalizeCommand, Finalize]]):
        self.name = name
        self.inputs = inputs
        self.instructions = instructions
        self.outputs = outputs
        self.finalize = finalize

    def dump(self) -> bytes:
        res = b""
        res += self.name.dump()
        res += self.inputs.dump()
        res += self.instructions.dump()
        res += self.outputs.dump()
        res += self.finalize.dump()
        return res

    @classmethod
    def load(cls, data: BytesIO):
        name = Identifier.load(data)
        inputs = Vec[FunctionInput, u16].load(data)
        instructions = Vec[Instruction, u32].load(data)
        outputs = Vec[FunctionOutput, u16].load(data)
        finalize = Option[Tuple[FinalizeCommand, Finalize]].load(data)
        return cls(name=name, inputs=inputs, instructions=instructions, outputs=outputs, finalize=finalize)

    def instruction_feature_string(self) -> str:
        return feature_string_from_instructions(self.instructions)


class ProgramDefinition(IntEnumu8):
    Mapping = 0
    Struct = 1
    Record = 2
    Closure = 3
    Function = 4


class Program(Serializable):
    version = u8()

    def __init__(self, *, id_: ProgramID, imports: Vec[Import, u8], mappings: dict[Identifier, Mapping],
                 structs: dict[Identifier, Struct], records: dict[Identifier, RecordType],
                 closures: dict[Identifier, Closure], functions: dict[Identifier, Function],
                 identifiers: list[tuple[Identifier, ProgramDefinition]]):
        self.id = id_
        self.imports = imports
        self.mappings = mappings
        self.structs = structs
        self.records = records
        self.closures = closures
        self.functions = functions
        self.identifiers = identifiers

    def dump(self) -> bytes:
        res = b""
        res += self.version.dump()
        res += self.id.dump()
        res += self.imports.dump()
        res += u16(len(self.identifiers)).dump()
        for i, d in self.identifiers:
            res += d.dump()
            if d == ProgramDefinition.Mapping:
                res += self.mappings[i].dump()
            elif d == ProgramDefinition.Struct:
                res += self.structs[i].dump()
            elif d == ProgramDefinition.Record:
                res += self.records[i].dump()
            elif d == ProgramDefinition.Closure:
                res += self.closures[i].dump()
            elif d == ProgramDefinition.Function:
                res += self.functions[i].dump()
        return res

    @classmethod
    def load(cls, data: BytesIO):
        version = u8.load(data)
        if version != cls.version:
            raise ValueError("Invalid version")
        id_ = ProgramID.load(data)
        imports = Vec[Import, u8].load(data)
        identifiers: list[tuple[Identifier, ProgramDefinition]] = []
        mappings = {}
        structs = {}
        records = {}
        closures = {}
        functions = {}
        n = u16.load(data)
        for _ in range(n):
            d = ProgramDefinition.load(data)
            if d == ProgramDefinition.Mapping:
                m = Mapping.load(data)
                mappings[m.name] = m
                identifiers.append((m.name, d))
            elif d == ProgramDefinition.Struct:
                i = Struct.load(data)
                structs[i.name] = i
                identifiers.append((i.name, d))
            elif d == ProgramDefinition.Record:
                r = RecordType.load(data)
                records[r.name] = r
                identifiers.append((r.name, d))
            elif d == ProgramDefinition.Closure:
                c = Closure.load(data)
                closures[c.name] = c
                identifiers.append((c.name, d))
            elif d == ProgramDefinition.Function:
                f = Function.load(data)
                functions[f.name] = f
                identifiers.append((f.name, d))
        return cls(id_=id_, imports=imports, mappings=mappings, structs=structs, records=records,
                   closures=closures, functions=functions, identifiers=identifiers)

    def is_helloworld(self) -> bool:
        header_length = len(self.version.dump() + self.id.dump())
        body = self.dump()[header_length:]
        if body == b'\x00\x01\x00\x04\x04main\x02\x00\x00\x00\x01\x00\x0b\x00\x00\x01\x02\x00\x0b\x00\x01\x00\x00\x00\x02\x00\x01\x00\x00\x01\x00\x01\x00\x02\x01\x00\x01\x00\x02\x02\x00\x0b\x00\x00':
            return True
        elif body == b'\x00\x01\x00\x04\x05hello\x02\x00\x00\x00\x01\x00\x0b\x00\x00\x01\x02\x00\x0b\x00\x01\x00\x00\x00\x02\x00\x01\x00\x00\x01\x00\x01\x00\x02\x01\x00\x01\x00\x02\x02\x00\x0b\x00\x00':
            return True
        return False

    def feature_hash(self) -> bytes:
        feature_string = "".join(
            ["S"] * len(self.structs) +
            ["R"] * len(self.records) +
            [("C" + c.instruction_feature_string()) for c in self.closures.values()] +
            [("F" + f.instruction_feature_string()) for f in self.functions.values()]
        )
        return md5(feature_string.encode()).digest()



class CircuitInfo(Serializable):

    def __init__(self, *, num_public_inputs: usize, num_variables: usize, num_constraints: usize,
                 num_non_zero_a: usize, num_non_zero_b: usize, num_non_zero_c: usize):
        self.num_public_inputs = num_public_inputs
        self.num_variables = num_variables
        self.num_constraints = num_constraints
        self.num_non_zero_a = num_non_zero_a
        self.num_non_zero_b = num_non_zero_b
        self.num_non_zero_c = num_non_zero_c

    def dump(self) -> bytes:
        res = b""
        res += self.num_public_inputs.dump()
        res += self.num_variables.dump()
        res += self.num_constraints.dump()
        res += self.num_non_zero_a.dump()
        res += self.num_non_zero_b.dump()
        res += self.num_non_zero_c.dump()
        return res

    @classmethod
    def load(cls, data: BytesIO):
        num_public_inputs = usize.load(data)
        num_variables = usize.load(data)
        num_constraints = usize.load(data)
        num_non_zero_a = usize.load(data)
        num_non_zero_b = usize.load(data)
        num_non_zero_c = usize.load(data)
        return cls(num_public_inputs=num_public_inputs, num_variables=num_variables, num_constraints=num_constraints,
                   num_non_zero_a=num_non_zero_a, num_non_zero_b=num_non_zero_b, num_non_zero_c=num_non_zero_c)


class KZGCommitment(Serializable):
    # Compressed for serde
    def __init__(self, *, element: G1Affine):
        self.element = element

    def dump(self) -> bytes:
        return self.element.dump()

    @classmethod
    def load(cls, data: BytesIO):
        return cls(element=G1Affine.load(data))


class KZGVerifierKey(Serializable):

    def __init__(self, *, g: G1Affine, gamma_g: G1Affine, h: G2Affine, beta_h: G2Affine):
        self.g = g
        self.gamma_g = gamma_g
        self.h = h
        self.beta_h = beta_h

    def dump(self) -> bytes:
        res = b""
        res += self.g.dump()
        res += self.gamma_g.dump()
        res += self.h.dump()
        res += self.beta_h.dump()
        return res

    @classmethod
    def load(cls, data: BytesIO):
        g = G1Affine.load(data)
        gamma_g = G1Affine.load(data)
        h = G2Affine.load(data)
        beta_h = G2Affine.load(data)
        return cls(g=g, gamma_g=gamma_g, h=h, beta_h=beta_h)


class SonicVerifierKey(Serializable):

    def __init__(self, *, vk: KZGVerifierKey,
                 degree_bounds_and_neg_powers_of_h: Option[Vec[Tuple[usize, G2Affine], u64]],
                 supported_degree: usize, max_degree: usize):
        self.vk = vk
        self.degree_bounds_and_neg_powers_of_h = degree_bounds_and_neg_powers_of_h
        self.supported_degree = supported_degree
        self.max_degree = max_degree

    def dump(self) -> bytes:
        res = b""
        res += self.vk.dump()
        res += self.degree_bounds_and_neg_powers_of_h.dump()
        res += self.supported_degree.dump()
        res += self.max_degree.dump()
        return res

    @classmethod
    def load(cls, data: BytesIO):
        vk = KZGVerifierKey.load(data)
        degree_bounds_and_neg_powers_of_h = Option[Vec[Tuple[usize, G2Affine], u64]].load(data)
        supported_degree = usize.load(data)
        max_degree = usize.load(data)
        return cls(vk=vk, degree_bounds_and_neg_powers_of_h=degree_bounds_and_neg_powers_of_h,
                   supported_degree=supported_degree, max_degree=max_degree)


class VerifyingKey(Serializable):
    version = u8()

    # Skipping a layer of marlin::CircuitVerifyingKey
    def __init__(self, *, circuit_info: CircuitInfo, circuit_commitments: Vec[KZGCommitment, u64], id_: Vec[u8, 32]):
        self.circuit_info = circuit_info
        self.circuit_commitments = circuit_commitments
        self.id = id_

    def dump(self) -> bytes:
        res = b""
        res += self.version.dump()
        res += self.circuit_info.dump()
        res += self.circuit_commitments.dump()
        res += self.id.dump()
        return res

    @classmethod
    def load(cls, data: BytesIO):
        version = u8.load(data)
        if version != cls.version:
            raise ValueError("Invalid version")
        circuit_info = CircuitInfo.load(data)
        circuit_commitments = Vec[KZGCommitment, u64].load(data)
        id_ = Vec[u8, 32].load(data)
        return cls(circuit_info=circuit_info, circuit_commitments=circuit_commitments, id_=id_)


class KZGProof(Serializable):

    def __init__(self, *, w: G1Affine, random_v: Option[Field]):
        self.w = w
        self.random_v = random_v

    def dump(self) -> bytes:
        return self.w.dump() + self.random_v.dump()

    @classmethod
    def load(cls, data: BytesIO):
        w = G1Affine.load(data)
        random_v = Option[Field].load(data)
        return cls(w=w, random_v=random_v)

class BatchProof(Serializable):

    def __init__(self, *, proof: Vec[KZGProof, u64]):
        self.proof = proof

    def dump(self) -> bytes:
        return self.proof.dump()

    @classmethod
    def load(cls, data: BytesIO):
        proof = Vec[KZGProof, u64].load(data)
        return cls(proof=proof)


class BatchLCProof(Serializable):

    def __init__(self, *, proof: BatchProof, evaluations: Option[Vec[Field, u64]]):
        self.proof = proof
        self.evaluations = evaluations

    def dump(self) -> bytes:
        return self.proof.dump() + self.evaluations.dump()

    @classmethod
    def load(cls, data: BytesIO):
        proof = BatchProof.load(data)
        evaluations = Option[Vec[Field, u64]].load(data)
        return cls(proof=proof, evaluations=evaluations)


class Certificate(Serializable):
    version = u8()

    # Skipping a layer of marlin::Certificate
    def __init__(self, *, pc_proof: BatchLCProof):
        self.pc_proof = pc_proof

    def dump(self) -> bytes:
        return self.version.dump() + self.pc_proof.dump()

    @classmethod
    def load(cls, data: BytesIO):
        version = u8.load(data)
        if version != cls.version:
            raise ValueError("Invalid version")
        pc_proof = BatchLCProof.load(data)
        return cls(pc_proof=pc_proof)


class Deployment(Serializable):
    version = u8()

    def __init__(self, *, edition: u16, program: Program,
                 verifying_keys: Vec[Tuple[Identifier, VerifyingKey, Certificate], u16]):
        self.edition = edition
        self.program = program
        self.verifying_keys = verifying_keys

    def dump(self) -> bytes:
        res = b""
        res += self.version.dump()
        res += self.edition.dump()
        res += self.program.dump()
        res += self.verifying_keys.dump()
        return res

    @classmethod
    def load(cls, data: BytesIO):
        version = u8.load(data)
        if version != cls.version:
            raise ValueError("Invalid version")
        edition = u16.load(data)
        program = Program.load(data)
        verifying_keys = Vec[Tuple[Identifier, VerifyingKey, Certificate], u16].load(data)
        return cls(edition=edition, program=program, verifying_keys=verifying_keys)

    @property
    def cost(self) -> tuple[int, int]:
        from node.testnet3 import Testnet3
        storage_cost = len(self.dump()) * Testnet3.deployment_fee_multiplier
        namespace_cost = 10 ** max(0, 10 - len(self.program.id.name.data)) * 1e6
        return storage_cost, namespace_cost


class WitnessCommitments(Serializable):

    def __init__(self, *, w: KZGCommitment, z_a: KZGCommitment, z_b: KZGCommitment):
        self.w = w
        self.z_a = z_a
        self.z_b = z_b

    def dump(self) -> bytes:
        return self.w.dump() + self.z_a.dump() + self.z_b.dump()

    @classmethod
    def load(cls, data: BytesIO):
        w = KZGCommitment.load(data)
        z_a = KZGCommitment.load(data)
        z_b = KZGCommitment.load(data)
        return cls(w=w, z_a=z_a, z_b=z_b)


class Commitments(Serializable):

    def __init__(self, *, witness_commitments: Vec[WitnessCommitments, u64], mask_poly: Option[KZGCommitment],
                 g_1: KZGCommitment, h_1: KZGCommitment, g_a_commitments: Vec[KZGCommitment, u64],
                 g_b_commitments: Vec[KZGCommitment, u64], g_c_commitments: Vec[KZGCommitment, u64], h_2: KZGCommitment):
        self.witness_commitments = witness_commitments
        self.mask_poly = mask_poly
        self.g_1 = g_1
        self.h_1 = h_1
        self.g_a_commitments = g_a_commitments
        self.g_b_commitments = g_b_commitments
        self.g_c_commitments = g_c_commitments
        self.h_2 = h_2

    def dump(self) -> bytes:
        res = b""
        for witness_commitment in self.witness_commitments:
            res += witness_commitment.dump()
        res += self.mask_poly.dump()
        res += self.g_1.dump()
        res += self.h_1.dump()
        for g_a_commitment in self.g_a_commitments:
            res += g_a_commitment.dump()
        for g_b_commitment in self.g_b_commitments:
            res += g_b_commitment.dump()
        for g_c_commitment in self.g_c_commitments:
            res += g_c_commitment.dump()
        res += self.h_2.dump()
        return res

    @classmethod
    def load(cls, data: BytesIO) -> Self:
        raise NotImplementedError("use load_with_batch_sizes instead")

    @classmethod
    def load_with_batch_sizes(cls, data: BytesIO, batch_sizes: Vec[u64, u64]):
        witness_commitments: list[WitnessCommitments] = []
        for _ in range(sum(batch_sizes)):
            witness_commitments.append(WitnessCommitments.load(data))
        witness_commitments = Vec[WitnessCommitments, u64](witness_commitments)
        mask_poly = Option[KZGCommitment].load(data)
        g_1 = KZGCommitment.load(data)
        h_1 = KZGCommitment.load(data)
        commitments: list[KZGCommitment] = []
        for _ in range(len(batch_sizes)):
            commitments.append(KZGCommitment.load(data))
        g_a_commitments = Vec[KZGCommitment, u64](commitments)
        commitments = []
        for _ in range(len(batch_sizes)):
            commitments.append(KZGCommitment.load(data))
        g_b_commitments = Vec[KZGCommitment, u64](commitments)
        commitments = []
        for _ in range(len(batch_sizes)):
            commitments.append(KZGCommitment.load(data))
        g_c_commitments = Vec[KZGCommitment, u64](commitments)
        h_2 = KZGCommitment.load(data)
        return cls(witness_commitments=witness_commitments, mask_poly=mask_poly, g_1=g_1, h_1=h_1,
                   g_a_commitments=g_a_commitments, g_b_commitments=g_b_commitments,
                   g_c_commitments=g_c_commitments, h_2=h_2)


class Evaluations(Serializable):

    def __init__(self, *, z_b_evals: Vec[Vec[Field, u64], u64], g_1_eval: Field, g_a_evals: Vec[Field, u64],
                 g_b_evals: Vec[Field, u64], g_c_evals: Vec[Field, u64]):
        self.z_b_evals = z_b_evals
        self.g_1_eval = g_1_eval
        self.g_a_evals = g_a_evals
        self.g_b_evals = g_b_evals
        self.g_c_evals = g_c_evals

    def dump(self) -> bytes:
        res = b""
        for z_b_eval in self.z_b_evals:
            for e in z_b_eval:
                res += e.dump()
        res += self.g_1_eval.dump()
        for g_a_eval in self.g_a_evals:
            res += g_a_eval.dump()
        for g_b_eval in self.g_b_evals:
            res += g_b_eval.dump()
        for g_c_eval in self.g_c_evals:
            res += g_c_eval.dump()
        return res

    @classmethod
    def load(cls, data: BytesIO) -> Self:
        raise NotImplementedError("use load_with_batch_sizes instead")

    @classmethod
    def load_with_batch_sizes(cls, data: BytesIO, batch_sizes: Vec[u64, u64]):
        z_b_evals: list[Vec[Field, u64]] = []
        for batch_size in batch_sizes:
            batch: list[Field] = []
            for _ in range(batch_size):
                batch.append(Field.load(data))
            z_b_evals.append(Vec[Field, u64](batch))
        g_1_eval = Field.load(data)
        evals: list[Field] = []
        for _ in range(len(batch_sizes)):
            evals.append(Field.load(data))
        g_a_evals = Vec[Field, u64](evals)
        evals = []
        for _ in range(len(batch_sizes)):
            evals.append(Field.load(data))
        g_b_evals = Vec[Field, u64](evals)
        evals = []
        for _ in range(len(batch_sizes)):
            evals.append(Field.load(data))
        g_c_evals = Vec[Field, u64](evals)
        return cls(z_b_evals=Vec[Vec[Field, u64], u64](z_b_evals), g_1_eval=g_1_eval, g_a_evals=g_a_evals, g_b_evals=g_b_evals, g_c_evals=g_c_evals)


class MatrixSums(Serializable):

    def __init__(self, *, sum_a: Field, sum_b: Field, sum_c: Field):
        self.sum_a = sum_a
        self.sum_b = sum_b
        self.sum_c = sum_c

    def dump(self) -> bytes:
        return self.sum_a.dump() + self.sum_b.dump() + self.sum_c.dump()

    @classmethod
    def load(cls, data: BytesIO):
        sum_a = Field.load(data)
        sum_b = Field.load(data)
        sum_c = Field.load(data)
        return cls(sum_a=sum_a, sum_b=sum_b, sum_c=sum_c)


class ThirdMessage(Serializable):

    def __init__(self, *, sums: Vec[MatrixSums, u64]):
        self.sums = sums

    def dump(self) -> bytes:
        return self.sums.dump()

    @classmethod
    def load(cls, data: BytesIO):
        sums = Vec[MatrixSums, u64].load(data)
        return cls(sums=sums)


class Proof(Serializable):
    version = u8()

    # Skipping a layer of marlin::Proof
    def __init__(self, *, batch_sizes: Vec[u64, u64], commitments: Commitments, evaluations: Evaluations, msg: ThirdMessage,
                 pc_proof: BatchLCProof):
        self.batch_sizes = batch_sizes
        self.commitments = commitments
        self.evaluations = evaluations
        self.msg = msg
        self.pc_proof = pc_proof

    def dump(self) -> bytes:
        res = b""
        res += self.version.dump()
        res += self.batch_sizes.dump()
        res += self.commitments.dump()
        res += self.evaluations.dump()
        res += self.msg.dump()
        res += self.pc_proof.dump()
        return res

    @classmethod
    def load(cls, data: BytesIO):
        version = u8.load(data)
        if version != cls.version:
            raise Exception("Invalid proof version")
        batch_sizes = Vec[u64, u64].load(data)
        commitments = Commitments.load_with_batch_sizes(data, batch_sizes=batch_sizes)
        evaluations = Evaluations.load_with_batch_sizes(data, batch_sizes=batch_sizes)
        msg = ThirdMessage.load(data)
        pc_proof = BatchLCProof.load(data)
        return cls(batch_sizes=batch_sizes, commitments=commitments, evaluations=evaluations, msg=msg, pc_proof=pc_proof)

    @classmethod
    def loads(cls, data: str):
        return cls.load(bech32_to_bytes(data))


    def __str__(self):
        return str(Bech32m(self.dump(), "proof"))


class Ciphertext(Serializable):

    def __init__(self, *, ciphertext: Vec[Field, u16]):
        self.ciphertext = ciphertext

    def dump(self) -> bytes:
        return self.ciphertext.dump()

    @classmethod
    def load(cls, data: BytesIO):
        ciphertext = Vec[Field, u16].load(data)
        return cls(ciphertext=ciphertext)

    @classmethod
    def loads(cls, data: str):
        return cls.load(bech32_to_bytes(data))

    def __str__(self):
        return str(Bech32m(self.dump(), "ciphertext"))


class Plaintext(EnumBaseSerialize, RustEnum, Serializable):  # enum

    class Type(IntEnumu8):
        Literal = 0
        Struct = 1

    type: Type

    @classmethod
    def load(cls, data: BytesIO):
        type_ = Plaintext.Type.load(data)
        if type_ == Plaintext.Type.Literal:
            return LiteralPlaintext.load(data)
        elif type_ == Plaintext.Type.Struct:
            return StructPlaintext.load(data)
        else:
            raise ValueError("invalid type")


class LiteralPlaintext(Plaintext):
    type = Plaintext.Type.Literal

    def __init__(self, *, literal: Literal):
        self.literal = literal

    def dump(self) -> bytes:
        return self.type.dump() + self.literal.dump()

    @classmethod
    def load(cls, data: BytesIO):
        literal = Literal.load(data)
        return cls(literal=literal)

    def __str__(self):
        return str(self.literal)

    def __eq__(self, other: object):
        if not isinstance(other, LiteralPlaintext):
            return False
        return self.literal == other.literal

    def __gt__(self, other: Self):
        return self.literal > other.literal

    def __ge__(self, other: Self):
        return self.literal >= other.literal


class StructPlaintext(Plaintext):
    type = Plaintext.Type.Struct

    def __init__(self, *, members: Vec[Tuple[Identifier, Plaintext], u8]):
        self.members = members

    def dump(self) -> bytes:
        res = self.type.dump()
        res += u8(len(self.members)).dump()
        for member in self.members:
            res += member[0].dump()  # Identifier
            num_bytes = member[1].dump()  # Plaintext
            res += u16(len(num_bytes)).dump()
            res += num_bytes
        return res

    @classmethod
    def load(cls, data: BytesIO):
        members: list[Tuple[Identifier, Plaintext]] = []
        num_members = u8.load(data)
        for _ in range(num_members):
            identifier = Identifier.load(data)
            num_bytes = u16.load(data)
            plaintext = Plaintext.load(BytesIO(data.read(num_bytes)))
            members.append(Tuple[Identifier, Plaintext]([identifier, plaintext]))
        return cls(members=Vec[Tuple[Identifier, Plaintext], u8](members))

    @classmethod
    def loads(cls, data: str, struct_type: Struct, struct_types: dict[Identifier, Struct]):
        members: list[Tuple[Identifier, Plaintext]] = []
        identifier_regex = re.compile(r"[a-zA-Z_][a-zA-Z0-9_]*")
        has_begin_brace: bool = False
        has_end_brace: bool = False
        data = data.replace(" ", "")
        def get_identifier(s: str):
            m = identifier_regex.search(s)
            if not m:
                raise ValueError("invalid identifier")
            i = m.group(0)
            if s[len(i)] != ":":
                raise ValueError("invalid identifier")
            return Identifier.loads(i), s[len(i):]
        def get_literal_value(s: str):
            is_comma = True
            comma = s.find(",")
            if comma == -1:
                comma = s.find("}")
                if comma == -1:
                    raise ValueError("invalid literal")
                else:
                    is_comma = False
            return s[:comma], s[comma + int(is_comma):]
        def get_struct_value(s: str):
            if s[0] != "{":
                raise ValueError("invalid struct literal")
            i = s.find("}")
            if i == -1:
                raise ValueError("invalid struct literal")
            is_comma = True
            comma = s.find(",")
            if comma == -1:
                comma = s.find("}")
                if comma == -1:
                    raise ValueError("invalid struct literal")
                else:
                    is_comma = False
            return s[:i], s[i + int(is_comma):]
        while data:
            if not has_begin_brace:
                if data[0] != "{":
                    raise ValueError("opening brace not found")
                data = data[1:]
                has_begin_brace = True
            elif has_end_brace:
                raise ValueError("trailing characters after closing brace")
            elif data[0] == "}":
                data = data[1:]
                has_end_brace = True
            else:
                identifier, data = get_identifier(data)
                if data[0] != ":":
                    raise ValueError("colon not found")
                data = data[1:]
                member_type = struct_type.get_member_type(identifier)
                if isinstance(member_type, LiteralPlaintextType):
                    literal_value, data = get_literal_value(data)
                    primitive_type = member_type.literal_type.primitive_type
                    members.append(
                        Tuple[Identifier, Plaintext](
                            (identifier, LiteralPlaintext(
                                literal=Literal(
                                    type_=Literal.reverse_primitive_type_map[primitive_type],
                                    primitive=primitive_type.loads(literal_value),
                                )
                            ))
                        )
                    )
                elif isinstance(member_type, StructPlaintextType):
                    struct_type = struct_types[member_type.struct]
                    struct_value, data = get_struct_value(data)
                    members.append(
                        Tuple[Identifier, Plaintext](
                            (identifier, StructPlaintext.loads(struct_value, struct_type, struct_types))
                        )
                    )
                else:
                    raise ValueError("invalid type")
        if not has_begin_brace:
            raise ValueError("opening brace not found")
        if not has_end_brace:
            raise ValueError("closing brace not found")
        return cls(members=Vec[Tuple[Identifier, Plaintext], u8](members))


    def __str__(self):
        data = {}
        for identifier, plaintext in self.members:
            data[str(identifier)] = str(plaintext)
        return json.dumps(data).replace('"', '')

    def get_member(self, identifier: Identifier):
        for member in self.members:
            if member[0] == identifier:
                return member[1]
        raise ValueError("Identifier not found")

    def set_member(self, identifier: Identifier, plaintext: Plaintext):
        for i, member in enumerate(self.members):
            if member[0] == identifier:
                self.members[i] = Tuple[Identifier, Plaintext]([identifier, plaintext])
                return
        raise ValueError("Identifier not found")

    def __eq__(self, other: object):
        if not isinstance(other, StructPlaintext):
            return False
        for identifier, plaintext in self.members:
            if plaintext != other.get_member(identifier):
                return False
        return True


@access_generic_type
class Owner(EnumBaseSerialize, RustEnum, Serializable, Generic[T]):
    types: tuple[TType[T]]

    def __init__(self):
        self.Private = self.types[0]

    class Type(IntEnumu8):
        Public = 0
        Private = 1

    @classmethod
    def load(cls, data: BytesIO, *, types: Optional[tuple[TType[T]]] = None):
        if types is None:
            raise ValueError("expected types")
        type_ = Owner.Type.load(data)
        if type_ == Owner.Type.Public:
            return PublicOwner.load(data)
        elif type_ == Owner.Type.Private:
            return PrivateOwner[*types].load(data)
        else:
            raise ValueError("invalid type")


class PublicOwner(Owner):
    type = Owner.Type.Public

    # This subtype is not generic
    # noinspection PyMissingConstructor
    def __init__(self, *, owner: Address):
        self.owner = owner

    def dump(self) -> bytes:
        return self.type.dump() + self.owner.dump()

    # noinspection PyMethodOverriding
    @classmethod
    def load(cls, data: BytesIO):
        owner = Address.load(data)
        return cls(owner=owner)

    def __str__(self):
        return str(self.owner)


@access_generic_type
class PrivateOwner(Owner, Generic[T]):
    types: tuple[TType[T]]
    type = Owner.Type.Private

    # noinspection PyMissingConstructor
    def __init__(self, *, owner: T):
        self.owner = owner

    def dump(self) -> bytes:
        return self.type.dump() + self.owner.dump()

    @classmethod
    def load(cls, data: BytesIO, *, types: Optional[tuple[TType[T]]] = None):
        if types is None:
            raise ValueError("expected types")
        owner = types[0].load(data)
        return cls[*types](owner=owner)

    def __str__(self):
        return str(self.owner)


@access_generic_type
class Entry(EnumBaseSerialize, RustEnum, Serializable, Generic[T]):
    types: tuple[TType[T]]

    def __init__(self):
        self.Private = self.types[0]

    class Type(IntEnumu8):
        Constant = 0
        Public = 1
        Private = 2

    @classmethod
    def load(cls, data: BytesIO, *, types: Optional[tuple[TType[T]]] = None):
        if types is None:
            raise ValueError("expected types")
        type_ = Entry.Type.load(data)
        if type_ == Entry.Type.Constant:
            return ConstantEntry.load(data)
        elif type_ == Entry.Type.Public:
            return PublicEntry.load(data)
        elif type_ == Entry.Type.Private:
            return PrivateEntry[*types].load(data)
        else:
            raise ValueError("invalid type")


class ConstantEntry(Entry):
    type = Entry.Type.Constant

    # noinspection PyMissingConstructor
    def __init__(self, *, plaintext: Plaintext):
        self.plaintext = plaintext

    def dump(self) -> bytes:
        return self.type.dump() + self.plaintext.dump()

    # noinspection PyMethodOverriding
    @classmethod
    def load(cls, data: BytesIO):
        plaintext = Plaintext.load(data)
        return cls(plaintext=plaintext)

    def __str__(self):
        return str(self.plaintext)


class PublicEntry(Entry):
    type = Entry.Type.Public

    # noinspection PyMissingConstructor
    def __init__(self, *, plaintext: Plaintext):
        self.plaintext = plaintext

    def dump(self) -> bytes:
        return self.type.dump() + self.plaintext.dump()

    # noinspection PyMethodOverriding
    @classmethod
    def load(cls, data: BytesIO):
        plaintext = Plaintext.load(data)
        return cls(plaintext=plaintext)

    def __str__(self):
        return str(self.plaintext)


@access_generic_type
class PrivateEntry(Entry, Generic[T]):
    types: tuple[TType[T]]
    type = Entry.Type.Private

    # noinspection PyMissingConstructor
    def __init__(self, *, plaintext: T):
        self.plaintext = plaintext

    def dump(self) -> bytes:
        return self.type.dump() + self.plaintext.dump()

    @classmethod
    def load(cls, data: BytesIO, *, types: Optional[tuple[TType[T]]] = None):
        if types is None:
            raise ValueError("expected types")
        plaintext = types[0].load(data)
        return cls[*types](plaintext=plaintext)

    def __str__(self):
        return str(self.plaintext)


@access_generic_type
class Record(Serializable, Generic[T]):
    types: tuple[TType[T]]

    def __init__(self, *, owner: Owner, data: Vec[Tuple[Identifier, Entry], u8], nonce: Group):
        self.owner = owner
        self.data = data
        self.nonce = nonce

    def dump(self) -> bytes:
        res = b""
        res += self.owner.dump()
        res += u8(len(self.data)).dump()
        for identifier, entry in self.data:
            res += identifier.dump()
            bytes_ = entry.dump()
            res += u16(len(bytes_)).dump()
            res += bytes_
        res += self.nonce.dump()
        return res

    @classmethod
    def load(cls, data: BytesIO, *, types: Optional[tuple[TType[T]]] = None):
        if types is None:
            raise ValueError("expected types")
        Private = types[0]
        owner = Owner[Private].load(data)
        data_len = u8.load(data)
        d: list[Tuple[Identifier, Entry]] = []
        for _ in range(data_len):
            identifier = Identifier.load(data)
            entry_len = u16.load(data)
            entry = Entry[Private].load(BytesIO(data.read(entry_len)))
            d.append(Tuple[Identifier, Entry]([identifier, entry]))
        data_ = Vec[Tuple[Identifier, Entry], u8](d)
        nonce = Group.load(data)
        return cls[*types](owner=owner, data=data_, nonce=nonce)

    @classmethod
    def loads(cls, data: str, *, types: Optional[tuple[TType[T]]] = None):
        if types is None:
            raise ValueError("expected types")
        return cls[*types].load(bech32_to_bytes(data))

    def __str__(self):
        return str(Bech32m(self.dump(), "record"))


class Value(EnumBaseSerialize, RustEnum, Serializable):

    class Type(IntEnumu8):
        Plaintext = 0
        Record = 1

    @classmethod
    def load(cls, data: BytesIO):
        type_ = Value.Type.load(data)
        if type_ == Value.Type.Plaintext:
            return PlaintextValue.load(data)
        elif type_ == Value.Type.Record:
            return RecordValue.load(data)
        else:
            raise ValueError("unknown value type")


class PlaintextValue(Value):
    type = Value.Type.Plaintext

    def __init__(self, *, plaintext: Plaintext):
        self.plaintext = plaintext

    def dump(self) -> bytes:
        return self.type.dump() + self.plaintext.dump()

    @classmethod
    def load(cls, data: BytesIO):
        plaintext = Plaintext.load(data)
        return cls(plaintext=plaintext)

    def __str__(self):
        return str(self.plaintext)


class RecordValue(Value):
    type = Value.Type.Record

    def __init__(self, *, record: Record[Plaintext]):
        if record.Private != Plaintext:
            raise ValueError("record must be of type Record[Plaintext]")
        self.record = record

    def dump(self) -> bytes:
        return self.type.dump() + self.record.dump()

    @classmethod
    def load(cls, data: BytesIO):
        record = Record[Plaintext].load(data)
        return cls(record=record)


class TransitionInput(EnumBaseSerialize, RustEnum, Serializable):

    class Type(IntEnumu8):
        Constant = 0
        Public = 1
        Private = 2
        Record = 3
        ExternalRecord = 4

    type: Type

    @classmethod
    def load(cls, data: BytesIO):
        type_ = TransitionInput.Type.load(data)
        if type_ == TransitionInput.Type.Constant:
            return ConstantTransitionInput.load(data)
        elif type_ == TransitionInput.Type.Public:
            return PublicTransitionInput.load(data)
        elif type_ == TransitionInput.Type.Private:
            return PrivateTransitionInput.load(data)
        elif type_ == TransitionInput.Type.Record:
            return RecordTransitionInput.load(data)
        elif type_ == TransitionInput.Type.ExternalRecord:
            return ExternalRecordTransitionInput.load(data)
        else:
            raise ValueError("unknown transition input type")

class ConstantTransitionInput(TransitionInput):
    type = TransitionInput.Type.Constant

    def __init__(self, *, plaintext_hash: Field, plaintext: Option[Plaintext]):
        self.plaintext_hash = plaintext_hash
        self.plaintext = plaintext

    def dump(self) -> bytes:
        return self.type.dump() + self.plaintext_hash.dump() + self.plaintext.dump()

    @classmethod
    def load(cls, data: BytesIO):
        plaintext_hash = Field.load(data)
        plaintext = Option[Plaintext].load(data)
        return cls(plaintext_hash=plaintext_hash, plaintext=plaintext)


class PublicTransitionInput(TransitionInput):
    type = TransitionInput.Type.Public

    def __init__(self, *, plaintext_hash: Field, plaintext: Option[Plaintext]):
        self.plaintext_hash = plaintext_hash
        self.plaintext = plaintext

    def dump(self) -> bytes:
        return self.type.dump() + self.plaintext_hash.dump() + self.plaintext.dump()

    @classmethod
    def load(cls, data: BytesIO):
        plaintext_hash = Field.load(data)
        plaintext = Option[Plaintext].load(data)
        return cls(plaintext_hash=plaintext_hash, plaintext=plaintext)


class PrivateTransitionInput(TransitionInput):
    type = TransitionInput.Type.Private

    def __init__(self, *, ciphertext_hash: Field, ciphertext: Option[Ciphertext]):
        self.ciphertext_hash = ciphertext_hash
        self.ciphertext = ciphertext

    def dump(self) -> bytes:
        return self.type.dump() + self.ciphertext_hash.dump() + self.ciphertext.dump()

    @classmethod
    def load(cls, data: BytesIO):
        ciphertext_hash = Field.load(data)
        ciphertext = Option[Ciphertext].load(data)
        return cls(ciphertext_hash=ciphertext_hash, ciphertext=ciphertext)


class RecordTransitionInput(TransitionInput):
    type = TransitionInput.Type.Record

    def __init__(self, *, serial_number: Field, tag: Field):
        self.serial_number = serial_number
        self.tag = tag

    def dump(self) -> bytes:
        return self.type.dump() + self.serial_number.dump() + self.tag.dump()

    @classmethod
    def load(cls, data: BytesIO):
        serial_number = Field.load(data)
        tag = Field.load(data)
        return cls(serial_number=serial_number, tag=tag)


class ExternalRecordTransitionInput(TransitionInput):
    type = TransitionInput.Type.ExternalRecord

    def __init__(self, *, input_commitment: Field):
        self.input_commitment = input_commitment

    def dump(self) -> bytes:
        return self.type.dump() + self.input_commitment.dump()

    @classmethod
    def load(cls, data: BytesIO):
        input_commitment = Field.load(data)
        return cls(input_commitment=input_commitment)


class TransitionOutput(EnumBaseSerialize, RustEnum, Serializable):

    class Type(IntEnumu8):
        Constant = 0
        Public = 1
        Private = 2
        Record = 3
        ExternalRecord = 4

    type: Type

    @classmethod
    def load(cls, data: BytesIO):
        type_ = TransitionOutput.Type.load(data)
        if type_ == TransitionOutput.Type.Constant:
            return ConstantTransitionOutput.load(data)
        elif type_ == TransitionOutput.Type.Public:
            return PublicTransitionOutput.load(data)
        elif type_ == TransitionOutput.Type.Private:
            return PrivateTransitionOutput.load(data)
        elif type_ == TransitionOutput.Type.Record:
            return RecordTransitionOutput.load(data)
        elif type_ == TransitionOutput.Type.ExternalRecord:
            return ExternalRecordTransitionOutput.load(data)
        else:
            raise ValueError("unknown transition output type")


class ConstantTransitionOutput(TransitionOutput):
    type = TransitionOutput.Type.Constant

    def __init__(self, *, plaintext_hash: Field, plaintext: Option[Plaintext]):
        self.plaintext_hash = plaintext_hash
        self.plaintext = plaintext

    def dump(self) -> bytes:
        return self.type.dump() + self.plaintext_hash.dump() + self.plaintext.dump()

    @classmethod
    def load(cls, data: BytesIO):
        plaintext_hash = Field.load(data)
        plaintext = Option[Plaintext].load(data)
        return cls(plaintext_hash=plaintext_hash, plaintext=plaintext)


class PublicTransitionOutput(TransitionOutput):
    type = TransitionOutput.Type.Public

    def __init__(self, *, plaintext_hash: Field, plaintext: Option[Plaintext]):
        self.plaintext_hash = plaintext_hash
        self.plaintext = plaintext

    def dump(self) -> bytes:
        return self.type.dump() + self.plaintext_hash.dump() + self.plaintext.dump()

    @classmethod
    def load(cls, data: BytesIO):
        plaintext_hash = Field.load(data)
        plaintext = Option[Plaintext].load(data)
        return cls(plaintext_hash=plaintext_hash, plaintext=plaintext)


class PrivateTransitionOutput(TransitionOutput):
    type = TransitionOutput.Type.Private

    def __init__(self, *, ciphertext_hash: Field, ciphertext: Option[Ciphertext]):
        self.ciphertext_hash = ciphertext_hash
        self.ciphertext = ciphertext

    def dump(self) -> bytes:
        return self.type.dump() + self.ciphertext_hash.dump() + self.ciphertext.dump()

    @classmethod
    def load(cls, data: BytesIO):
        ciphertext_hash = Field.load(data)
        ciphertext = Option[Ciphertext].load(data)
        return cls(ciphertext_hash=ciphertext_hash, ciphertext=ciphertext)


class RecordTransitionOutput(TransitionOutput):
    type = TransitionOutput.Type.Record

    def __init__(self, *, commitment: Field, checksum: Field, record_ciphertext: Option[Record[Ciphertext]]):
        self.commitment = commitment
        self.checksum = checksum
        self.record_ciphertext = record_ciphertext

    def dump(self) -> bytes:
        return self.type.dump() + self.commitment.dump() + self.checksum.dump() + self.record_ciphertext.dump()

    @classmethod
    def load(cls, data: BytesIO):
        commitment = Field.load(data)
        checksum = Field.load(data)
        record_ciphertext = Option[Record[Ciphertext]].load(data)
        return cls(commitment=commitment, checksum=checksum, record_ciphertext=record_ciphertext)


class ExternalRecordTransitionOutput(TransitionOutput):
    type = TransitionOutput.Type.ExternalRecord

    def __init__(self, *, commitment: Field):
        self.commitment = commitment

    def dump(self) -> bytes:
        return self.type.dump() + self.commitment.dump()

    @classmethod
    def load(cls, data: BytesIO):
        commitment = Field.load(data)
        return cls(commitment=commitment)


class Transition(Serializable):
    version = u8()

    def __init__(self, *, id_: TransitionID, program_id: ProgramID, function_name: Identifier,
                 inputs: Vec[TransitionInput, u8], outputs: Vec[TransitionOutput, u8], finalize: Option[Vec[Value, u8]],
                 tpk: Group, tcm: Field):
        self.id = id_
        self.program_id = program_id
        self.function_name = function_name
        self.inputs = inputs
        self.outputs = outputs
        self.finalize = finalize
        self.tpk = tpk
        self.tcm = tcm

    def dump(self) -> bytes:
        res = b""
        res += self.version.dump()
        res += self.id.dump()
        res += self.program_id.dump()
        res += self.function_name.dump()
        res += self.inputs.dump()
        res += self.outputs.dump()
        res += self.finalize.dump()
        res += self.tpk.dump()
        res += self.tcm.dump()
        return res

    @classmethod
    def load(cls, data: BytesIO):
        version = u8.load(data)
        if version != cls.version:
            raise ValueError(f"version mismatch: expected {cls.version}, got {version}")
        id_ = TransitionID.load(data)
        program_id = ProgramID.load(data)
        function_name = Identifier.load(data)
        inputs = Vec[TransitionInput, u8].load(data)
        outputs = Vec[TransitionOutput, u8].load(data)
        finalize = Option[Vec[Value, u8]].load(data)
        tpk = Group.load(data)
        tcm = Field.load(data)
        return cls(id_=id_, program_id=program_id, function_name=function_name, inputs=inputs, outputs=outputs,
                   finalize=finalize, tpk=tpk, tcm=tcm)


class Fee(Serializable):
    version = u8()

    def __init__(self, *, transition: Transition, global_state_root: StateRoot, proof: Option[Proof]):
        self.transition = transition
        self.global_state_root = global_state_root
        self.proof = proof

    def dump(self) -> bytes:
        res = b""
        res += self.version.dump()
        res += self.transition.dump()
        res += self.global_state_root.dump()
        res += self.proof.dump()
        return res

    @classmethod
    def load(cls, data: BytesIO):
        version = u8.load(data)
        if version != cls.version:
            raise ValueError(f"version mismatch: expected {cls.version}, got {version}")
        transition = Transition.load(data)
        global_state_root = StateRoot.load(data)
        proof = Option[Proof].load(data)
        return cls(transition=transition, global_state_root=global_state_root, proof=proof)


class Execution(Serializable):
    version = u8()

    def __init__(self, *, transitions: Vec[Transition, u8], global_state_root: StateRoot,
                 proof: Option[Proof]):
        self.transitions = transitions
        self.global_state_root = global_state_root
        self.proof = proof

    def dump(self) -> bytes:
        res = b""
        res += self.version.dump()
        res += self.transitions.dump()
        res += self.global_state_root.dump()
        res += self.proof.dump()
        return res

    @classmethod
    def load(cls, data: BytesIO):
        version = u8.load(data)
        if version != cls.version:
            raise ValueError(f"version mismatch: expected {cls.version}, got {version}")
        transitions = Vec[Transition, u8].load(data)
        global_state_root = StateRoot.load(data)
        proof = Option[Proof].load(data)
        return cls(transitions=transitions, global_state_root=global_state_root, proof=proof)

    @property
    def is_free_execution(self):
        if len(self.transitions) != 1:
            return False
        transition: Transition = self.transitions[0]
        if transition.program_id != "credits.aleo":
            return False
        if str(transition.function_name) in ["mint", "fee", "split"]:
            return True
        return False

    @property
    def cost(self) -> tuple[int, int]:
        if self.is_free_execution:
            return 0, 0
        storage_cost = len(self.dump())
        # we can't get the finalize cost without the program, and we don't have database here,
        # plus we want to give a detailed breakdown, so we just return -1
        return storage_cost, -1


class Transaction(EnumBaseSerialize, RustEnum, Serializable):
    version = u8()

    class Type(IntEnumu8):
        Deploy = 0
        Execute = 1
        Fee = 2

    @classmethod
    def load(cls, data: BytesIO):
        if data.getbuffer().nbytes < 1:
            raise ValueError("incorrect length")
        version = u8.load(data)
        type_ = cls.Type.load(data)
        if type_ == cls.Type.Deploy:
            if version != DeployTransaction.version:
                raise ValueError("incorrect version")
            return DeployTransaction.load(data)
        elif type_ == cls.Type.Execute:
            if version != ExecuteTransaction.version:
                raise ValueError("incorrect version")
            return ExecuteTransaction.load(data)
        elif type_ == cls.Type.Fee:
            if version != FeeTransaction.version:
                raise ValueError("incorrect version")
            return FeeTransaction.load(data)
        else:
            raise ValueError("incorrect type")


class ProgramOwner(Serializable):
    version = u8()

    def __init__(self, *, address: Address, signature: "Signature"):
        self.address = address
        self.signature = signature

    def dump(self) -> bytes:
        return self.version.dump() + self.address.dump() + self.signature.dump()

    @classmethod
    def load(cls, data: BytesIO):
        version = u8.load(data)
        if version != cls.version:
            raise ValueError(f"version mismatch: expected {cls.version}, got {version}")
        address = Address.load(data)
        signature = Signature.load(data)
        return cls(address=address, signature=signature)


class DeployTransaction(Transaction):
    type = Transaction.Type.Deploy

    def __init__(self, *, id_: TransactionID, owner: ProgramOwner, deployment: Deployment, fee: Fee):
        self.id = id_
        self.owner = owner
        self.deployment = deployment
        self.fee = fee

    def dump(self) -> bytes:
        return self.version.dump() + self.type.dump() + self.id.dump() \
            + self.owner.dump() + self.deployment.dump() + self.fee.dump()

    @classmethod
    def load(cls, data: BytesIO):
        id_ = TransactionID.load(data)
        owner = ProgramOwner.load(data)
        deployment = Deployment.load(data)
        fee = Fee.load(data)
        return cls(id_=id_, owner=owner, deployment=deployment, fee=fee)


class ExecuteTransaction(Transaction):
    type = Transaction.Type.Execute

    def __init__(self, *, id_: TransactionID, execution: Execution, additional_fee: Option[Fee]):
        self.id = id_
        self.execution = execution
        self.additional_fee = additional_fee

    def dump(self) -> bytes:
        return self.version.dump() + self.type.dump() + self.id.dump() + self.execution.dump() + self.additional_fee.dump()

    @classmethod
    def load(cls, data: BytesIO):
        id_ = TransactionID.load(data)
        execution = Execution.load(data)
        additional_fee = Option[Fee].load(data)
        return cls(id_=id_, execution=execution, additional_fee=additional_fee)

class FeeTransaction(Transaction):
    type = Transaction.Type.Fee

    def __init__(self, *, id_: TransactionID, fee: Fee):
        self.id = id_
        self.fee = fee

    def dump(self) -> bytes:
        return self.version.dump() + self.type.dump() + self.id.dump() + self.fee.dump()

    @classmethod
    def load(cls, data: BytesIO):
        id_ = TransactionID.load(data)
        fee = Fee.load(data)
        return cls(id_=id_, fee=fee)

class ConfirmedTransaction(EnumBaseSerialize, RustEnum, Serializable):
    class Type(IntEnumu8):
        AcceptedDeploy = 0
        AcceptedExecute = 1
        RejectedDeploy = 2
        RejectedExecute = 3

    type: Type

    @classmethod
    def load(cls, data: BytesIO):
        type_ = cls.Type.load(data)
        if type_ == cls.Type.AcceptedDeploy:
            return AcceptedDeploy.load(data)
        elif type_ == cls.Type.AcceptedExecute:
            return AcceptedExecute.load(data)
        elif type_ == cls.Type.RejectedDeploy:
            return RejectedDeploy.load(data)
        elif type_ == cls.Type.RejectedExecute:
            return RejectedExecute.load(data)
        else:
            raise ValueError("incorrect type")


class FinalizeOperation(EnumBaseSerialize, RustEnum, Serializable):
    class Type(IntEnumu8):
        InitializeMapping = 0
        InsertKeyValue = 1
        UpdateKeyValue = 2
        RemoveKeyValue = 3
        RemoveMapping = 4

    type: Type
    mapping_id: Field

    @classmethod
    def load(cls, data: BytesIO):
        type_ = cls.Type.load(data)
        if type_ == cls.Type.InitializeMapping:
            return InitializeMapping.load(data)
        elif type_ == cls.Type.InsertKeyValue:
            return InsertKeyValue.load(data)
        elif type_ == cls.Type.UpdateKeyValue:
            return UpdateKeyValue.load(data)
        elif type_ == cls.Type.RemoveKeyValue:
            return RemoveKeyValue.load(data)
        elif type_ == cls.Type.RemoveMapping:
            return RemoveMapping.load(data)
        else:
            raise ValueError("incorrect type")


class InitializeMapping(FinalizeOperation):
    type = FinalizeOperation.Type.InitializeMapping

    def __init__(self, *, mapping_id: Field):
        self.mapping_id = mapping_id

    def dump(self) -> bytes:
        return self.type.dump() + self.mapping_id.dump()

    @classmethod
    def load(cls, data: BytesIO):
        mapping_id = Field.load(data)
        return cls(mapping_id=mapping_id)


class InsertKeyValue(FinalizeOperation):
    type = FinalizeOperation.Type.InsertKeyValue

    def __init__(self, *, mapping_id: Field, key_id: Field, value_id: Field):
        self.mapping_id = mapping_id
        self.key_id = key_id
        self.value_id = value_id

    def dump(self) -> bytes:
        return self.type.dump() + self.mapping_id.dump() + self.key_id.dump() + self.value_id.dump()

    @classmethod
    def load(cls, data: BytesIO):
        mapping_id = Field.load(data)
        key_id = Field.load(data)
        value_id = Field.load(data)
        return cls(mapping_id=mapping_id, key_id=key_id, value_id=value_id)


class UpdateKeyValue(FinalizeOperation):
    type = FinalizeOperation.Type.UpdateKeyValue

    def __init__(self, *, mapping_id: Field, index: u64, key_id: Field, value_id: Field):
        self.mapping_id = mapping_id
        self.index = index
        self.key_id = key_id
        self.value_id = value_id

    def dump(self) -> bytes:
        return self.type.dump() + self.mapping_id.dump() + self.index.dump() + self.key_id.dump() + self.value_id.dump()

    @classmethod
    def load(cls, data: BytesIO):
        mapping_id = Field.load(data)
        index = u64.load(data)
        key_id = Field.load(data)
        value_id = Field.load(data)
        return cls(mapping_id=mapping_id, index=index, key_id=key_id, value_id=value_id)


class RemoveKeyValue(FinalizeOperation):
    type = FinalizeOperation.Type.RemoveKeyValue

    def __init__(self, *, mapping_id: Field, index: u64):
        self.mapping_id = mapping_id
        self.index = index

    def dump(self) -> bytes:
        return self.type.dump() + self.mapping_id.dump() + self.index.dump()

    @classmethod
    def load(cls, data: BytesIO):
        mapping_id = Field.load(data)
        index = u64.load(data)
        return cls(mapping_id=mapping_id, index=index)


class RemoveMapping(FinalizeOperation):
    type = FinalizeOperation.Type.RemoveMapping

    def __init__(self, *, mapping_id: Field):
        self.mapping_id = mapping_id

    def dump(self) -> bytes:
        return self.type.dump() + self.mapping_id.dump()

    @classmethod
    def load(cls, data: BytesIO):
        mapping_id = Field.load(data)
        return cls(mapping_id=mapping_id)


class AcceptedDeploy(ConfirmedTransaction):
    type = ConfirmedTransaction.Type.AcceptedDeploy

    def __init__(self, *, index: u32, transaction: Transaction, finalize: Vec[FinalizeOperation, u16]):
        self.index = index
        self.transaction = transaction
        self.finalize = finalize

    def dump(self) -> bytes:
        return self.type.dump() + self.index.dump() + self.transaction.dump() + self.finalize.dump()

    @classmethod
    def load(cls, data: BytesIO):
        index = u32.load(data)
        transaction = Transaction.load(data)
        finalize = Vec[FinalizeOperation, u16].load(data)
        return cls(index=index, transaction=transaction, finalize=finalize)


class AcceptedExecute(ConfirmedTransaction):
    type = ConfirmedTransaction.Type.AcceptedExecute

    def __init__(self, *, index: u32, transaction: Transaction, finalize: Vec[FinalizeOperation, u16]):
        self.index = index
        self.transaction = transaction
        self.finalize = finalize

    def dump(self) -> bytes:
        return self.type.dump() + self.index.dump() + self.transaction.dump() + self.finalize.dump()

    @classmethod
    def load(cls, data: BytesIO):
        index = u32.load(data)
        transaction = Transaction.load(data)
        finalize = Vec[FinalizeOperation, u16].load(data)
        return cls(index=index, transaction=transaction, finalize=finalize)

class Rejected(EnumBaseSerialize, RustEnum, Serializable):
    class Type(IntEnumu8):
        Deployment = 0
        Execution = 1

    @classmethod
    def load(cls, data: BytesIO):
        type_ = cls.Type.load(data)
        if type_ == cls.Type.Deployment:
            return RejectedDeployment.load(data)
        elif type_ == cls.Type.Execution:
            return RejectedExecution.load(data)
        else:
            raise ValueError("Invalid Rejected Type")

class RejectedDeployment(Rejected):
    type = Rejected.Type.Deployment

    def __init__(self, *, program_owner: ProgramOwner, deploy: Deployment):
        self.program_owner = program_owner
        self.deploy = deploy

    def dump(self) -> bytes:
        return self.type.dump() + self.program_owner.dump() + self.deploy.dump()

    @classmethod
    def load(cls, data: BytesIO):
        program_owner = ProgramOwner.load(data)
        deploy = Deployment.load(data)
        return cls(program_owner=program_owner, deploy=deploy)

class RejectedExecution(Rejected):
    type = Rejected.Type.Execution

    def __init__(self, *, execution: Execution):
        self.execution = execution

    def dump(self) -> bytes:
        return self.type.dump() + self.execution.dump()

    @classmethod
    def load(cls, data: BytesIO):
        execution = Execution.load(data)
        return cls(execution=execution)

class RejectedDeploy(ConfirmedTransaction):
    type = ConfirmedTransaction.Type.RejectedDeploy

    def __init__(self, *, index: u32, transaction: Transaction, rejected: Rejected):
        self.index = index
        self.transaction = transaction
        self.rejected = rejected

    def dump(self) -> bytes:
        return self.type.dump() + self.index.dump() + self.transaction.dump() + self.rejected.dump()

    @classmethod
    def load(cls, data: BytesIO):
        index = u32.load(data)
        transaction = Transaction.load(data)
        rejected = Rejected.load(data)
        return cls(index=index, transaction=transaction, rejected=rejected)


class RejectedExecute(ConfirmedTransaction):
    type = ConfirmedTransaction.Type.RejectedExecute

    def __init__(self, *, index: u32, transaction: Transaction, rejected: Rejected):
        self.index = index
        self.transaction = transaction
        self.rejected = rejected

    def dump(self) -> bytes:
        return self.type.dump() + self.index.dump() + self.transaction.dump() + self.rejected.dump()

    @classmethod
    def load(cls, data: BytesIO):
        index = u32.load(data)
        transaction = Transaction.load(data)
        rejected = Rejected.load(data)
        return cls(index=index, transaction=transaction, rejected=rejected)


class Transactions(Serializable):
    version = u8()

    def __init__(self, *, transactions: Vec[ConfirmedTransaction, u32]):  # we probably don't need IDs here so using Vec
        self.transactions = transactions

    def dump(self) -> bytes:
        return self.version.dump() + self.transactions.dump()

    @classmethod
    def load(cls, data: BytesIO):
        version = u8.load(data)
        if version != cls.version:
            raise ValueError("invalid transactions version")
        # noinspection PyArgumentList
        transactions = Vec[ConfirmedTransaction, u32].load(data)
        return cls(transactions=transactions)

    def __iter__(self):
        return iter(self.transactions)


class BlockHeaderMetadata(Serializable):
    version = u8()

    def __init__(self, *, network: u16, round_: u64, height: u32, total_supply_in_microcredits: u64,
                 cumulative_weight: u128, cumulative_proof_target: u128, coinbase_target: u64, proof_target: u64,
                 last_coinbase_target: u64, last_coinbase_timestamp: i64, timestamp: i64):
        self.network = network
        self.round = round_
        self.height = height
        self.total_supply_in_microcredits = total_supply_in_microcredits
        self.cumulative_weight = cumulative_weight
        self.cumulative_proof_target = cumulative_proof_target
        self.coinbase_target = coinbase_target
        self.proof_target = proof_target
        self.last_coinbase_target = last_coinbase_target
        self.last_coinbase_timestamp = last_coinbase_timestamp
        self.timestamp = timestamp

    def dump(self) -> bytes:
        return self.version.dump() + self.network.dump() + self.round.dump() + self.height.dump() + \
               self.total_supply_in_microcredits.dump() + self.cumulative_weight.dump() + \
               self.cumulative_proof_target.dump() + self.coinbase_target.dump() + self.proof_target.dump() + \
               self.last_coinbase_target.dump() + self.last_coinbase_timestamp.dump() + self.timestamp.dump()


    @classmethod
    def load(cls, data: BytesIO):
        version = u8.load(data)
        if version != cls.version:
            raise ValueError("invalid metadata version")
        network = u16.load(data)
        round_ = u64.load(data)
        height = u32.load(data)
        total_supply_in_microcredits = u64.load(data)
        cumulative_weight = u128.load(data)
        cumulative_proof_target = u128.load(data)
        coinbase_target = u64.load(data)
        proof_target = u64.load(data)
        last_coinbase_target = u64.load(data)
        last_coinbase_timestamp = i64.load(data)
        timestamp = i64.load(data)
        return cls(network=network, round_=round_, height=height,
                   total_supply_in_microcredits=total_supply_in_microcredits,
                   cumulative_weight=cumulative_weight,
                   cumulative_proof_target=cumulative_proof_target,
                   coinbase_target=coinbase_target, proof_target=proof_target,
                   last_coinbase_target=last_coinbase_target,
                   last_coinbase_timestamp=last_coinbase_timestamp, timestamp=timestamp)


class BlockHeader(Serializable):
    version = u8()

    def __init__(self, *, previous_state_root: Field, transactions_root: Field, coinbase_accumulator_point: Field,
                 finalize_root: Field, ratifications_root: Field, metadata: BlockHeaderMetadata):
        self.previous_state_root = previous_state_root
        self.transactions_root = transactions_root
        self.finalize_root = finalize_root
        self.ratifications_root = ratifications_root
        self.coinbase_accumulator_point = coinbase_accumulator_point
        self.metadata = metadata

    def dump(self) -> bytes:
        return self.version.dump() + self.previous_state_root.dump() + self.transactions_root.dump() \
               + self.finalize_root.dump() + self.ratifications_root.dump() + self.coinbase_accumulator_point.dump() \
               + self.metadata.dump()

    @classmethod
    def load(cls, data: BytesIO):
        version = u8.load(data)
        previous_state_root = Field.load(data)
        transactions_root = Field.load(data)
        finalize_root = Field.load(data)
        ratifications_root = Field.load(data)
        coinbase_accumulator_point = Field.load(data)
        metadata = BlockHeaderMetadata.load(data)
        if version != cls.version:
            raise ValueError("invalid header version")
        return cls(previous_state_root=previous_state_root, transactions_root=transactions_root,
                   finalize_root=finalize_root, ratifications_root=ratifications_root,
                   coinbase_accumulator_point=coinbase_accumulator_point, metadata=metadata)


class PuzzleCommitment(Serializable):

    def __init__(self, *, commitment: KZGCommitment):
        self.commitment = commitment

    def dump(self) -> bytes:
        return self.commitment.dump()

    @classmethod
    def load(cls, data: BytesIO):
        commitment = KZGCommitment.load(data)
        return cls(commitment=commitment)

    @classmethod
    def loads(cls, data: str):
        return cls.load(bech32_to_bytes(data))

    def to_target(self) -> int:
        return (2 ** 64 - 1) // int.from_bytes(sha256(sha256(self.dump()).digest()).digest()[:8], byteorder='little')

    def __str__(self):
        return str(Bech32m(self.dump(), "puzzle"))


class PartialSolution(Serializable):

    def __init__(self, *, address: Address, nonce: u64, commitment: PuzzleCommitment):
        self.address = address
        self.nonce = nonce
        self.commitment = commitment

    def dump(self) -> bytes:
        return self.address.dump() + self.nonce.dump() + self.commitment.dump()

    @classmethod
    def load(cls, data: BytesIO):
        address = Address.load(data)
        nonce = u64.load(data)
        commitment = PuzzleCommitment.load(data)
        return cls(address=address, nonce=nonce, commitment=commitment)

    def __hash__(self):
        return hash(self.nonce)


PuzzleProof = KZGProof


class ProverSolution(Serializable):

    def __init__(self, *, partial_solution: PartialSolution, proof: PuzzleProof):
        self.partial_solution = partial_solution
        self.proof = proof

    def dump(self) -> bytes:
        return self.partial_solution.dump() + self.proof.dump()

    @classmethod
    def load(cls, data: BytesIO):
        partial_solution = PartialSolution.load(data)
        proof = PuzzleProof.load(data)
        return cls(partial_solution=partial_solution, proof=proof)


class CoinbaseSolution(Serializable):

    def __init__(self, *, partial_solutions: Vec[PartialSolution, u32], proof: PuzzleProof):
        self.partial_solutions = partial_solutions
        self.proof = proof

    def dump(self) -> bytes:
        return self.partial_solutions.dump() + self.proof.dump()

    @classmethod
    def load(cls, data: BytesIO):
        partial_solutions = Vec[PartialSolution, u32].load(data)
        proof = PuzzleProof.load(data)
        return cls(partial_solutions=partial_solutions, proof=proof)


class ComputeKey(Serializable):

    def __init__(self, *, pk_sig: Group, pr_sig: Group):
        self.pk_sig = pk_sig
        self.pr_sig = pr_sig

    def dump(self) -> bytes:
        return self.pk_sig.dump() + self.pr_sig.dump()

    @classmethod
    def load(cls, data: BytesIO):
        pk_sig = Group.load(data)
        pr_sig = Group.load(data)
        return cls(pk_sig=pk_sig, pr_sig=pr_sig)


class Signature(Serializable):

    def __init__(self, *, challange: Scalar, response: Scalar, compute_key: ComputeKey):
        self.challange = challange
        self.response = response
        self.compute_key = compute_key

    def dump(self) -> bytes:
        return self.challange.dump() + self.response.dump() + self.compute_key.dump()

    @classmethod
    def load(cls, data: BytesIO):
        challange = Scalar.load(data)
        response = Scalar.load(data)
        compute_key = ComputeKey.load(data)
        return cls(challange=challange, response=response, compute_key=compute_key)

    @classmethod
    def loads(cls, data: str):
        return cls.load(bech32_to_bytes(data))

    def __str__(self):
        return str(Bech32m(self.dump(), "sign"))


class Ratify(EnumBaseSerialize, RustEnum, Serializable):

    class Type(IntEnumu8):
        ProvingReward = 0
        StakingReward = 1

    version = 0
    address: Address
    amount: u64
    type: Type

    @classmethod
    def load(cls, data: BytesIO):
        version = u8.load(data)
        if version != cls.version:
            raise ValueError(f"invalid version {version}")
        type_ = cls.Type.load(data)
        if type_ == cls.Type.ProvingReward:
            return ProvingReward.load(data)
        elif type_ == cls.Type.StakingReward:
            return StakingReward.load(data)
        else:
            raise ValueError(f"invalid ratify type {type_}")

class ProvingReward(Ratify):
    type = Ratify.Type.ProvingReward

    def __init__(self, *, address: Address, amount: u64):
        self.address = address
        self.amount = amount

    def dump(self) -> bytes:
        return self.address.dump() + self.amount.dump()

    @classmethod
    def load(cls, data: BytesIO):
        address = Address.load(data)
        amount = u64.load(data)
        return cls(address=address, amount=amount)

class StakingReward(Ratify):
    type = Ratify.Type.StakingReward

    def __init__(self, *, address: Address, amount: u64):
        self.address = address
        self.amount = amount

    def dump(self) -> bytes:
        return self.address.dump() + self.amount.dump()

    @classmethod
    def load(cls, data: BytesIO):
        address = Address.load(data)
        amount = u64.load(data)
        return cls(address=address, amount=amount)


def retarget(prev_target: int, prev_block_timestamp: int, block_timestamp: int, half_life: int, inverse: bool, anchor_time: int):
    drift = max(block_timestamp - prev_block_timestamp, 1) - anchor_time
    if drift == 0:
        return prev_target
    if inverse:
        drift = -drift
    exponent = int((1 << 16) * drift / half_life)
    integral = exponent >> 16
    fractional = exponent - (integral << 16)
    fractional_multiplier = (1 << 16) + ((195_766_423_245_049 * fractional + 971_821_376 * pow(fractional, 2) + 5_127 * pow(fractional, 3) + pow(2, 47)) >> 48)
    candidate_target = prev_target * fractional_multiplier
    shifts = integral - 16
    if shifts < 0:
        candidate_target = max(candidate_target >> -shifts, 1)
    else:
        candidate_target = max(candidate_target << shifts, 1)
    candidate_target = min(candidate_target, 2 ** 64 - 1)
    return candidate_target


class Block(Serializable):
    version = u8()

    def __init__(self, *, block_hash: BlockHash, previous_hash: BlockHash, header: BlockHeader,
                 transactions: Transactions, ratifications: Vec[Ratify, u32], coinbase: Option[CoinbaseSolution],
                 signature: Signature):
        self.block_hash = block_hash
        self.previous_hash = previous_hash
        self.header = header
        self.transactions = transactions
        self.ratifications = ratifications
        self.coinbase = coinbase
        self.signature = signature

    def dump(self) -> bytes:
        return self.version.dump() + self.block_hash.dump() + self.previous_hash.dump() + self.header.dump() \
               + self.transactions.dump() + self.ratifications.dump() + self.coinbase.dump() + self.signature.dump()

    @classmethod
    def load(cls, data: BytesIO):
        version = u8.load(data)
        block_hash = BlockHash.load(data)
        previous_hash = BlockHash.load(data)
        header = BlockHeader.load(data)
        transactions = Transactions.load(data)
        ratifications = Vec[Ratify, u32].load(data)
        coinbase = Option[CoinbaseSolution].load(data)
        signature = Signature.load(data)
        if version != cls.version:
            raise ValueError("invalid block version")
        return cls(block_hash=block_hash, previous_hash=previous_hash, header=header, transactions=transactions,
                   ratifications=ratifications, coinbase=coinbase, signature=signature)


    def __str__(self):
        return f"Block {self.header.metadata.height} ({str(self.block_hash)[:16]}...)"

    def get_coinbase_reward(self, last_timestamp: int) -> int:
        if self.coinbase.value is None:
            return 0
        anchor_reward = 18
        y10_anchor_height = 31536000 // 25 * 10
        remaining_blocks = y10_anchor_height - self.header.metadata.height
        if remaining_blocks <= 0:
            return 0
        return retarget(remaining_blocks * anchor_reward, last_timestamp, self.header.metadata.timestamp, 25, True, 25)

    def get_epoch_number(self) -> int:
        return self.header.metadata.height // 256

    @property
    def height(self) -> u32:
        return self.header.metadata.height

    @property
    def round(self) -> u64:
        return self.header.metadata.round

    @property
    def cumulative_weight(self) -> u128:
        return self.header.metadata.cumulative_weight

    @property
    def cumulative_proof_target(self) -> u128:
        return self.header.metadata.cumulative_proof_target
