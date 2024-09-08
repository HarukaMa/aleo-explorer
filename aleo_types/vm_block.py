from __future__ import annotations

import json
import re
from hashlib import md5
from typing import NamedTuple

from .vm_instruction import *

if TYPE_CHECKING:
    from db import Database


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


class MapKey(Serializable, JSONSerialize):

    def __init__(self, *, plaintext_type: PlaintextType):
        self.plaintext_type = plaintext_type

    def dump(self) -> bytes:
        return self.plaintext_type.dump()

    @classmethod
    def load(cls, data: BytesIO):
        plaintext_type = PlaintextType.load(data)
        return cls(plaintext_type=plaintext_type)


class MapValue(Serializable, JSONSerialize):

    def __init__(self, *, plaintext_type: PlaintextType):
        self.plaintext_type = plaintext_type

    def dump(self) -> bytes:
        return self.plaintext_type.dump()

    @classmethod
    def load(cls, data: BytesIO):
        plaintext_type = PlaintextType.load(data)
        return cls(plaintext_type=plaintext_type)


class Mapping(Serializable, JSONSerialize):

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


class Struct(Serializable, JSONSerialize):

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


class EntryType(Serializable, JSONSerialize):  # enum

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


class RecordType(Serializable, JSONSerialize):

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


class ClosureInput(Serializable, JSONSerialize):

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


class ClosureOutput(Serializable, JSONSerialize):

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


class Closure(Serializable, JSONSerialize):

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


class Command(EnumBaseSerialize, RustEnum, Serializable, JSONSerialize):
    type: "Command.Type"

    class Type(IntEnumu8):

        @staticmethod
        def _generate_next_value_(name: str, start: int, count: int, last_values: list[int]):
            return count

        Instruction = auto()
        Await = auto()
        Contains = auto()
        Get = auto()
        GetOrUse = auto()
        RandChaCha = auto()
        Remove = auto()
        Set = auto()
        BranchEq = auto()
        BranchNeq = auto()
        Position = auto()

    fee_map = {
        Type.Instruction: 0,
        Type.Await: 500,
        Type.Contains: -2,
        Type.Get: -2,
        Type.GetOrUse: -2,
        Type.RandChaCha: 25_000,
        Type.Remove: -2,
        Type.Set: -2,
        Type.BranchEq: 500,
        Type.BranchNeq: 500,
        Type.Position: 100,
    }

    @classmethod
    def load(cls, data: BytesIO):
        type_ = cls.Type.load(data)
        if type_ == cls.Type.Instruction:
            return InstructionCommand.load(data)
        elif type_ == cls.Type.Await:
            return AwaitCommand.load(data)
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

    def cost(self, program: Program) -> int:
        if isinstance(self, InstructionCommand):
            return self.instruction.cost(program)
        return self.fee_map[self.type]

    def __str__(self):
        from disasm.aleo import disasm_command
        return disasm_command(self)

    def __repr__(self):
        return str(self)

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


class AwaitCommand(Command):
    type = Command.Type.Await

    def __init__(self, *, register: Register):
        self.register = register

    def dump(self) -> bytes:
        return self.type.dump() + self.register.dump()

    @classmethod
    def load(cls, data: BytesIO):
        register = Register.load(data)
        return cls(register=register)

class ContainsCommand(Command):
    type = Command.Type.Contains

    def __init__(self, *, mapping: CallOperator, key: Operand, destination: Register):
        self.mapping = mapping
        self.key = key
        self.destination = destination

    def dump(self) -> bytes:
        return self.type.dump() + self.mapping.dump() + self.key.dump() + self.destination.dump()

    @classmethod
    def load(cls, data: BytesIO):
        mapping = CallOperator.load(data)
        key = Operand.load(data)
        destination = Register.load(data)
        return cls(mapping=mapping, key=key, destination=destination)

class GetCommand(Command):
    type = Command.Type.Get

    def __init__(self, *, mapping: CallOperator, key: Operand, destination: Register):
        self.mapping = mapping
        self.key = key
        self.destination = destination

    def dump(self) -> bytes:
        data = self.type.dump() + self.mapping.dump()
        return data + self.key.dump() + self.destination.dump()

    @classmethod
    def load(cls, data: BytesIO):
        mapping = CallOperator.load(data)
        key = Operand.load(data)
        destination = Register.load(data)
        return cls(mapping=mapping, key=key, destination=destination)


class GetOrUseCommand(Command):
    type = Command.Type.GetOrUse

    def __init__(self, *, mapping: CallOperator, key: Operand, default: Operand, destination: Register):
        self.mapping = mapping
        self.key = key
        self.default = default
        self.destination = destination

    def dump(self) -> bytes:
        data = self.type.dump() + self.mapping.dump()
        return data + self.key.dump() + self.default.dump() + self.destination.dump()

    @classmethod
    def load(cls, data: BytesIO):
        mapping = CallOperator.load(data)
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


class FinalizeType(EnumBaseSerialize, RustEnum, Serializable, JSONSerialize):

    class Type(IntEnumu8):
        Plaintext = 0
        Future = 1

    type: Type

    @classmethod
    def load(cls, data: BytesIO):
        type_ = cls.Type.load(data)
        if type_ == cls.Type.Plaintext:
            return PlaintextFinalizeType.load(data)
        elif type_ == cls.Type.Future:
            return FutureFinalizeType.load(data)
        else:
            raise ValueError("Invalid variant")

class PlaintextFinalizeType(FinalizeType):
    type = FinalizeType.Type.Plaintext

    def __init__(self, *, plaintext_type: PlaintextType):
        self.plaintext_type = plaintext_type

    def dump(self) -> bytes:
        return self.type.dump() + self.plaintext_type.dump()

    @classmethod
    def load(cls, data: BytesIO):
        plaintext_type = PlaintextType.load(data)
        return cls(plaintext_type=plaintext_type)

class FutureFinalizeType(FinalizeType):
    type = FinalizeType.Type.Future

    def __init__(self, *, locator: Locator):
        self.locator = locator

    def dump(self) -> bytes:
        return self.type.dump() + self.locator.dump()

    @classmethod
    def load(cls, data: BytesIO):
        locator = Locator.load(data)
        return cls(locator=locator)

class FinalizeInput(Serializable, JSONSerialize):

    def __init__(self, *, register: Register, finalize_type: FinalizeType):
        self.register = register
        self.finalize_type = finalize_type

    def dump(self) -> bytes:
        return self.register.dump() + self.finalize_type.dump()

    @classmethod
    def load(cls, data: BytesIO):
        register = Register.load(data)
        finalize_type = FinalizeType.load(data)
        return cls(register=register, finalize_type=finalize_type)

class Finalize(Serializable, JSONSerialize):

    def __init__(self, *, name: Identifier, inputs: Vec[FinalizeInput, u16], commands: Vec[Command, u16]):
        self.name = name
        self.inputs = inputs
        self.commands = commands
        self.num_writes = len(list(filter(lambda x: isinstance(x, SetCommand) or isinstance(x, RemoveCommand), commands)))
        positions: dict[Identifier, int] = {}
        for i, c in enumerate(commands):
            if isinstance(c, PositionCommand):
                positions[c.position] = i
        self.positions = positions

    def dump(self) -> bytes:
        return self.name.dump() + self.inputs.dump() + self.commands.dump()

    @classmethod
    def load(cls, data: BytesIO):
        name = Identifier.load(data)
        inputs = Vec[FinalizeInput, u16].load(data)
        commands = Vec[Command, u16].load(data)
        return cls(name=name, inputs=inputs, commands=commands)

    def json(self) -> JSONType:
        res = {
            "name": self.name.json(),
            "inputs": self.inputs.json(),
            "commands": self.commands.json(),
        }
        return res

    def cost(self, program: Program) -> int:
        return sum(command.cost(program) for command in self.commands)


class ValueType(EnumBaseSerialize, RustEnum, Serializable, JSONSerialize):

    class Type(IntEnumu8):
        Constant = 0
        Public = 1
        Private = 2
        Record = 3
        ExternalRecord = 4
        Future = 5

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
        elif type_ == cls.Type.Future:
            return FutureValueType.load(data)
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


class FutureValueType(ValueType):
    type = ValueType.Type.Future

    def __init__(self, *, locator: Locator):
        self.locator = locator

    def dump(self) -> bytes:
        return self.type.dump() + self.locator.dump()

    @classmethod
    def load(cls, data: BytesIO):
        locator = Locator.load(data)
        return cls(locator=locator)


class FunctionInput(Serializable, JSONSerialize):

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


class FunctionOutput(Serializable, JSONSerialize):

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


class Function(Serializable, JSONSerialize):

    def __init__(self, *, name: Identifier, inputs: Vec[FunctionInput, u16], instructions: Vec[Instruction, u32],
                 outputs: Vec[FunctionOutput, u16], finalize: Option[Finalize]):
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
        finalize = Option[Finalize].load(data)
        return cls(name=name, inputs=inputs, instructions=instructions, outputs=outputs, finalize=finalize)

    def instruction_feature_string(self) -> str:
        return feature_string_from_instructions(self.instructions)

    def finalize_cost(self, program: Program):
        if self.finalize.value is None:
            return 0
        return self.finalize.value.cost(program)

class ProgramDefinition(IntEnumu8):
    Mapping = 0
    Struct = 1
    Record = 2
    Closure = 3
    Function = 4


class Program(Serializable, JSONSerialize):
    version = u8(1)

    def __init__(self, *, id_: ProgramID, imports: Vec[Import, u8], mappings: dict[Identifier, Mapping],
                 structs: dict[Identifier, Struct], records: dict[Identifier, RecordType],
                 closures: dict[Identifier, Closure], functions: dict[Identifier, Function],
                 identifiers: dict[Identifier, ProgramDefinition]):
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
        res += len(self.identifiers).to_bytes(2, "little")
        for i, d in self.identifiers.items():
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
        identifiers: dict[Identifier, ProgramDefinition] = {}
        mappings: dict[Identifier, Mapping] = {}
        structs: dict[Identifier, Struct] = {}
        records: dict[Identifier, RecordType] = {}
        closures: dict[Identifier, Closure] = {}
        functions: dict[Identifier, Function] = {}
        n = u16.load(data)
        for _ in range(n):
            d = ProgramDefinition.load(data)
            if d == ProgramDefinition.Mapping:
                m = Mapping.load(data)
                mappings[m.name] = m
                identifiers[m.name] = d
            elif d == ProgramDefinition.Struct:
                i = Struct.load(data)
                structs[i.name] = i
                identifiers[i.name] = d
            elif d == ProgramDefinition.Record:
                r = RecordType.load(data)
                records[r.name] = r
                identifiers[r.name] = d
            elif d == ProgramDefinition.Closure:
                c = Closure.load(data)
                closures[c.name] = c
                identifiers[c.name] = d
            elif d == ProgramDefinition.Function:
                f = Function.load(data)
                functions[f.name] = f
                identifiers[f.name] = d
        return cls(id_=id_, imports=imports, mappings=mappings, structs=structs, records=records,
                   closures=closures, functions=functions, identifiers=identifiers)

    def json(self) -> JSONType:
        res = super().json()
        if not isinstance(res, dict):
            raise ValueError("invalid json")
        from disasm.aleo import disassemble_program
        res["_text"] = disassemble_program(self)
        return res

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


class CircuitVerifyingKey(Serializable):

    def __init__(self, *, circuit_info: CircuitInfo, circuit_commitments: Vec[KZGCommitment, u64], id_: Vec[u8, FixedSize[32]]):
        self.circuit_info = circuit_info
        self.circuit_commitments = circuit_commitments
        self.id = id_

    def dump(self) -> bytes:
        return self.circuit_info.dump() + self.circuit_commitments.dump() + self.id.dump()

    @classmethod
    def load(cls, data: BytesIO):
        circuit_info = CircuitInfo.load(data)
        circuit_commitments = Vec[KZGCommitment, u64].load(data)
        id_ = Vec[u8, FixedSize[32]].load(data)
        return cls(circuit_info=circuit_info, circuit_commitments=circuit_commitments, id_=id_)


class VerifyingKey(Serializable, JSONSerialize):
    version = u8(1)

    def __init__(self, *, verifying_key: CircuitVerifyingKey, num_variables: u64):
        self.verifying_key = verifying_key
        self.num_variables = num_variables

    def dump(self) -> bytes:
        return self.version.dump() + self.verifying_key.dump() + self.num_variables.dump()

    @classmethod
    def load(cls, data: BytesIO):
        version = u8.load(data)
        if version != cls.version:
            raise ValueError("Invalid version")
        verifying_key = CircuitVerifyingKey.load(data)
        num_variables = u64.load(data)
        return cls(verifying_key=verifying_key, num_variables=num_variables)

    def json(self) -> JSONType:
        return str(self)

    def __str__(self):
        return str(Bech32m(self.dump(), "verifier"))


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

    def __init__(self, *, proof: BatchProof):
        self.proof = proof

    def dump(self) -> bytes:
        return self.proof.dump()

    @classmethod
    def load(cls, data: BytesIO):
        proof = BatchProof.load(data)
        return cls(proof=proof)


class Certificate(Serializable):
    version = u8(1)

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

    def json(self) -> JSONType:
        return str(self)

    def __str__(self):
        return str(Bech32m(self.dump(), "certificate"))


class Deployment(Serializable, JSONSerialize):
    version = u8(1)

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
        from node import Network
        storage_cost = len(self.dump()) * Network.deployment_fee_multiplier
        namespace_cost = 10 ** max(0, 10 - len(self.program.id.name.data)) * 1000000
        return storage_cost, namespace_cost


class WitnessCommitments(Serializable):

    def __init__(self, *, w: KZGCommitment):
        self.w = w

    def dump(self) -> bytes:
        return self.w.dump()

    @classmethod
    def load(cls, data: BytesIO):
        w = KZGCommitment.load(data)
        return cls(w=w)


class Commitments(Serializable):

    def __init__(self, *, witness_commitments: Vec[WitnessCommitments, u64], mask_poly: Option[KZGCommitment],
                 h_0: KZGCommitment,
                 g_1: KZGCommitment, h_1: KZGCommitment, g_a_commitments: Vec[KZGCommitment, u64],
                 g_b_commitments: Vec[KZGCommitment, u64], g_c_commitments: Vec[KZGCommitment, u64], h_2: KZGCommitment):
        self.witness_commitments = witness_commitments
        self.mask_poly = mask_poly
        self.h_0 = h_0
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
        res += self.h_0.dump()
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
        w_commitments: list[WitnessCommitments] = []
        for _ in range(sum(batch_sizes)):
            w_commitments.append(WitnessCommitments.load(data))
        witness_commitments = Vec[WitnessCommitments, u64](w_commitments)
        mask_poly = Option[KZGCommitment].load(data)
        h_0 = KZGCommitment.load(data)
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
        return cls(witness_commitments=witness_commitments, mask_poly=mask_poly, h_0=h_0, g_1=g_1, h_1=h_1,
                   g_a_commitments=g_a_commitments, g_b_commitments=g_b_commitments,
                   g_c_commitments=g_c_commitments, h_2=h_2)


class Evaluations(Serializable):

    def __init__(self, *, g_1_eval: Field, g_a_evals: Vec[Field, u64], g_b_evals: Vec[Field, u64],
                 g_c_evals: Vec[Field, u64]):
        self.g_1_eval = g_1_eval
        self.g_a_evals = g_a_evals
        self.g_b_evals = g_b_evals
        self.g_c_evals = g_c_evals

    def dump(self) -> bytes:
        res = self.g_1_eval.dump()
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
        return cls(g_1_eval=g_1_eval, g_a_evals=g_a_evals, g_b_evals=g_b_evals, g_c_evals=g_c_evals)


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

    def __init__(self, *, sums: Vec[Vec[MatrixSums, u64], u64]):
        self.sums = sums

    def dump(self) -> bytes:
        res = b""
        for sum_ in self.sums:
            for s in sum_:
                res += s.dump()
        return res

    @classmethod
    def load(cls, data: BytesIO) -> Self:
        raise NotImplementedError("use load_with_batch_sizes instead")

    @classmethod
    def load_with_batch_sizes(cls, data: BytesIO, batch_sizes: Vec[u64, u64]):
        sums: list[Vec[MatrixSums, u64]] = []
        for batch_size in batch_sizes:
            batch: list[MatrixSums] = []
            for _ in range(batch_size):
                batch.append(MatrixSums.load(data))
            sums.append(Vec[MatrixSums, u64](batch))
        return cls(sums=Vec[Vec[MatrixSums, u64], u64](sums))

class FourthMessage(Serializable):

    def __init__(self, *, sums: Vec[MatrixSums, u64]):
        self.sums = sums

    def dump(self) -> bytes:
        res = b""
        for sum_ in self.sums:
            res += sum_.dump()
        return res

    @classmethod
    def load(cls, data: BytesIO) -> Self:
        raise NotImplementedError("use load_with_batch_sizes instead")

    @classmethod
    def load_with_batch_sizes(cls, data: BytesIO, batch_sizes: Vec[u64, u64]):
        sums: list[MatrixSums] = []
        for _ in range(len(batch_sizes)):
            sums.append(MatrixSums.load(data))
        return cls(sums=Vec[MatrixSums, u64](sums))


class Proof(Serializable, JSONSerialize):
    version = u8(1)

    # Skipping a layer of varuna::Proof
    def __init__(self, *, batch_sizes: Vec[u64, u64], commitments: Commitments, evaluations: Evaluations,
                 third_msg: ThirdMessage, fourth_msg: FourthMessage, pc_proof: BatchLCProof):
        self.batch_sizes = batch_sizes
        self.commitments = commitments
        self.evaluations = evaluations
        self.third_msg = third_msg
        self.fourth_msg = fourth_msg
        self.pc_proof = pc_proof

    def dump(self) -> bytes:
        res = b""
        res += self.version.dump()
        res += self.batch_sizes.dump()
        res += self.commitments.dump()
        res += self.evaluations.dump()
        res += self.third_msg.dump()
        res += self.fourth_msg.dump()
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
        third_msg = ThirdMessage.load_with_batch_sizes(data, batch_sizes=batch_sizes)
        fourth_msg = FourthMessage.load_with_batch_sizes(data, batch_sizes=batch_sizes)
        pc_proof = BatchLCProof.load(data)
        return cls(batch_sizes=batch_sizes, commitments=commitments, evaluations=evaluations, third_msg=third_msg,
                   fourth_msg=fourth_msg, pc_proof=pc_proof)

    @classmethod
    def loads(cls, data: str):
        return cls.load(bech32_to_bytes(data))

    def json(self) -> JSONType:
        return str(self)

    def __str__(self):
        return str(Bech32m(self.dump(), "proof"))

    def __repr__(self):
        return str(self)


class Ciphertext(Serializable, JSONSerialize):

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

    def json(self) -> JSONType:
        return str(self)

    def __str__(self):
        return str(Bech32m(self.dump(), "ciphertext"))


class Plaintext(EnumBaseSerialize, RustEnum, Serializable, JSONSerialize):  # enum

    class Type(IntEnumu8):
        Literal = 0
        Struct = 1
        Array = 2

    type: Type

    @classmethod
    def load(cls, data: BytesIO):
        type_ = Plaintext.Type.load(data)
        if type_ == Plaintext.Type.Literal:
            return LiteralPlaintext.load(data)
        elif type_ == Plaintext.Type.Struct:
            return StructPlaintext.load(data)
        elif type_ == Plaintext.Type.Array:
            return ArrayPlaintext.load(data)
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

    def json(self) -> JSONType:
        return str(self)

    def __str__(self):
        return str(self.literal)

    def __repr__(self):
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
        res += len(self.members).to_bytes(byteorder="little")
        for member in self.members:
            res += member[0].dump()  # Identifier
            num_bytes = member[1].dump()  # Plaintext
            res += len(num_bytes).to_bytes(2, "little")
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
            members.append(Tuple[Identifier, Plaintext]((identifier, plaintext)))
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
            match = re.search(r"[,}]", s)
            if not match:
                raise ValueError("invalid literal")
            pos = match.start()
            if s[pos] == "}":
                is_comma = False
            return s[:pos], s[pos + int(is_comma):]
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
        ordered_members: list[Tuple[Identifier, Plaintext]] = []
        for key, _ in struct_type.members:
            for member in members:
                if member[0] == key:
                    ordered_members.append(member)
                    break
        return cls(members=Vec[Tuple[Identifier, Plaintext], u8](ordered_members))

    def json(self) -> JSONType:
        return str(self)

    def __str__(self):
        data = {}
        for identifier, plaintext in self.members:
            data[str(identifier)] = str(plaintext)
        return json.dumps(data).replace('"', '')

    def __repr__(self):
        return str(self)

    def get_member(self, identifier: Identifier) -> Plaintext:
        for member in self.members:
            if member[0] == identifier:
                return member[1]
        raise ValueError("Identifier not found")

    def set_member(self, identifier: Identifier, plaintext: Plaintext):
        for i, member in enumerate(self.members):
            if member[0] == identifier:
                self.members[i] = Tuple[Identifier, Plaintext]((identifier, plaintext))
                return
        raise ValueError("Identifier not found")

    def __getitem__(self, item: str) -> Plaintext:
        return self.get_member(Identifier.loads(item))

    def __setitem__(self, key: str, value: Plaintext):
        self.set_member(Identifier.loads(key), value)

    def __eq__(self, other: object):
        if not isinstance(other, StructPlaintext):
            return False
        for identifier, plaintext in self.members:
            if plaintext != other.get_member(identifier):
                return False
        return True

class ArrayPlaintext(Plaintext):
    type = Plaintext.Type.Array

    def __init__(self, *, elements: Vec[Plaintext, u32]):
        self.elements = elements

    def dump(self) -> bytes:
        res = self.type.dump()
        res += len(self.elements).to_bytes(4, "little")
        for element in self.elements:
            data = element.dump()
            res += len(data).to_bytes(2, "little")
            res += data
        return res

    @classmethod
    def load(cls, data: BytesIO):
        elements: list[Plaintext] = []
        num_elements = u32.load(data)
        for _ in range(num_elements):
            num_bytes = u16.load(data)
            element = Plaintext.load(BytesIO(data.read(num_bytes)))
            elements.append(element)
        return cls(elements=Vec[Plaintext, u32](elements))

    @classmethod
    def loads(cls, data: str) -> Self:
        raise NotImplementedError

    def json(self) -> JSONType:
        return str(self)

    def __str__(self):
        return str(self.elements)

    def __repr__(self):
        return str(self.elements)

    def __getitem__(self, item: int) -> Plaintext:
        return self.elements[item]

    def __setitem__(self, key: int, value: Plaintext):
        self.elements[key] = value

    def __len__(self):
        return len(self.elements)

    def __eq__(self, other: object):
        if not isinstance(other, ArrayPlaintext):
            return False
        if len(self.elements) != len(other.elements):
            return False
        for i in range(len(self.elements)):
            if self.elements[i] != other.elements[i]:
                return False
        return True


class Owner(EnumBaseSerialize, RustEnum, Serializable, JSONSerialize, Generic[T]):
    Private: TType[T]

    @tp_cache
    def __class_getitem__(cls, item: TType[T]) -> GenericAlias:
        param_type = type(
            f"Owner[{item.__name__}]",
            (Owner,),
            {"Private": item}
        )
        return GenericAlias(param_type, item)

    class Type(IntEnumu8):
        Public = 0
        Private = 1

    @classmethod
    def load(cls, data: BytesIO):
        type_ = Owner.Type.load(data)
        if type_ == Owner.Type.Public:
            return PublicOwner[T].load(data)
        elif type_ == Owner.Type.Private:
            return PrivateOwner[cls.Private].load(data)
        else:
            raise ValueError("invalid type")

    def json(self) -> JSONType:
        return str(self)


class PublicOwner(Owner[T]):
    type = Owner.Type.Public

    @tp_cache
    def __class_getitem__(cls, item: TType[T]) -> GenericAlias:
        param_type = type(
            f"PublicOwner[{item.__name__}]",
            (PublicOwner,),
            {"Private": item}
        )
        return GenericAlias(param_type, item)

    # This subtype is not generic
    # noinspection PyMissingConstructor
    def __init__(self, *, owner: Address):
        self.owner = owner

    def dump(self) -> bytes:
        return self.type.dump() + self.owner.dump()

    @classmethod
    def load(cls, data: BytesIO):
        owner = Address.load(data)
        return cls(owner=owner)

    def __str__(self):
        return str(self.owner)


class PrivateOwner(Owner[T]):
    Private: TType[T]
    type = Owner.Type.Private

    # noinspection PyMissingConstructor
    def __init__(self, *, owner: T):
        self.owner = owner

    @tp_cache
    def __class_getitem__(cls, item: TType[T]) -> GenericAlias:
        param_type = type(
            f"PrivateOwner[{item.__name__}]",
            (PrivateOwner,),
            {"Private": item}
        )
        return GenericAlias(param_type, item)

    def dump(self) -> bytes:
        return self.type.dump() + self.owner.dump()

    @classmethod
    def load(cls, data: BytesIO):
        owner = cls.Private.load(data)
        return cls(owner=owner)

    def __str__(self):
        return str(self.owner)

class Entry(EnumBaseSerialize, RustEnum, Serializable, JSONSerialize, Generic[T]):
    Private: TType[T]

    @tp_cache
    def __class_getitem__(cls, item: TType[T]) -> GenericAlias:
        param_type = type(
            f"Entry[{item.__name__}]",
            (Entry,),
            {"Private": item}
        )
        return GenericAlias(param_type, item)

    class Type(IntEnumu8):
        Constant = 0
        Public = 1
        Private = 2

    @classmethod
    def load(cls, data: BytesIO):
        type_ = Entry.Type.load(data)
        if type_ == Entry.Type.Constant:
            return ConstantEntry[T].load(data)
        elif type_ == Entry.Type.Public:
            return PublicEntry[T].load(data)
        elif type_ == Entry.Type.Private:
            return PrivateEntry[cls.Private].load(data)
        else:
            raise ValueError("invalid type")


class ConstantEntry(Entry[T]):
    type = Entry.Type.Constant

    @tp_cache
    def __class_getitem__(cls, item: TType[T]) -> GenericAlias:
        param_type = type(
            f"ConstantEntry[{item.__name__}]",
            (ConstantEntry,),
            {"Private": item}
        )
        return GenericAlias(param_type, item)

    # noinspection PyMissingConstructor
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


class PublicEntry(Entry[T]):
    type = Entry.Type.Public

    @tp_cache
    def __class_getitem__(cls, item: TType[T]) -> GenericAlias:
        param_type = type(
            f"PublicEntry[{item.__name__}]",
            (PublicEntry,),
            {"Private": item}
        )
        return GenericAlias(param_type, item)

    # noinspection PyMissingConstructor
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


class PrivateEntry(Entry[T]):
    Private: TType[T]
    type = Entry.Type.Private

    # noinspection PyMissingConstructor
    def __init__(self, *, private: T):
        self.private = private

    @tp_cache
    def __class_getitem__(cls, item: TType[T]) -> GenericAlias:
        param_type = type(
            f"PrivateEntry[{item.__name__}]",
            (PrivateEntry,),
            {"Private": item}
        )
        return GenericAlias(param_type, item)

    def dump(self) -> bytes:
        return self.type.dump() + self.private.dump()

    @classmethod
    def load(cls, data: BytesIO):
        plaintext = cls.Private.load(data)
        return cls(private=plaintext)

    def __str__(self):
        return str(self.private)


class Record(Serializable, JSONSerialize, Generic[T]):
    Private: TType[T]

    def __init__(self, *, owner: Owner[T], data: Vec[Tuple[Identifier, Entry[T]], u8], nonce: Group):
        self.owner = owner
        self.data = data
        self.nonce = nonce

    @tp_cache
    def __class_getitem__(cls, item: TType[T]) -> GenericAlias:
        param_type = type(
            f"Record[{item.__name__}]",
            (Record,),
            {"Private": item}
        )
        return GenericAlias(param_type, item)

    def dump(self) -> bytes:
        res = b""
        res += self.owner.dump()
        res += len(self.data).to_bytes(byteorder="little")
        for identifier, entry in self.data:
            res += identifier.dump()
            bytes_ = entry.dump()
            res += len(bytes_).to_bytes(2, "little")
            res += bytes_
        res += self.nonce.dump()
        return res

    @classmethod
    def load(cls, data: BytesIO):
        Private = cls.Private
        owner = Owner[Private].load(data)
        data_len = u8.load(data)
        d: list[Tuple[Identifier, Entry[T]]] = []
        for _ in range(data_len):
            identifier = Identifier.load(data)
            entry_len = u16.load(data)
            entry = Entry[Private].load(BytesIO(data.read(entry_len)))
            d.append(Tuple[Identifier, Entry[T]]((identifier, entry)))
        data_ = Vec[Tuple[Identifier, Entry[T]], u8](d)
        nonce = Group.load(data)
        return cls(owner=owner, data=data_, nonce=nonce)

    @classmethod
    def loads(cls, data: str):
        return cls.load(bech32_to_bytes(data))

    def json(self) -> JSONType:
        res = super().json()
        if not isinstance(res, dict):
            raise ValueError("invalid json")
        res["_text"] = str(self)
        return res

    def __str__(self):
        return str(Bech32m(self.dump(), "record"))


class Value(EnumBaseSerialize, RustEnum, Serializable):

    class Type(IntEnumu8):
        Plaintext = 0
        Record = 1
        Future = 2

    type: Type

    @classmethod
    def load(cls, data: BytesIO):
        type_ = Value.Type.load(data)
        if type_ == Value.Type.Plaintext:
            return PlaintextValue.load(data)
        elif type_ == Value.Type.Record:
            return RecordValue.load(data)
        elif type_ == Value.Type.Future:
            return FutureValue.load(data)
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

    def __repr__(self):
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


class Future(Serializable, JSONSerialize):

    def __init__(self, *, program_id: ProgramID, function_name: Identifier, arguments: Vec[Argument, u8]):
        self.program_id = program_id
        self.function_name = function_name
        self.arguments = arguments

    def dump(self) -> bytes:
        return self.program_id.dump() + self.function_name.dump() + self.arguments.dump()

    @classmethod
    def load(cls, data: BytesIO):
        program_id = ProgramID.load(data)
        function_name = Identifier.load(data)
        arguments = Vec[Argument, u8].load(data)
        return cls(program_id=program_id, function_name=function_name, arguments=arguments)

    def __str__(self):
        return f"{self.program_id}.{self.function_name}({', '.join(str(arg) for arg in self.arguments)})"

    def __repr__(self):
        return f"{self.program_id}.{self.function_name}({', '.join(str(arg) for arg in self.arguments)})"


class Argument(EnumBaseSerialize, RustEnum, Serializable, JSONSerialize):

    class Type(IntEnumu8):
        Plaintext = 0
        Future = 1

    type: Type

    @classmethod
    def load(cls, data: BytesIO):
        size = u16.load(data)
        data = BytesIO(data.read(size))
        type_ = Argument.Type.load(data)
        if type_ == Argument.Type.Plaintext:
            return PlaintextArgument.load(data)
        elif type_ == Argument.Type.Future:
            return FutureArgument.load(data)
        else:
            raise ValueError("unknown argument type")

class PlaintextArgument(Argument):
    type = Argument.Type.Plaintext

    def __init__(self, *, plaintext: Plaintext):
        self.plaintext = plaintext

    def dump(self) -> bytes:
        data = self.type.dump() + self.plaintext.dump()
        return len(data).to_bytes(2, "little") + data

    @classmethod
    def load(cls, data: BytesIO):
        plaintext = Plaintext.load(data)
        return cls(plaintext=plaintext)

    def json(self) -> JSONType:
        return str(self)

    def __str__(self):
        return str(self.plaintext)

    def __repr__(self):
        return str(self.plaintext)

class FutureArgument(Argument):
    type = Argument.Type.Future

    def __init__(self, *, future: Future):
        self.future = future

    def dump(self) -> bytes:
        data = self.type.dump() + self.future.dump()
        return len(data).to_bytes(2, "little") + data

    @classmethod
    def load(cls, data: BytesIO):
        future = Future.load(data)
        return cls(future=future)

    def json(self) -> JSONType:
        return self.future.json()

    def __str__(self):
        return str(self.future)

    def __repr__(self):
        return str(self.future)

class FutureValue(Value):
    type = Value.Type.Future

    def __init__(self, *, future: Future):
        self.future = future

    def dump(self) -> bytes:
        return self.type.dump() + self.future.dump()

    @classmethod
    def load(cls, data: BytesIO):
        future = Future.load(data)
        return cls(future=future)

    def __str__(self):
        return str(self.future)

    def __repr__(self):
        return str(self.future)


class TransitionInput(EnumBaseSerialize, RustEnum, Serializable, JSONSerialize):

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


class TransitionOutput(EnumBaseSerialize, RustEnum, Serializable, JSONSerialize):

    class Type(IntEnumu8):
        Constant = 0
        Public = 1
        Private = 2
        Record = 3
        ExternalRecord = 4
        Future = 5

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
        elif type_ == TransitionOutput.Type.Future:
            return FutureTransitionOutput.load(data)
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

class FutureTransitionOutput(TransitionOutput):
    type = TransitionOutput.Type.Future

    def __init__(self, *, future_hash: Field, future: Option[Future]):
        self.future_hash = future_hash
        self.future = future

    def dump(self) -> bytes:
        return self.type.dump() + self.future_hash.dump() + self.future.dump()

    @classmethod
    def load(cls, data: BytesIO):
        future_hash = Field.load(data)
        future = Option[Future].load(data)
        return cls(future_hash=future_hash, future=future)


class Transition(Serializable, JSONSerialize):
    version = u8(1)

    def __init__(self, *, id_: TransitionID, program_id: ProgramID, function_name: Identifier,
                 inputs: Vec[TransitionInput, u8], outputs: Vec[TransitionOutput, u8],
                 tpk: Group, tcm: Field, scm: Field):
        self.id = id_
        self.program_id = program_id
        self.function_name = function_name
        self.inputs = inputs
        self.outputs = outputs
        self.tpk = tpk
        self.tcm = tcm
        self.scm = scm

    def dump(self) -> bytes:
        res = b""
        res += self.version.dump()
        res += self.id.dump()
        res += self.program_id.dump()
        res += self.function_name.dump()
        res += self.inputs.dump()
        res += self.outputs.dump()
        res += self.tpk.dump()
        res += self.tcm.dump()
        res += self.scm.dump()
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
        tpk = Group.load(data)
        tcm = Field.load(data)
        scm = Field.load(data)
        return cls(id_=id_, program_id=program_id, function_name=function_name, inputs=inputs, outputs=outputs,
                   tpk=tpk, tcm=tcm, scm=scm)


class Fee(Serializable, JSONSerialize):
    version = u8(1)

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

    def json(self) -> JSONType:
        data = super().json()
        if not isinstance(data, dict):
            raise ValueError("invalid json")
        data["amount"] = self.amount
        return data

    @property
    def amount(self):
        """ Returns base_fee, priority_fee """
        def get_primitive_from_public_input(i: PublicTransitionInput):
            value = i.plaintext.value
            if not isinstance(value, LiteralPlaintext):
                raise RuntimeError("bad transition data")
            primitive = value.literal.primitive
            if not isinstance(primitive, int):
                raise RuntimeError("bad transition data")
            return primitive

        ts = self.transition

        if str(ts.program_id) != "credits.aleo" or str(ts.function_name) not in ["fee_public", "fee_private"]:
            raise RuntimeError("transition is not fee transition")
        if str(ts.function_name) == "fee_public":
            fee_start_index = 0
        else:
            fee_start_index = 1
        fee_input = ts.inputs[fee_start_index]
        if not isinstance(fee_input, PublicTransitionInput):
            raise RuntimeError("malformed fee transition")
        priority_fee_input = ts.inputs[fee_start_index + 1]
        if not isinstance(priority_fee_input, PublicTransitionInput):
            raise RuntimeError("malformed fee transition")
        return int(get_primitive_from_public_input(fee_input)), int(get_primitive_from_public_input(priority_fee_input))


class Execution(Serializable, JSONSerialize):
    version = u8(1)

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
        if str(transition.function_name).startswith("fee_"):
            return True
        return False

    @property
    def storage_cost(self) -> int:
        if self.is_free_execution:
            return 0
        return len(self.dump())

    async def finalize_costs(self, db: "Database"):
        finalize_costs: list[int] = []
        for transition in self.transitions:
            from util.global_cache import get_program
            program = await get_program(db, str(transition.program_id))
            if program is None:
                raise RuntimeError("program not found")
            finalize_costs.append(program.functions[transition.function_name].finalize_cost(program))
        return finalize_costs

FeeComponent = NamedTuple("FeeComponent", [
    ("storage_cost", int),
    ("namespace_cost", int),
    ("finalize_costs", list[int]),
    ("priority_fee", int),
    ("burnt", int)
])

class Transaction(EnumBaseSerialize, RustEnum, Serializable, JSONSerialize):
    version = u8(1)

    class Type(IntEnumu8):
        Deploy = 0
        Execute = 1
        Fee = 2

    id: TransactionID
    type: Type
    fee: Fee | Option[Fee]

    @classmethod
    def load(cls, data: BytesIO):
        version = u8.load(data)
        type_ = cls.Type.load(data)
        if version != cls.version:
            raise ValueError("incorrect version")
        if type_ == cls.Type.Deploy:
            return DeployTransaction.load(data)
        elif type_ == cls.Type.Execute:
            return ExecuteTransaction.load(data)
        elif type_ == cls.Type.Fee:
            return FeeTransaction.load(data)
        else:
            raise ValueError("incorrect type")

    async def get_fee_breakdown(self, db: "Database") -> FeeComponent:
        if isinstance(self, DeployTransaction):
            fee = cast(Fee, self.fee)
            deployment = self.deployment
            storage_cost, namespace_cost = deployment.cost
            base_fee, priority_fee = fee.amount
            burnt = base_fee - storage_cost - namespace_cost
            return FeeComponent(storage_cost, namespace_cost, [], priority_fee, burnt)
        elif isinstance(self, ExecuteTransaction):
            execution = self.execution
            fee = cast(Option[Fee], self.fee).value
            storage_cost = execution.storage_cost
            finalize_costs = await execution.finalize_costs(db)
            if fee is not None:
                base_fee, priority_fee = fee.amount
            else:
                return FeeComponent(0, 0, [], 0, 0)
            burnt = base_fee - storage_cost - sum(finalize_costs)
            return FeeComponent(storage_cost, 0, finalize_costs, priority_fee, burnt)
        elif isinstance(self, FeeTransaction):
            raise TypeError("use ConfirmedTransaction to get fee breakdown")
        else:
            raise NotImplementedError


class ProgramOwner(Serializable, JSONSerialize):
    version = u8(1)

    def __init__(self, *, address: Address, signature: Signature):
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

    def __init__(self, *, id_: TransactionID, execution: Execution, fee: Option[Fee]):
        self.id = id_
        self.execution = execution
        self.fee = fee

    def dump(self) -> bytes:
        return self.version.dump() + self.type.dump() + self.id.dump() + self.execution.dump() + self.fee.dump()

    @classmethod
    def load(cls, data: BytesIO):
        id_ = TransactionID.load(data)
        execution = Execution.load(data)
        fee = Option[Fee].load(data)
        return cls(id_=id_, execution=execution, fee=fee)

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


class FinalizeOperation(EnumBaseSerialize, RustEnum, Serializable, JSONSerialize):
    class Type(IntEnumu8):
        InitializeMapping = 0
        InsertKeyValue = 1
        UpdateKeyValue = 2
        RemoveKeyValue = 3
        ReplaceMapping = 4
        RemoveMapping = 5

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
        elif type_ == cls.Type.ReplaceMapping:
            return ReplaceMapping.load(data)
        elif type_ == cls.Type.RemoveMapping:
            return RemoveMapping.load(data)
        else:
            raise ValueError("incorrect type")

class ConfirmedTransaction(EnumBaseSerialize, RustEnum, Serializable, JSONSerialize):
    class Type(IntEnumu8):
        AcceptedDeploy = 0
        AcceptedExecute = 1
        RejectedDeploy = 2
        RejectedExecute = 3

    type: Type
    index: u32
    transaction: Transaction
    finalize: Vec[FinalizeOperation, u16]

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

    async def get_fee_breakdown(self, db: "Database") -> FeeComponent:
        """
        Returns (storage_cost, namespace_cost, finalize_costs, priority_fee, burnt)
        """
        tx = self.transaction
        # TODO: better way to express this?
        if isinstance(tx, DeployTransaction) or isinstance(self, RejectedDeploy):
            if isinstance(tx, DeployTransaction):
                fee = cast(Fee, tx.fee)
                deployment = tx.deployment
            elif isinstance(self, RejectedDeploy):
                if not isinstance(self.rejected, RejectedDeployment):
                    raise RuntimeError("bad transaction data")
                if not isinstance(tx, FeeTransaction):
                    raise RuntimeError("bad transaction data")
                deployment = self.rejected.deploy
                fee = cast(Fee, tx.fee)
            else:
                raise RuntimeError("bad transaction data")
            storage_cost, namespace_cost = deployment.cost
            base_fee, priority_fee = fee.amount
            burnt = base_fee - storage_cost - namespace_cost
            return FeeComponent(storage_cost, namespace_cost, [], priority_fee, burnt)
        elif isinstance(tx, ExecuteTransaction) or isinstance(self, RejectedExecute):
            if isinstance(tx, ExecuteTransaction):
                execution = tx.execution
                fee = cast(Option[Fee], tx.fee).value
            elif isinstance(self, RejectedExecute):
                if not isinstance(self.rejected, RejectedExecution):
                    raise RuntimeError("bad transaction data")
                if not isinstance(tx, FeeTransaction):
                    raise RuntimeError("bad transaction data")
                execution = self.rejected.execution
                fee = cast(Fee, tx.fee)
            else:
                raise RuntimeError("bad transaction data")
            storage_cost = execution.storage_cost
            finalize_costs = await execution.finalize_costs(db)
            if fee is not None:
                base_fee, priority_fee = fee.amount
            else:
                return FeeComponent(0, 0, [], 0, 0)
            burnt = base_fee - storage_cost - sum(finalize_costs)
            return FeeComponent(storage_cost, 0, finalize_costs, priority_fee, burnt)
        else:
            raise NotImplementedError


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

    def __eq__(self, other: Any):
        if not isinstance(other, UpdateKeyValue):
            return False
        return self.mapping_id == other.mapping_id and self.key_id == other.key_id and self.value_id == other.value_id


class RemoveKeyValue(FinalizeOperation):
    type = FinalizeOperation.Type.RemoveKeyValue

    def __init__(self, *, mapping_id: Field, key_id: Field):
        self.mapping_id = mapping_id
        self.key_id = key_id

    def dump(self) -> bytes:
        return self.type.dump() + self.mapping_id.dump() + self.key_id.dump()

    @classmethod
    def load(cls, data: BytesIO):
        mapping_id = Field.load(data)
        key_id = Field.load(data)
        return cls(mapping_id=mapping_id, key_id=key_id)

    def __eq__(self, other: Any):
        if not isinstance(other, RemoveKeyValue):
            return False
        return self.mapping_id == other.mapping_id and self.key_id == other.key_id


class ReplaceMapping(FinalizeOperation):
    type = FinalizeOperation.Type.ReplaceMapping

    def __init__(self, *, mapping_id: Field):
        self.mapping_id = mapping_id

    def dump(self) -> bytes:
        return self.type.dump() + self.mapping_id.dump()

    @classmethod
    def load(cls, data: BytesIO):
        mapping_id = Field.load(data)
        return cls(mapping_id=mapping_id)


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

class Rejected(EnumBaseSerialize, RustEnum, Serializable, JSONSerialize):
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

    def __init__(self, *, index: u32, transaction: Transaction, rejected: Rejected, finalize: Vec[FinalizeOperation, u16]):
        self.index = index
        self.transaction = transaction
        self.rejected = rejected
        self.finalize = finalize

    def dump(self) -> bytes:
        return self.type.dump() + self.index.dump() + self.transaction.dump() + self.rejected.dump() + self.finalize.dump()

    @classmethod
    def load(cls, data: BytesIO):
        index = u32.load(data)
        transaction = Transaction.load(data)
        rejected = Rejected.load(data)
        finalize = Vec[FinalizeOperation, u16].load(data)
        return cls(index=index, transaction=transaction, rejected=rejected, finalize=finalize)


class RejectedExecute(ConfirmedTransaction):
    type = ConfirmedTransaction.Type.RejectedExecute

    def __init__(self, *, index: u32, transaction: Transaction, rejected: Rejected, finalize: Vec[FinalizeOperation, u16]):
        self.index = index
        self.transaction = transaction
        self.rejected = rejected
        self.finalize = finalize

    def dump(self) -> bytes:
        return self.type.dump() + self.index.dump() + self.transaction.dump() + self.rejected.dump() + self.finalize.dump()

    @classmethod
    def load(cls, data: BytesIO):
        index = u32.load(data)
        transaction = Transaction.load(data)
        rejected = Rejected.load(data)
        finalize = Vec[FinalizeOperation, u16].load(data)
        return cls(index=index, transaction=transaction, rejected=rejected, finalize=finalize)


class Transactions(Serializable, JSONSerialize):
    version = u8(1)

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

    def json(self) -> JSONType:
        return self.transactions.json()

    def __iter__(self):
        return iter(self.transactions)

    @property
    def total_priority_fee(self):
        total = 0
        for ctx in self:
            tx = ctx.transaction
            if isinstance(tx, (DeployTransaction, FeeTransaction)):
                total += cast(Fee, tx.fee).amount[1]
            elif isinstance(tx, ExecuteTransaction):
                fee = cast(Option[Fee], tx.fee)
                if fee.value is not None:
                    total += fee.value.amount[1]
        return total

class BlockHeaderMetadata(Serializable, JSONSerialize):
    version = u8(1)

    def __init__(self, *, network: u16, round_: u64, height: u32,
                 cumulative_weight: u128, cumulative_proof_target: u128, coinbase_target: u64, proof_target: u64,
                 last_coinbase_target: u64, last_coinbase_timestamp: i64, timestamp: i64):
        self.network = network
        self.round = round_
        self.height = height
        self.cumulative_weight = cumulative_weight
        self.cumulative_proof_target = cumulative_proof_target
        self.coinbase_target = coinbase_target
        self.proof_target = proof_target
        self.last_coinbase_target = last_coinbase_target
        self.last_coinbase_timestamp = last_coinbase_timestamp
        self.timestamp = timestamp

    def dump(self) -> bytes:
        return self.version.dump() + self.network.dump() + self.round.dump() + self.height.dump() + \
               self.cumulative_weight.dump() + \
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
        cumulative_weight = u128.load(data)
        cumulative_proof_target = u128.load(data)
        coinbase_target = u64.load(data)
        proof_target = u64.load(data)
        last_coinbase_target = u64.load(data)
        last_coinbase_timestamp = i64.load(data)
        timestamp = i64.load(data)
        return cls(network=network, round_=round_, height=height,
                   cumulative_weight=cumulative_weight,
                   cumulative_proof_target=cumulative_proof_target,
                   coinbase_target=coinbase_target, proof_target=proof_target,
                   last_coinbase_target=last_coinbase_target,
                   last_coinbase_timestamp=last_coinbase_timestamp, timestamp=timestamp)


class BlockHeader(Serializable, JSONSerialize):
    version = u8(1)

    def __init__(self, *, previous_state_root: StateRoot, transactions_root: Field, finalize_root: Field,
                 ratifications_root: Field, solutions_root: Field, subdag_root: Field, metadata: BlockHeaderMetadata):
        self.previous_state_root = previous_state_root
        self.transactions_root = transactions_root
        self.finalize_root = finalize_root
        self.ratifications_root = ratifications_root
        self.solutions_root = solutions_root
        self.subdag_root = subdag_root
        self.metadata = metadata

    def dump(self) -> bytes:
        return self.version.dump() + self.previous_state_root.dump() + self.transactions_root.dump() \
               + self.finalize_root.dump() + self.ratifications_root.dump() + self.solutions_root.dump() \
               + self.subdag_root.dump() + self.metadata.dump()

    @classmethod
    def load(cls, data: BytesIO):
        version = u8.load(data)
        previous_state_root = StateRoot.load(data)
        transactions_root = Field.load(data)
        finalize_root = Field.load(data)
        ratifications_root = Field.load(data)
        solutions_root = Field.load(data)
        subdag_root = Field.load(data)
        metadata = BlockHeaderMetadata.load(data)
        if version != cls.version:
            raise ValueError("invalid header version")
        return cls(previous_state_root=previous_state_root, transactions_root=transactions_root,
                   finalize_root=finalize_root, ratifications_root=ratifications_root,
                   solutions_root=solutions_root, subdag_root=subdag_root, metadata=metadata)


class PartialSolution(Serializable, JSONSerialize):

    def __init__(self, *, solution_id: SolutionID, epoch_hash: BlockHash, address: Address, counter: u64):
        self.solution_id = solution_id
        self.epoch_hash = epoch_hash
        self.address = address
        self.counter = counter

    def dump(self) -> bytes:
        return self.epoch_hash.dump() + self.address.dump() + self.counter.dump()

    @classmethod
    def load(cls, data: BytesIO):
        epoch_hash = BlockHash.load(data)
        address = Address.load(data)
        counter = u64.load(data)
        solution_id = SolutionID.load(BytesIO(aleo_explorer_rust.solution_to_id(str(epoch_hash), str(address), counter)))
        return cls(solution_id=solution_id, epoch_hash=epoch_hash, address=address, counter=counter)


class Solution(Serializable, JSONSerialize):

    def __init__(self, *, partial_solution: PartialSolution, target: u64):
        self.partial_solution = partial_solution
        self.target = target

    def dump(self) -> bytes:
        return self.partial_solution.dump() + self.target.dump()

    @classmethod
    def load(cls, data: BytesIO):
        partial_solution = PartialSolution.load(data)
        target = u64.load(data)
        return cls(partial_solution=partial_solution, target=target)


class PuzzleSolutions(Serializable, JSONSerialize):

    def __init__(self, *, solutions: Vec[Solution, u8]):
        self.solutions = solutions

    def dump(self) -> bytes:
        return self.solutions.dump()

    @classmethod
    def load(cls, data: BytesIO):
        solutions = Vec[Solution, u8].load(data)
        return cls(solutions=solutions)


class Ratify(EnumBaseSerialize, RustEnum, Serializable, JSONSerialize):

    class Type(IntEnumu8):
        Genesis = 0
        BlockReward = 1
        PuzzleReward = 2

    version = u8(1)

    @classmethod
    def load(cls, data: BytesIO):
        version = u8.load(data)
        if version != cls.version:
            raise ValueError(f"invalid version {version}")
        type_ = cls.Type.load(data)
        if type_ == cls.Type.Genesis:
            return GenesisRatify.load(data)
        elif type_ == cls.Type.BlockReward:
            return BlockRewardRatify.load(data)
        elif type_ == cls.Type.PuzzleReward:
            return PuzzleRewardRatify.load(data)
        else:
            raise ValueError(f"invalid ratify type {type_}")

class Committee(Serializable, JSONSerialize):
    version = u8(1)

    def __init__(self, *, id_: Field, starting_round: u64, members: Vec[Tuple[Address, u64, bool_, u8], u16], total_stake: u64):
        self.id = id_
        self.starting_round = starting_round
        self.members = members
        self.total_stake = total_stake

    def dump(self) -> bytes:
        return self.version.dump() + self.id.dump() + self.starting_round.dump() + self.members.dump() + self.total_stake.dump()

    @classmethod
    def load(cls, data: BytesIO):
        version = u8.load(data)
        if version != cls.version:
            raise ValueError(f"invalid committee version")
        id_ = Field.load(data)
        starting_round = u64.load(data)
        members = Vec[Tuple[Address, u64, bool_, u8], u16].load(data)
        total_stake = u64.load(data)
        return cls(id_=id_, starting_round=starting_round, members=members, total_stake=total_stake)

    @staticmethod
    def compute_committee_id(starting_round: u64, members: Vec[Tuple[Address, u64, bool_, u8], u16], total_stake: u64) -> Field:
        data: bytes = starting_round.dump() + members.dump() + total_stake.dump()
        return Field.load(BytesIO(aleo_explorer_rust.hash_bytes_to_field(data, "bhp1024")))


class GenesisRatify(Ratify):
    type = Ratify.Type.Genesis

    def __init__(self, *, committee: Committee, public_balances: Vec[Tuple[Address, u64], u16], bonded_balances: Vec[Tuple[Address, Address, Address, u64], u16]):
        self.committee = committee
        self.public_balances = public_balances
        self.bonded_balances = bonded_balances

    def dump(self) -> bytes:
        return self.version.dump() + self.type.dump() + self.committee.dump() + self.public_balances.dump() + self.bonded_balances.dump()

    @classmethod
    def load(cls, data: BytesIO):
        committee = Committee.load(data)
        public_balances = Vec[Tuple[Address, u64], u16].load(data)
        bonded_balances = Vec[Tuple[Address, Address, Address, u64], u16].load(data)
        return cls(committee=committee, public_balances=public_balances, bonded_balances=bonded_balances)


class BlockRewardRatify(Ratify):
    type = Ratify.Type.BlockReward

    def __init__(self, *, amount: u64):
        self.amount = amount

    def dump(self) -> bytes:
        return self.version.dump() + self.amount.dump()

    @classmethod
    def load(cls, data: BytesIO):
        amount = u64.load(data)
        return cls(amount=amount)

class PuzzleRewardRatify(Ratify):
    type = Ratify.Type.PuzzleReward

    def __init__(self, *, amount: u64):
        self.amount = amount

    def dump(self) -> bytes:
        return self.version.dump() + self.amount.dump()

    @classmethod
    def load(cls, data: BytesIO):
        amount = u64.load(data)
        return cls(amount=amount)


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


class Authority(EnumBaseSerialize, RustEnum, Serializable, JSONSerialize):

    class Type(IntEnumu8):
        Beacon = 0
        Quorum = 1

    type: Type

    @classmethod
    def load(cls, data: BytesIO):
        type_ = cls.Type.load(data)
        if type_ == cls.Type.Beacon:
            return BeaconAuthority.load(data)
        elif type_ == cls.Type.Quorum:
            return QuorumAuthority.load(data)
        else:
            raise ValueError("incorrect type")


class BeaconAuthority(Authority):
    type = Authority.Type.Beacon

    def __init__(self, *, signature: Signature):
        self.signature = signature

    def dump(self) -> bytes:
        return self.type.dump() + self.signature.dump()

    @classmethod
    def load(cls, data: BytesIO):
        signature = Signature.load(data)
        return cls(signature=signature)


class TransmissionID(EnumBaseSerialize, RustEnum, Serializable, JSONSerialize):

    class Type(IntEnumu8):
        Ratification = 0
        Solution = 1
        Transaction = 2

    type: Type

    @classmethod
    def load(cls, data: BytesIO):
        type_ = cls.Type.load(data)
        if type_ == cls.Type.Ratification:
            return RatificationTransmissionID.load(data)
        elif type_ == cls.Type.Solution:
            return SolutionTransmissionID.load(data)
        elif type_ == cls.Type.Transaction:
            return TransactionTransmissionID.load(data)
        else:
            raise ValueError("incorrect type")


class RatificationTransmissionID(TransmissionID):
    type = TransmissionID.Type.Ratification

    def dump(self) -> bytes:
        return self.type.dump()

    @classmethod
    def load(cls, data: BytesIO):
        return cls()

class SolutionID(Serializable, JSONSerialize):

    def __init__(self, *, nonce: u64):
        self.nonce = nonce

    def dump(self) -> bytes:
        return self.nonce.dump()

    @classmethod
    def load(cls, data: BytesIO):
        nonce = u64.load(data)
        return cls(nonce=nonce)

    @classmethod
    def loads(cls, data: str):
        hrp, raw = aleo_explorer_rust.bech32_decode(data)
        if hrp != "solution":
            raise ValueError("invalid hrp")
        return cls.load(BytesIO(raw))

    def json(self) -> JSONType:
        return str(self)

    def __str__(self):
        return str(Bech32m(self.dump(), "solution"))


class SolutionTransmissionID(TransmissionID):
    type = TransmissionID.Type.Solution

    def __init__(self, *, id_: SolutionID, checksum: u128):
        self.id = id_
        self.checksum = checksum

    def dump(self) -> bytes:
        return self.type.dump() + self.id.dump() + self.checksum.dump()

    @classmethod
    def load(cls, data: BytesIO):
        id_ = SolutionID.load(data)
        checksum = u128.load(data)
        return cls(id_=id_, checksum=checksum)

class TransactionTransmissionID(TransmissionID):
    type = TransmissionID.Type.Transaction

    def __init__(self, *, id_: TransactionID, checksum: u128):
        self.id = id_
        self.checksum = checksum

    def dump(self) -> bytes:
        return self.type.dump() + self.id.dump() + self.checksum.dump()

    @classmethod
    def load(cls, data: BytesIO):
        id_ = TransactionID.load(data)
        checksum = u128.load(data)
        return cls(id_=id_, checksum=checksum)


class BatchHeader(Serializable, JSONSerialize):
    version = u8(1)

    def __init__(self, *, batch_id: Field, author: Address, round_: u64, timestamp: i64, committee_id: Field,
                 transmission_ids: Vec[TransmissionID, u32], previous_certificate_ids: Vec[Field, u16],
                 signature: Signature):
        self.batch_id = batch_id
        self.author = author
        self.round = round_
        self.timestamp = timestamp
        self.committee_id = committee_id
        self.transmission_ids = transmission_ids
        self.previous_certificate_ids = previous_certificate_ids
        self.signature = signature

    def dump(self) -> bytes:
        return (self.version.dump() + self.batch_id.dump() + self.author.dump() + self.round.dump() + self.timestamp.dump()
                + self.committee_id.dump() + self.transmission_ids.dump() + self.previous_certificate_ids.dump()
                + self.signature.dump())

    @classmethod
    def load(cls, data: BytesIO):
        version = u8.load(data)
        if version != cls.version:
            raise ValueError("invalid batch header version")
        batch_id = Field.load(data)
        author = Address.load(data)
        round_ = u64.load(data)
        timestamp = i64.load(data)
        committee_id = Field.load(data)
        transmission_ids = Vec[TransmissionID, u32].load(data)
        previous_certificate_ids = Vec[Field, u16].load(data)
        signature = Signature.load(data)
        return cls(batch_id=batch_id, author=author, round_=round_, timestamp=timestamp, committee_id=committee_id,
                   transmission_ids=transmission_ids, previous_certificate_ids=previous_certificate_ids,
                   signature=signature)


class BatchCertificate(Serializable, JSONSerialize):
    version = u8(1)

    def __init__(self, *, batch_header: BatchHeader, signatures: Vec[Signature, u16]):
        self.batch_header = batch_header
        self.signatures = signatures

    def dump(self) -> bytes:
        return self.version.dump() + self.batch_header.dump() + self.signatures.dump()

    @classmethod
    def load(cls, data: BytesIO):
        version = u8.load(data)
        if version != cls.version:
            raise ValueError("invalid certificate version")
        batch_header = BatchHeader.load(data)
        signatures = Vec[Signature, u16].load(data)
        return cls(batch_header=batch_header, signatures=signatures)


class Subdag(Serializable, JSONSerialize):
    version = u8(1)

    def __init__(self, *, subdag: dict[u64, Vec[BatchCertificate, u16]]):
        self.subdag = subdag

    def dump(self) -> bytes:
        res = self.version.dump()
        res += len(self.subdag).to_bytes(4, 'little')
        for round_, certificates in self.subdag.items():
            res += round_.dump() + certificates.dump()
        return res

    @classmethod
    def load(cls, data: BytesIO):
        version = u8.load(data)
        if version != cls.version:
            raise ValueError("invalid subdag version")
        subdag: dict[u64, Vec[BatchCertificate, u16]] = {}
        for _ in range(u32.load(data)):
            round_ = u64.load(data)
            certificates = Vec[BatchCertificate, u16].load(data)
            subdag[round_] = certificates
        return cls(subdag=subdag)


class QuorumAuthority(Authority):
    type = Authority.Type.Quorum

    def __init__(self, *, subdag: Subdag):
        self.subdag = subdag

    def dump(self) -> bytes:
        return self.type.dump() + self.subdag.dump()

    @classmethod
    def load(cls, data: BytesIO):
        subdag = Subdag.load(data)
        return cls(subdag=subdag)


class Ratifications(Serializable, JSONSerialize):
    version = u8(1)

    def __init__(self, *, ratifications: Vec[Ratify, u32]):
        self.ratifications = ratifications

    def dump(self) -> bytes:
        return self.version.dump() + self.ratifications.dump()

    @classmethod
    def load(cls, data: BytesIO):
        version = u8.load(data)
        if version != cls.version:
            raise ValueError("invalid ratifications version")
        ratifications = Vec[Ratify, u32].load(data)
        return cls(ratifications=ratifications)

    def json(self) -> JSONType:
        return [r.json() for r in self.ratifications]

    def __iter__(self):
        return iter(self.ratifications)

class Solutions(Serializable, JSONSerialize):
    version = u8(1)

    def __init__(self, *, solutions: Option[PuzzleSolutions]):
        self.solutions = solutions

    def dump(self) -> bytes:
        return self.version.dump() + self.solutions.dump()

    @classmethod
    def load(cls, data: BytesIO):
        version = u8.load(data)
        solutions = Option[PuzzleSolutions].load(data)
        if version != cls.version:
            raise ValueError("invalid solutions version")
        return cls(solutions=solutions)

    def json(self) -> JSONType:
        return self.solutions.json()

    @property
    def value(self):
        return self.solutions.value


class Block(Serializable, JSONSerialize):
    version = u8(1)

    def __init__(self, *, block_hash: BlockHash, previous_hash: BlockHash, header: BlockHeader, authority: Authority,
                 ratifications: Ratifications, solutions: Solutions, aborted_solution_ids: Vec[SolutionID, u32],
                 transactions: Transactions, aborted_transactions_ids: Vec[TransactionID, u32]):
        self.block_hash = block_hash
        self.previous_hash = previous_hash
        self.header = header
        self.authority = authority
        self.ratifications = ratifications
        self.solutions = solutions
        self.aborted_solution_ids = aborted_solution_ids
        self.transactions = transactions
        self.aborted_transactions_ids = aborted_transactions_ids

    def dump(self) -> bytes:
        return (self.version.dump()
                + self.block_hash.dump()
                + self.previous_hash.dump()
                + self.header.dump()
                + self.authority.dump()
                + self.ratifications.dump()
                + self.solutions.dump()
                + self.aborted_solution_ids.dump()
                + self.transactions.dump()
                + self.aborted_transactions_ids.dump())

    @classmethod
    def load(cls, data: BytesIO):
        version = u8.load(data)
        block_hash = BlockHash.load(data)
        previous_hash = BlockHash.load(data)
        header = BlockHeader.load(data)
        authority = Authority.load(data)
        ratifications = Ratifications.load(data)
        solutions = Solutions.load(data)
        aborted_solution_ids = Vec[SolutionID, u32].load(data)
        transactions = Transactions.load(data)
        aborted_transactions_ids = Vec[TransactionID, u32].load(data)
        if version != cls.version:
            raise ValueError("invalid block version")
        return cls(block_hash=block_hash, previous_hash=previous_hash, header=header, authority=authority,
                   ratifications=ratifications, solutions=solutions, aborted_solution_ids=aborted_solution_ids,
                   transactions=transactions, aborted_transactions_ids=aborted_transactions_ids)


    def __str__(self):
        return f"Block {self.header.metadata.height} ({str(self.block_hash)[:16]}...)"

    def compute_rewards(self, last_coinbase_target: int, last_cumulative_proof_target: int) -> tuple[int, int]:
        starting_supply = 1_500_000_000_000_000
        anchor_time = 25
        block_time = 10
        anchor_height = anchor_time // block_time
        if self.solutions.value is None:
            combined_proof_target = 0
        else:
            combined_proof_target = sum(s.target for s in self.solutions.value.solutions)

        remaining_coinbase_target = max(0, last_coinbase_target - last_cumulative_proof_target)
        remaining_proof_target = min(combined_proof_target, remaining_coinbase_target)

        block_height_at_year_10 = 31536000 // 10 * 10
        remaining_blocks = max(0, block_height_at_year_10 - self.header.metadata.height)
        anchor_block_reward = 2 * starting_supply * anchor_height * remaining_blocks // (block_height_at_year_10 * (block_height_at_year_10 + 1))
        coinbase_reward = anchor_block_reward * remaining_proof_target // last_coinbase_target

        block_height_at_year_1 = 31536000 // 10
        annual_reward = starting_supply // 1000 * 50
        block_reward = annual_reward // block_height_at_year_1 + coinbase_reward // 3 + self.transactions.total_priority_fee

        return block_reward, int(coinbase_reward)

    def get_epoch_number(self) -> int:
        return self.header.metadata.height // 360

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

    async def get_total_priority_fee(self, db: "Database"):
        return sum([(await t.get_fee_breakdown(db)).priority_fee for t in self.transactions])

    async def get_total_burnt_fee(self, db: "Database"):
        """Includes both explicitly burnt fee and costs"""
        fees: list[FeeComponent] = [await t.get_fee_breakdown(db) for t in self.transactions]
        total = 0
        for fee in fees:
            total += fee.burnt + fee.storage_cost + fee.namespace_cost + sum(fee.finalize_costs)
        return total