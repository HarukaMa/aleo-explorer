from db import Database
from node.types import *
from .environment import Registers
from .utils import load_plaintext_from_operand


def execute_finalizer(db: Database, program: Program, function_name: Identifier, inputs: [Plaintext], expected_operations: [FinalizeOperation]):
    registers = Registers()
    operations = {}
    function: Function = program.functions[function_name]
    finalize: Finalize = function.finalize.value[1]

    finalize_inputs: [FinalizeInput] = list(finalize.inputs)
    if len(inputs) != len(finalize_inputs):
        raise TypeError("invalid number of inputs")
    for fi, i in zip(finalize_inputs, inputs):
        fi: FinalizeInput
        i: Plaintext
        if fi.plaintext_type.type.value != i.type.value:
            raise TypeError("invalid input type")
        ir: Register = fi.register
        if ir.type is not Register.Type.Locator:
            raise TypeError("invalid input register type")
        ir: LocatorRegister
        registers[int(ir.locator)] = i

    registers.dump()

    for c in finalize.commands:
        c: Command
        match c.type:
            case Command.Type.Instruction:
                c: InstructionCommand
                raise NotImplementedError("dude too deep")
            case Command.Type.Get:
                c: GetCommand
                raise NotImplementedError
            case Command.Type.GetOrInit:
                c: GetOrInitCommand
                raise NotImplementedError
            case Command.Type.Set:
                c: SetCommand
                mapping_id = Field.loads(aleo.get_mapping_id(str(program.id), str(c.mapping)))
                key = load_plaintext_from_operand(c.key, registers)
                value = load_plaintext_from_operand(c.value, registers)
                key_id = Field.loads(aleo.get_key_id(str(mapping_id), key.dump()))
                value_id = Field.loads(aleo.get_value_id(str(key_id), value.dump()))
                print(mapping_id)
                print(key_id)
                print(value_id)
                print(f"{c.mapping}[{key}](index) = {value}")
