import aleo

from node.types import *
from .environment import Registers


def execute_finalizer(program: Program, function_name: Identifier, inputs: [Plaintext], expected_operations: [FinalizeOperation]):
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
                match c.key.type:
                    case Operand.Type.Literal:
                        key = LiteralPlaintext(literal=c.key.literal)
                    case Operand.Type.Register:
                        register = c.key.register
                        match register.type:
                            case Register.Type.Locator:
                                register: LocatorRegister
                                key = registers[int(register.locator)]
                            case Register.Type.Member:
                                register: MemberRegister
                                struct_: StructPlaintext = registers[int(register.locator)]
                                if not isinstance(struct_, StructPlaintext):
                                    raise TypeError("register is not struct")
                                for i, identifier in enumerate(register.identifiers):
                                    if i == len(register.identifiers) - 1:
                                        key = struct_.members[identifier]
                                    else:
                                        struct_ = struct_.members[identifier]
                    case _:
                        raise NotImplementedError
                key_id = Field.loads(aleo.get_key_id(str(mapping_id), str(c.key)))
                value_id = Field.loads(aleo.get_value_id(str(key_id), str(c.value)))
                print(mapping_id, key_id, value_id)
