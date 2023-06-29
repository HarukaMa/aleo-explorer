from db import Database
from disasm.aleo import disasm_instruction, disasm_command
from node.types import *
from .environment import Registers
from .instruction import execute_instruction
from .utils import load_plaintext_from_operand, store_plaintext_to_register

async def mapping_cache_read(db: Database, mapping_id: Field) -> list:
    mapping = await db.get_mapping_cache(str(mapping_id))
    if not mapping:
        return []
    res = []
    index = 0
    for m in mapping:
        if m["index"] != index:
            raise RuntimeError("invalid mapping index")
        res.append((
            Field.loads(m["key_id"]),
            Field.loads(m["value_id"]),
            Plaintext.load(bytearray(m["key"])),
            Value.load(bytearray(m["value"])),
        ))
        index += 1
    return res

def mapping_find_index(mapping: list, key_id: Field) -> int:
    for i, (k, _, _, _) in enumerate(mapping):
        if k == key_id:
            return i
    return -1

class ExecuteError(Exception):
    def __init__(self, message: str, exception: Exception, instruction: str):
        super().__init__(message)
        self.original_exception = exception
        self.instruction = instruction

async def execute_finalizer(db: Database, program: Program, function_name: Identifier, inputs: [Plaintext],
                            mapping_cache: dict) -> list:
    registers = Registers()
    operations = []
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

    print("loaded inputs")
    registers.dump()

    for c in finalize.commands:
        c: Command
        match c.type:
            case Command.Type.Instruction:
                c: InstructionCommand
                instruction: Instruction = c.instruction
                print(disasm_instruction(instruction))
                try:
                    execute_instruction(instruction, program, registers)
                except Exception as e:
                    raise ExecuteError(f"failed to execute instruction: {e}", e, disasm_instruction(instruction))
                registers.dump()

            case Command.Type.Get | Command.Type.GetOrUse:
                print(disasm_command(c))
                if c.type == Command.Type.Get:
                    c: GetCommand
                else:
                    c: GetOrUseCommand
                mapping_id = Field.loads(aleo.get_mapping_id(str(program.id), str(c.mapping)))
                if mapping_id not in mapping_cache:
                    mapping_cache[mapping_id] = await mapping_cache_read(db, mapping_id)
                key = load_plaintext_from_operand(c.key, registers)
                key_id = Field.loads(aleo.get_key_id(str(mapping_id), key.dump()))
                index = mapping_find_index(mapping_cache[mapping_id], key_id)
                if index == -1:
                    if c.type == Command.Type.Get:
                        raise RuntimeError("key not found")
                    default = load_plaintext_from_operand(c.default, registers)
                    value = PlaintextValue(plaintext=default)
                else:
                    value = mapping_cache[mapping_id][index][3]
                destination: Register = c.destination
                store_plaintext_to_register(value.plaintext, destination, registers)
                registers.dump()

            case Command.Type.Set:
                print(disasm_command(c))
                c: SetCommand
                mapping_id = Field.loads(aleo.get_mapping_id(str(program.id), str(c.mapping)))
                if mapping_id not in mapping_cache:
                    mapping_cache[mapping_id] = await mapping_cache_read(db, mapping_id)
                key = load_plaintext_from_operand(c.key, registers)
                value = PlaintextValue(plaintext=load_plaintext_from_operand(c.value, registers))
                key_id = Field.loads(aleo.get_key_id(str(mapping_id), key.dump()))
                value_id = Field.loads(aleo.get_value_id(str(key_id), value.dump()))
                index = mapping_find_index(mapping_cache[mapping_id], key_id)
                if index == -1:
                    index = len(mapping_cache[mapping_id])
                    mapping_cache[mapping_id].append((key_id, value_id, key, value))
                else:
                    if mapping_cache[mapping_id][index][0] != key_id:
                        raise RuntimeError("find_index returned invalid index")
                    mapping_cache[mapping_id][index] = (key_id, value_id, key, value)

                operations.append({
                    "type": FinalizeOperation.Type.UpdateKeyValue,
                    "mapping_id": mapping_id,
                    "index": index,
                    "key_id": key_id,
                    "value_id": value_id,
                    "mapping": c.mapping,
                    "key": key,
                    "value": value,
                })
                registers.dump()
            case _:
                raise NotImplementedError

    return operations




