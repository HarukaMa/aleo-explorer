from db import Database
from disasm.aleo import disasm_instruction, disasm_command
from node.types import *
from .environment import Registers
from .instruction import execute_instruction
from .utils import load_plaintext_from_operand, store_plaintext_to_register, FinalizeState

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
            Plaintext.load(BytesIO(m["key"])),
            Value.load(BytesIO(m["value"])),
        ))
        index += 1
    return res

def mapping_find_index(mapping: list, key_id: Field) -> int:
    for i, (k, _, _, _) in enumerate(mapping):
        if k == key_id:
            return i
    return -1

class ExecuteError(Exception):
    def __init__(self, message: str, exception: Exception | None, instruction: str):
        super().__init__(message)
        self.original_exception = exception
        self.instruction = instruction

async def execute_finalizer(db: Database, finalize_state: FinalizeState, transition_id: TransitionID, program: Program,
                            function_name: Identifier, inputs: [Plaintext], mapping_cache: dict) -> list:
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
                    execute_instruction(instruction, program, registers, finalize_state)
                except (AssertionError, OverflowError) as e:
                    raise ExecuteError(str(e), e, disasm_instruction(instruction))
                except Exception:
                    registers.dump()
                    raise

            case Command.Type.Contains:
                print(disasm_command(c))
                c: ContainsCommand
                mapping_id = Field.loads(aleo.get_mapping_id(str(program.id), str(c.mapping)))
                if mapping_id not in mapping_cache:
                    mapping_cache[mapping_id] = await mapping_cache_read(db, mapping_id)
                key = load_plaintext_from_operand(c.key, registers, finalize_state)
                key_id = Field.loads(aleo.get_key_id(str(mapping_id), key.dump()))
                index = mapping_find_index(mapping_cache[mapping_id], key_id)
                contains = index != -1
                value = PlaintextValue(
                    plaintext=LiteralPlaintext(
                        literal=Literal(
                            type_=Literal.Type.Boolean,
                            primitive=bool_(contains)
                        )
                    )
                )
                destination: Register = c.destination
                store_plaintext_to_register(value.plaintext, destination, registers)

            case Command.Type.Get | Command.Type.GetOrUse:
                print(disasm_command(c))
                if c.type == Command.Type.Get:
                    c: GetCommand
                else:
                    c: GetOrUseCommand
                mapping_id = Field.loads(aleo.get_mapping_id(str(program.id), str(c.mapping)))
                if mapping_id not in mapping_cache:
                    mapping_cache[mapping_id] = await mapping_cache_read(db, mapping_id)
                key = load_plaintext_from_operand(c.key, registers, finalize_state)
                key_id = Field.loads(aleo.get_key_id(str(mapping_id), key.dump()))
                index = mapping_find_index(mapping_cache[mapping_id], key_id)
                if index == -1:
                    if c.type == Command.Type.Get:
                        raise ExecuteError(f"key {key} not found in mapping {c.mapping}", None, disasm_command(c))
                    default = load_plaintext_from_operand(c.default, registers, finalize_state)
                    value = PlaintextValue(plaintext=default)
                else:
                    value = mapping_cache[mapping_id][index][3]
                destination: Register = c.destination
                store_plaintext_to_register(value.plaintext, destination, registers)

            case Command.Type.Set:
                print(disasm_command(c))
                c: SetCommand
                mapping_id = Field.loads(aleo.get_mapping_id(str(program.id), str(c.mapping)))
                if mapping_id not in mapping_cache:
                    mapping_cache[mapping_id] = await mapping_cache_read(db, mapping_id)
                key = load_plaintext_from_operand(c.key, registers, finalize_state)
                value = PlaintextValue(plaintext=load_plaintext_from_operand(c.value, registers, finalize_state))
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

            case Command.Type.RandChaCha:
                print(disasm_command(c))
                c: RandChaChaCommand
                additional_seeds = list(map(lambda x: PlaintextValue(plaintext=load_plaintext_from_operand(x, registers, finalize_state)), c.operands))
                chacha_seed = bytes(aleo.chacha_random_seed(
                    finalize_state.random_seed,
                    transition_id.dump(),
                    program.id.dump(),
                    function_name.dump(),
                    int(c.destination.locator),
                    c.destination_type.value,
                    additional_seeds,
                ))
                primitive_type = c.destination_type.primitive_type
                value = primitive_type.load(BytesIO(bytes(aleo.chacha_random_value(chacha_seed, c.destination_type.dump()))))
                res = LiteralPlaintext(
                    literal=Literal(
                        type_=Literal.Type(c.destination_type.value),
                        primitive=value,
                    )
                )
                store_plaintext_to_register(res, c.destination, registers)

            case _:
                raise NotImplementedError

    return operations




