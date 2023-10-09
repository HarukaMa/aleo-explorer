import os
import time

import psycopg

from aleo_types import *
from db import Database
from disasm.aleo import disasm_instruction, disasm_command
from .environment import Registers
from .instruction import execute_instruction
from .utils import load_plaintext_from_operand, store_plaintext_to_register, FinalizeState, MappingCacheDict


async def mapping_cache_read(db: Database, mapping_id: Field) -> MappingCacheDict:
    print(f"Reading mapping cache {mapping_id}")
    return await db.get_mapping_cache(str(mapping_id))

async def mapping_cache_read_with_cur(db: Database, cur: psycopg.AsyncCursor[dict[str, Any]], mapping_id: Field
                                      ) -> MappingCacheDict:
    print(f"Reading mapping cache {mapping_id}")
    return await db.get_mapping_cache_with_cur(cur, str(mapping_id))

class ExecuteError(Exception):
    def __init__(self, message: str, exception: Optional[Exception], instruction: str):
        super().__init__(message)
        self.original_exception = exception
        self.instruction = instruction


async def execute_finalizer(db: Database, cur: psycopg.AsyncCursor[dict[str, Any]], finalize_state: FinalizeState,
                            transition_id: TransitionID, program: Program,
                            function_name: Identifier, inputs: list[Value],
                            mapping_cache: Optional[dict[Field, MappingCacheDict]],
                            allow_state_change: bool) -> list[dict[str, Any]]:
    registers = Registers()
    operations: list[dict[str, Any]] = []
    function = program.functions[function_name]
    if function.finalize.value is None:
        raise ValueError("invalid finalize function")
    finalize = function.finalize.value

    if len(inputs) != len(finalize.inputs):
        raise TypeError("invalid number of inputs")
    for fi, i in zip(finalize.inputs, inputs):
        if fi.finalize_type.type.name != i.type.name:
            raise TypeError("invalid input type")
        ir = fi.register
        if not isinstance(ir, LocatorRegister):
            raise TypeError("invalid input register type")
        registers[int(ir.locator)] = i

    debug = os.environ.get("DEBUG", False)
    timer = time.perf_counter_ns()

    print(f"finalize {program.id}/{function_name}({', '.join(str(i) for i in registers)})")

    for c in finalize.commands:
        if debug:
            if isinstance(c, InstructionCommand):
                print(disasm_instruction(c.instruction))
            else:
                print(disasm_command(c))

        if isinstance(c, InstructionCommand):
            instruction = c.instruction
            try:
                execute_instruction(instruction, program, registers, finalize_state)
            except (AssertionError, OverflowError, ZeroDivisionError) as e:
                raise ExecuteError(str(e), e, disasm_instruction(instruction))
            except Exception:
                registers.dump()
                raise

        elif isinstance(c, ContainsCommand):
            mapping_id = Field.loads(aleo.get_mapping_id(str(program.id), str(c.mapping)))
            if mapping_cache is not None and mapping_id not in mapping_cache:
                mapping_cache[mapping_id] = await mapping_cache_read_with_cur(db, cur, mapping_id)
            key = load_plaintext_from_operand(c.key, registers, finalize_state)
            key_id = Field.loads(aleo.get_key_id(str(mapping_id), key.dump()))
            if mapping_cache:
                contains = key_id in mapping_cache[mapping_id]
            else:
                value = await db.get_mapping_value(str(program.id), str(mapping_id), str(key_id))
                contains = value is not None
            value = PlaintextValue(
                plaintext=LiteralPlaintext(
                    literal=Literal(
                        type_=Literal.Type.Boolean,
                        primitive=bool_(contains)
                    )
                )
            )
            destination = c.destination
            store_plaintext_to_register(value.plaintext, destination, registers)

        elif isinstance(c, GetCommand | GetOrUseCommand):
            mapping_id = Field.loads(aleo.get_mapping_id(str(program.id), str(c.mapping)))
            if mapping_cache is not None and mapping_id not in mapping_cache:
                mapping_cache[mapping_id] = await mapping_cache_read_with_cur(db, cur, mapping_id)
            key = load_plaintext_from_operand(c.key, registers, finalize_state)
            key_id = Field.loads(aleo.get_key_id(str(mapping_id), key.dump()))
            if mapping_cache:
                if key_id not in mapping_cache[mapping_id]:
                    if isinstance(c, GetCommand):
                        raise ExecuteError(f"key {key} not found in mapping {c.mapping}", None, disasm_command(c))
                    default = load_plaintext_from_operand(c.default, registers, finalize_state)
                    value = PlaintextValue(plaintext=default)
                else:
                    value = mapping_cache[mapping_id][key_id]["value"]
                    print(f"get {c.mapping}[{key}, {mapping_cache[mapping_id][key_id]['index']}] = {value}")
                    if not isinstance(value, PlaintextValue):
                        raise TypeError("invalid value type")
            else:
                value = await db.get_mapping_value(str(program.id), str(mapping_id), str(key_id))
                if value is None:
                    if isinstance(c, GetCommand):
                        raise ExecuteError(f"key {key} not found in mapping {c.mapping}", None, disasm_command(c))
                    default = load_plaintext_from_operand(c.default, registers, finalize_state)
                    value = PlaintextValue(plaintext=default)
                else:
                    value = Value.load(BytesIO(value))
                    if not isinstance(value, PlaintextValue):
                        raise TypeError("invalid value type")
            destination = c.destination
            store_plaintext_to_register(value.plaintext, destination, registers)

        elif isinstance(c, SetCommand):
            mapping_id = Field.loads(aleo.get_mapping_id(str(program.id), str(c.mapping)))
            if mapping_cache is not None and mapping_id not in mapping_cache:
                mapping_cache[mapping_id] = await mapping_cache_read_with_cur(db, cur, mapping_id)
            key = load_plaintext_from_operand(c.key, registers, finalize_state)
            value = PlaintextValue(plaintext=load_plaintext_from_operand(c.value, registers, finalize_state))
            key_id = Field.loads(aleo.get_key_id(str(mapping_id), key.dump()))
            value_id = Field.loads(aleo.get_value_id(str(key_id), value.dump()))
            if mapping_cache:
                if allow_state_change:
                    if key_id not in mapping_cache[mapping_id]:
                        index = len(mapping_cache[mapping_id])
                        mapping_cache[mapping_id][key_id] = {
                            "value_id": value_id,
                            "key": key,
                            "value": value,
                            "index": index,
                        }
                        print(f"new {c.mapping}[{key}, {index}] = {value}")
                    else:
                        index = mapping_cache[mapping_id][key_id]['index']
                        mapping_cache[mapping_id][key_id]["value_id"] = value_id
                        mapping_cache[mapping_id][key_id]["value"] = value
                        print(f"set {c.mapping}[{key}, {index}] = {value}")
                else:
                    index = mapping_cache[mapping_id][key_id]['index'] if key_id in mapping_cache[mapping_id] else len(mapping_cache[mapping_id])
                    print("Not updating mapping cache because allow_state_change is False")
            else:
                index = await db.get_mapping_index_by_key(str(program.id), str(mapping_id), str(key_id))
                if index is None:
                    index = await db.get_mapping_size(str(program.id), str(mapping_id))
                if allow_state_change:
                    raise RuntimeError("unsupported execution configuration")
                else:
                    print("Not updating database because allow_state_change is False")

            operations.append({
                "type": FinalizeOperation.Type.UpdateKeyValue,
                "mapping_id": mapping_id,
                "index": index,
                "key_id": key_id,
                "value_id": value_id,
                "mapping": c.mapping,
                "key": key,
                "value": value,
                "height": finalize_state.block_height,
            })

        elif isinstance(c, RandChaChaCommand):
            additional_seeds = list(map(lambda x: PlaintextValue(plaintext=load_plaintext_from_operand(x, registers, finalize_state)).dump(), c.operands))
            chacha_seed = aleo.chacha_random_seed(
                finalize_state.random_seed,
                transition_id.dump(),
                program.id.dump(),
                function_name.dump(),
                int(c.destination.locator),
                c.destination_type.value,
                additional_seeds,
            )
            primitive_type = c.destination_type.primitive_type
            value = primitive_type.load(BytesIO(aleo.chacha_random_value(chacha_seed, c.destination_type)))
            res = LiteralPlaintext(
                literal=Literal(
                    type_=Literal.Type(c.destination_type.value),
                    primitive=value,
                )
            )
            store_plaintext_to_register(res, c.destination, registers)

        elif isinstance(c, RemoveCommand):
            mapping_id = Field.loads(aleo.get_mapping_id(str(program.id), str(c.mapping)))
            if mapping_cache is not None and mapping_id not in mapping_cache:
                mapping_cache[mapping_id] = await mapping_cache_read_with_cur(db, cur, mapping_id)
            key = load_plaintext_from_operand(c.key, registers, finalize_state)
            key_id = Field.loads(aleo.get_key_id(str(mapping_id), key.dump()))
            if mapping_cache:
                if key_id not in mapping_cache[mapping_id]:
                    print(f"Key {key} not found in mapping {c.mapping}")
                    continue
                if allow_state_change:
                    index = mapping_cache[mapping_id][key_id]['index']
                    if index != len(mapping_cache[mapping_id]) - 1:
                        for key_id, kv in mapping_cache[mapping_id].items():
                            if kv['index'] == len(mapping_cache[mapping_id]) - 1:
                                mapping_cache[mapping_id][key_id]['index'] = index
                                break
                    mapping_cache[mapping_id].pop(key_id)
                    print(f"del {c.mapping}[{key}, {index}]")
                else:
                    print("Not updating mapping cache because allow_state_change is False")
            else:
                index = await db.get_mapping_index_by_key(str(program.id), str(mapping_id), str(key_id))
                if index is None:
                    print(f"Key {key} not found in mapping {c.mapping}")
                    continue
                if allow_state_change:
                    raise RuntimeError("unsupported execution configuration")
                else:
                    print("Not updating database because allow_state_change is False")
            operations.append({
                "type": FinalizeOperation.Type.RemoveKeyValue,
                "mapping_id": mapping_id,
                "index": index,
                "mapping": c.mapping,
            })

        else:
            raise NotImplementedError

        if debug:
            registers.dump()
    print(f"execution took {time.perf_counter_ns() - timer} ns")
    return operations




