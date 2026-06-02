import os
import time
from typing import ParamSpec, Awaitable

import psycopg
from aleo_explorer_rust import RustExecuteError

from aleo_types import *
from aleo_types.cached import cached_get_key_id, cached_get_mapping_id
from db import Database
from disasm.aleo import disasm_instruction, disasm_command
from util.global_cache import MappingCacheDict, get_program, MappingCache
from .environment import Registers
from .instruction import execute_instruction
from .utils import load_plaintext_from_operand, store_plaintext_to_register, FinalizeState, load_future_from_register, resolve_dynamic_program_mapping

try:
    from line_profiler import profile  # pyright: ignore [reportUnknownVariableType, reportMissingImports]
except ImportError:
    P = ParamSpec('P')
    R = TypeVar('R')
    def profile(func: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            return await func(*args, **kwargs)
        return wrapper


async def mapping_cache_read(db: Database, program_name: str, mapping_name: str) -> MappingCacheDict:
    return await db.get_mapping_cache(program_name, mapping_name)

async def mapping_cache_read_with_cur(db: Database, cur: psycopg.AsyncCursor[dict[str, Any]], program_name: str,
                                      mapping_name: str) -> MappingCacheDict:
    return await db.get_mapping_cache_with_cur(cur, program_name, mapping_name)

class ExecuteError(Exception):
    def __init__(self, message: str, exception: Optional[Exception], instruction: str, transition_index: int,
                 program: Optional[str] = None, function_name: Optional[str] = None):
        super().__init__(message)
        self.original_exception = exception
        self.instruction = instruction
        self.transition_index = transition_index
        self.program = program
        self.function_name = function_name


@profile  # pyright: ignore [reportUntypedFunctionDecorator]
async def execute_finalizer(db: Database, cur: Optional[psycopg.AsyncCursor[dict[str, Any]]], finalize_state: FinalizeState,
                            transitions: list[TransitionID], transition_index_executed: set[int],
                            program: Program, function_name: Identifier, inputs: list[Value],
                            mapping_cache: MappingCache, local_mapping_cache: dict[Field, MappingCacheDict],
                            allow_state_change: bool, deploy_owner: Optional[Address] = None,
                            dynamic_future_map: Optional[dict[tuple[Field, Field, Field, Field], tuple[Future, TransitionID]]] = None,
                            ) -> list[dict[str, Any]]:
    transition_index = len(transition_index_executed)
    transition_index_executed.add(transition_index)
    registers = Registers()
    registers.owner = deploy_owner
    if function_name == "constructor":
        finalize = program.constructor.value
        if finalize is None:
            raise ValueError("no constructor on program")
        if len(inputs) != 0:
            raise TypeError("invalid number of inputs")
    else:
        function = program.functions[function_name]
        if function.finalize.value is None:
            raise ValueError("invalid finalize function")
        finalize = function.finalize.value

        if len(inputs) != len(finalize.inputs):
            raise TypeError("invalid number of inputs")
        for fi, i in zip(finalize.inputs, inputs):
            if isinstance(fi.finalize_type, DynamicFutureFinalizeType):
                if not isinstance(i, (FutureValue, DynamicFutureValue)):
                    raise TypeError("invalid input type: expected future or dynamic future")
            elif fi.finalize_type.type.name != i.type.name:
                raise TypeError("invalid input type")
            ir = fi.register
            if not isinstance(ir, LocatorRegister):
                raise TypeError("invalid input register type")
            registers[int(ir.locator)] = i

    return await _execute_commands(
        db, cur, finalize_state, transitions, transition_index, transition_index_executed,
        program, function_name, finalize.commands, finalize.positions, registers,
        mapping_cache, local_mapping_cache, allow_state_change, dynamic_future_map,
    )


@profile  # pyright: ignore [reportUntypedFunctionDecorator]
async def _execute_commands(db: Database, cur: Optional[psycopg.AsyncCursor[dict[str, Any]]], finalize_state: FinalizeState,
                            transitions: list[TransitionID], transition_index: int, transition_index_executed: set[int],
                            program: Program, function_name: Identifier, commands: list[Command], positions: dict[Identifier, int],
                            registers: Registers, mapping_cache: MappingCache, local_mapping_cache: dict[Field, MappingCacheDict],
                            allow_state_change: bool,
                            dynamic_future_map: Optional[dict[tuple[Field, Field, Field, Field], tuple[Future, TransitionID]]] = None,
                            ) -> list[dict[str, Any]]:
    operations: list[dict[str, Any]] = []
    debug = os.environ.get("DEBUG", False)
    timer = time.perf_counter_ns()

    if debug:
        print(f"finalize {program.id}/{function_name}({', '.join(str(i) for i in registers)})")

    pc = 0

    def load_mapping_cache_id(program_id_: ProgramID, mapping_: Identifier):
        mapping_id_ = Field.loads(cached_get_mapping_id(str(program_id_), str(mapping_)))
        if not allow_state_change and mapping_id_ not in local_mapping_cache:
            local_mapping_cache[mapping_id_] = {}
        return mapping_id_

    while pc < len(commands):
        c = commands[pc]
        if debug:
            if isinstance(c, InstructionCommand):
                print(disasm_instruction(c.instruction))
            else:
                print(disasm_command(c))

        try:
            if isinstance(c, InstructionCommand):
                instruction = c.instruction
                if isinstance(instruction.literals, CallInstruction):
                    await _invoke_view(
                        instruction.literals, db, cur, finalize_state, transitions, transition_index,
                        transition_index_executed, program, registers, mapping_cache, local_mapping_cache,
                        allow_state_change, dynamic_future_map,
                    )
                else:
                    try:
                        await execute_instruction(instruction, program, registers, finalize_state, db)
                    except (AssertionError, OverflowError, ZeroDivisionError, RustExecuteError) as e:
                        raise ExecuteError(str(e), e, disasm_instruction(instruction), transition_index, str(program.id), str(function_name))
                    except Exception:
                        registers.dump()
                        raise

            elif isinstance(c, ContainsCommand):
                operator = c.mapping
                if isinstance(operator, LocatorCallOperator):
                    program_id = operator.locator.id
                    mapping = operator.locator.resource
                elif isinstance(operator, ResourceCallOperator):
                    program_id = program.id
                    mapping = operator.resource
                else:
                    raise TypeError("invalid locator type")
                mapping_id = load_mapping_cache_id(program_id, mapping)
                key = await load_plaintext_from_operand(c.key, registers, finalize_state, db, program)
                key_id = Field.loads(cached_get_key_id(str(program_id), str(mapping), key.dump()))
                if not allow_state_change and key_id in local_mapping_cache[mapping_id]:
                    contains = local_mapping_cache[mapping_id][key_id]["value"] is not None
                else:
                    data = await mapping_cache[mapping_id][key_id]
                    contains = data is not None

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
                operator = c.mapping
                if isinstance(operator, LocatorCallOperator):
                    program_id = operator.locator.id
                    mapping = operator.locator.resource
                elif isinstance(operator, ResourceCallOperator):
                    program_id = program.id
                    mapping = operator.resource
                else:
                    raise TypeError("invalid locator type")
                mapping_id = load_mapping_cache_id(program_id, mapping)
                key = await load_plaintext_from_operand(c.key, registers, finalize_state, db, program)
                key_id = Field.loads(cached_get_key_id(str(program_id), str(mapping), key.dump()))

                if not allow_state_change and key_id in local_mapping_cache[mapping_id]:
                    if local_mapping_cache[mapping_id][key_id]["value"] is None:
                        if isinstance(c, GetCommand):
                            raise ExecuteError(f"key {key} not found in mapping {mapping}", None, disasm_command(c), transition_index, str(program.id), str(function_name))
                        default = await load_plaintext_from_operand(c.default, registers, finalize_state, db, program)
                        value = PlaintextValue(plaintext=default)
                    else:
                        value = local_mapping_cache[mapping_id][key_id]["value"]
                else:
                    data = await mapping_cache[mapping_id][key_id]
                    if data is None:
                        if isinstance(c, GetCommand):
                            raise ExecuteError(f"key {key} not found in mapping {mapping}", None, disasm_command(c), transition_index, str(program.id), str(function_name))
                        default = await load_plaintext_from_operand(c.default, registers, finalize_state, db, program)
                        value = PlaintextValue(plaintext=default)
                    else:
                        value = data["value"]
                if debug:
                    print(f"get {mapping}[{key}] = {value}")
                if not isinstance(value, PlaintextValue):
                    raise TypeError("invalid value type")
                destination = c.destination
                store_plaintext_to_register(value.plaintext, destination, registers)

            elif isinstance(c, SetCommand):
                mapping_id = load_mapping_cache_id(program.id, c.mapping)
                key = await load_plaintext_from_operand(c.key, registers, finalize_state, db, program)
                value = PlaintextValue(plaintext=await load_plaintext_from_operand(c.value, registers, finalize_state, db, program))
                key_id = Field.loads(cached_get_key_id(str(program.id), str(c.mapping), key.dump()))
                value_id = Field.loads(aleo_explorer_rust.get_value_id(str(key_id), value.dump()))
                if allow_state_change:
                    mapping_cache[mapping_id][key_id] = {
                        "key": key,
                        "value": value,
                    }
                else:
                    local_mapping_cache[mapping_id][key_id] = {
                        "key": key,
                        "value": value,
                    }
                if debug:
                    print(f"set {c.mapping}[{key}] = {value}")
                operations.append({
                    "type": FinalizeOperation.Type.UpdateKeyValue,
                    "program_name": str(program.id),
                    "mapping_id": mapping_id,
                    "key_id": key_id,
                    "value_id": value_id,
                    "mapping_name": c.mapping,
                    "key": key,
                    "value": value,
                    "height": finalize_state.block_height,
                    "from_transaction": True,
                })

            elif isinstance(c, RandChaChaCommand):
                from node import Network

                additional_seeds: list[bytes] = []
                for operand in c.operands:
                    additional_seeds.append(PlaintextValue(plaintext=await load_plaintext_from_operand(operand, registers, finalize_state, db, program)).dump())
                if finalize_state.block_height >= Network.consensus_v3_height:
                    rand_transition_index = 0
                else:
                    rand_transition_index = transition_index
                chacha_seed = aleo_explorer_rust.chacha_random_seed(
                    finalize_state.random_seed,
                    transitions[rand_transition_index].dump(),
                    program.id.dump(),
                    function_name.dump(),
                    int(c.destination.locator),
                    c.destination_type.value,
                    additional_seeds,
                    finalize_state.block_height >= Network.consensus_v3_height,
                    transition_index,
                )
                primitive_type = c.destination_type.primitive_type
                value = primitive_type.load(BytesIO(aleo_explorer_rust.chacha_random_value(chacha_seed, c.destination_type)))
                res = LiteralPlaintext(
                    literal=Literal(
                        type_=Literal.Type(c.destination_type.value),
                        primitive=value,
                    )
                )
                store_plaintext_to_register(res, c.destination, registers)

            elif isinstance(c, RemoveCommand):
                mapping_id = load_mapping_cache_id(program.id, c.mapping)
                key = await load_plaintext_from_operand(c.key, registers, finalize_state, db, program)
                key_id = Field.loads(cached_get_key_id(str(program.id), str(c.mapping), key.dump()))
                if allow_state_change:
                    # accept
                    if (await mapping_cache[mapping_id][key_id]) is None:
                        # accept and not exist
                        print(f"Key {key} not found in mapping {c.mapping}")
                        pc += 1
                        continue
                    # accept and exist
                    mapping_cache[mapping_id][key_id] = None
                else:
                    # reject
                    if key_id not in local_mapping_cache[mapping_id]:
                        # reject and not in cache
                        if (await mapping_cache[mapping_id][key_id]) is None:
                            # reject and not in cache and not exist
                            print(f"Key {key} not found in mapping {c.mapping}")
                            pc += 1
                            continue
                        # reject and not in cache and exist
                        local_mapping_cache[mapping_id][key_id] = {
                            "key": key,
                            "value": None,
                        }
                    else:
                        # reject and in cache
                        if local_mapping_cache[mapping_id][key_id]["value"] is None:
                            # reject and in cache and not exist
                            print(f"Key {key} not found in mapping {c.mapping}")
                            pc += 1
                            continue
                        # reject and in cache and exist
                        local_mapping_cache[mapping_id][key_id]["value"] = None

                if debug:
                    print(f"del {c.mapping}[{key}]")
                operations.append({
                    "type": FinalizeOperation.Type.RemoveKeyValue,
                    "program_name": str(program.id),
                    "mapping_id": mapping_id,
                    "mapping_name": c.mapping,
                    "key_id": key_id,
                    "key": key,
                    "height": finalize_state.block_height,
                    "from_transaction": True,
                })

            elif isinstance(c, (BranchEqCommand, BranchNeqCommand)):
                first = await load_plaintext_from_operand(c.first, registers, finalize_state, db, program)
                second = await load_plaintext_from_operand(c.second, registers, finalize_state, db, program)
                if (first == second and isinstance(c, BranchEqCommand)) or (first != second and isinstance(c, BranchNeqCommand)):
                    pc = positions[c.position]
                    continue

            elif isinstance(c, PositionCommand):
                pass

            elif isinstance(c, AwaitCommand):
                register = c.register
                if not isinstance(register, LocatorRegister):
                    raise ValueError("register is not locator")
                reg_value = registers[int(register.locator)]

                if isinstance(reg_value, DynamicFutureValue):
                    if dynamic_future_map is None:
                        raise RuntimeError("dynamic future encountered but no dynamic_future_map provided")
                    key = reg_value.dynamic_future.key()
                    if key not in dynamic_future_map:
                        raise RuntimeError("dynamic future key not found in map")
                    call_future, _transition_id = dynamic_future_map[key]
                else:
                    call_future = load_future_from_register(c.register, registers, finalize_state)

                call_program_id = call_future.program_id
                latest_edition = await db.get_program_latest_edition(str(call_program_id))
                if latest_edition is None:
                    raise RuntimeError("program not found")
                call_program = await get_program(db, str(call_program_id), latest_edition)
                if not call_program:
                    raise RuntimeError("program not found")

                from interpreter.interpreter import load_input_from_arguments
                call_inputs: list[Value] = load_input_from_arguments(call_future.arguments)

                operations.extend(
                    await execute_finalizer(db, cur, finalize_state, transitions, transition_index_executed, call_program, call_future.function_name, call_inputs, mapping_cache, local_mapping_cache, allow_state_change, dynamic_future_map=dynamic_future_map)
                )

            elif isinstance(c, ContainsDynamicCommand):
                program_id, mapping = await resolve_dynamic_program_mapping(list(c.operands[:3]), registers, finalize_state, db, program)
                mapping_id = load_mapping_cache_id(program_id, mapping)
                key = await load_plaintext_from_operand(c.operands[3], registers, finalize_state, db, program)
                key_id = Field.loads(cached_get_key_id(str(program_id), str(mapping), key.dump()))
                if not allow_state_change and key_id in local_mapping_cache[mapping_id]:
                    contains = local_mapping_cache[mapping_id][key_id]["value"] is not None
                else:
                    data = await mapping_cache[mapping_id][key_id]
                    contains = data is not None
                value = PlaintextValue(
                    plaintext=LiteralPlaintext(
                        literal=Literal(type_=Literal.Type.Boolean, primitive=bool_(contains))
                    )
                )
                store_plaintext_to_register(value.plaintext, c.destination, registers)

            elif isinstance(c, GetDynamicCommand | GetOrUseDynamicCommand):
                program_id, mapping = await resolve_dynamic_program_mapping(list(c.operands[:3]), registers, finalize_state, db, program)
                mapping_id = load_mapping_cache_id(program_id, mapping)
                key = await load_plaintext_from_operand(c.operands[3], registers, finalize_state, db, program)
                key_id = Field.loads(cached_get_key_id(str(program_id), str(mapping), key.dump()))

                if not allow_state_change and key_id in local_mapping_cache[mapping_id]:
                    if local_mapping_cache[mapping_id][key_id]["value"] is None:
                        if isinstance(c, GetDynamicCommand):
                            raise ExecuteError(f"key {key} not found in mapping {program_id}/{mapping}", None, disasm_command(c), transition_index, str(program.id), str(function_name))
                        default = await load_plaintext_from_operand(c.operands[4], registers, finalize_state, db, program)
                        value = PlaintextValue(plaintext=default)
                    else:
                        value = local_mapping_cache[mapping_id][key_id]["value"]
                else:
                    data = await mapping_cache[mapping_id][key_id]
                    if data is None:
                        if isinstance(c, GetDynamicCommand):
                            raise ExecuteError(f"key {key} not found in mapping {program_id}/{mapping}", None, disasm_command(c), transition_index, str(program.id), str(function_name))
                        default = await load_plaintext_from_operand(c.operands[4], registers, finalize_state, db, program)
                        value = PlaintextValue(plaintext=default)
                    else:
                        value = data["value"]
                if not isinstance(value, PlaintextValue):
                    raise TypeError("invalid value type")
                store_plaintext_to_register(value.plaintext, c.destination, registers)

            else:
                raise NotImplementedError

        except IndexError as e:
            raise ExecuteError(f"r{e} does not exist", e, disasm_command(c), transition_index, str(program.id), str(function_name))

        pc += 1

        if debug:
            registers.dump()
    if debug:
        print(f"execution took {time.perf_counter_ns() - timer} ns")
    return operations


async def _invoke_view(call: CallInstruction, db: Database, cur: Optional[psycopg.AsyncCursor[dict[str, Any]]],
                       finalize_state: FinalizeState, transitions: list[TransitionID], transition_index: int,
                       transition_index_executed: set[int], program: Program, caller_registers: Registers,
                       mapping_cache: MappingCache, local_mapping_cache: dict[Field, MappingCacheDict],
                       allow_state_change: bool,
                       dynamic_future_map: Optional[dict[tuple[Field, Field, Field, Field], tuple[Future, TransitionID]]] = None,
                       ):
    # A `call` in a finalize body always targets a view (V15): resource = a view in the current
    # program, locator = a view in an imported program. The view is a read-only leaf - it cannot
    # call, write, await, or use rand - so it emits no finalize operations and never re-enters here.
    operator = call.operator
    if isinstance(operator, LocatorCallOperator):
        callee_id = operator.locator.id
        view_name = operator.locator.resource
        latest_edition = await db.get_program_latest_edition(str(callee_id))
        if latest_edition is None:
            raise RuntimeError("program not found")
        callee_program = await get_program(db, str(callee_id), latest_edition)
        if callee_program is None:
            raise RuntimeError("program not found")
    elif isinstance(operator, ResourceCallOperator):
        callee_id = program.id
        view_name = operator.resource
        callee_program = program
    else:
        raise TypeError("invalid call operator")

    if view_name not in callee_program.views:
        raise RuntimeError(f"view {view_name} not found in program {callee_id}")
    view = callee_program.views[view_name]
    if len(call.operands) != len(view.inputs):
        raise TypeError("invalid number of view inputs")

    view_registers = Registers()
    for view_input, operand in zip(view.inputs, call.operands):
        input_register = view_input.register
        if not isinstance(input_register, LocatorRegister):
            raise TypeError("invalid view input register type")
        input_plaintext = await load_plaintext_from_operand(operand, caller_registers, finalize_state, db, program)
        view_registers[int(input_register.locator)] = PlaintextValue(plaintext=input_plaintext)

    # Returned operations are always empty (views cannot write), so they are intentionally dropped.
    await _execute_commands(
        db, cur, finalize_state, transitions, transition_index, transition_index_executed,
        callee_program, view_name, view.commands, view.positions, view_registers,
        mapping_cache, local_mapping_cache, allow_state_change, dynamic_future_map,
    )

    if len(call.destinations) != len(view.outputs):
        raise TypeError("invalid number of view outputs")
    for destination, view_output in zip(call.destinations, view.outputs):
        output_plaintext = await load_plaintext_from_operand(view_output.operand, view_registers, finalize_state, db, callee_program)
        store_plaintext_to_register(output_plaintext, destination, caller_registers)




