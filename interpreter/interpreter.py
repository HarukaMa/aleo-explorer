import psycopg

from aleo_types import *
from db import Database
from interpreter.finalizer import execute_finalizer, ExecuteError, mapping_cache_read
from interpreter.utils import FinalizeState
from util.global_cache import global_mapping_cache, global_program_cache, MappingCacheDict


async def init_builtin_program(db: Database, program: Program):
    for mapping in program.mappings.keys():
        mapping_id = Field.loads(cached_get_mapping_id(str(program.id), str(mapping)))
        await db.initialize_builtin_mapping(str(mapping_id), str(program.id), str(mapping))
        if await db.get_program(str(program.id)) is None:
            await db.save_builtin_program(program)

async def finalize_deploy(confirmed_transaction: ConfirmedTransaction) -> tuple[list[FinalizeOperation], list[dict[str, Any]], Optional[str]]:
    if isinstance(confirmed_transaction, AcceptedDeploy):
        transaction: Transaction = confirmed_transaction.transaction
        if not isinstance(transaction, DeployTransaction):
            raise TypeError("invalid deploy transaction")
        deployment = transaction.deployment
        program = deployment.program
        expected_operations = confirmed_transaction.finalize
        operations: list[dict[str, Any]] = []
        for mapping in program.mappings.keys():
            mapping_id = Field.loads(aleo.get_mapping_id(str(program.id), str(mapping)))
            operations.append({
                "type": FinalizeOperation.Type.InitializeMapping,
                "mapping_id": mapping_id,
                "program_id": program.id,
                "mapping": mapping,
            })
    else:
        raise NotImplementedError
    return expected_operations, operations, None

async def _load_program(db: Database, program_id: str) -> Program:
    if program_id in global_program_cache:
        program = global_program_cache[program_id]
    else:
        program_bytes = await db.get_program(program_id)
        if program_bytes is None:
            raise RuntimeError("program not found")
        program = Program.load(BytesIO(program_bytes))
        global_program_cache[program_id] = program
    return program

def _load_input_from_arguments(arguments: list[Argument]) -> list[Value]:
    inputs: list[Value] = []
    for argument in arguments:
        if isinstance(argument, PlaintextArgument):
            inputs.append(PlaintextValue(plaintext=argument.plaintext))
        elif isinstance(argument, FutureArgument):
            inputs.append(FutureValue(future=argument.future))
        else:
            raise NotImplementedError
    return inputs

async def finalize_execute(db: Database, cur: psycopg.AsyncCursor[dict[str, Any]], finalize_state: FinalizeState,
                           confirmed_transaction: ConfirmedTransaction, mapping_cache: dict[Field, MappingCacheDict]
                           ) -> tuple[list[FinalizeOperation], list[dict[str, Any]], Optional[str]]:
    expected_operations = list(confirmed_transaction.finalize)
    if isinstance(confirmed_transaction, AcceptedExecute):
        transaction = confirmed_transaction.transaction
        if not isinstance(transaction, ExecuteTransaction):
            raise TypeError("invalid execute transaction")
        execution = transaction.execution
        allow_state_change = True
        fee = transaction.additional_fee.value
    elif isinstance(confirmed_transaction, RejectedExecute):
        if not isinstance(confirmed_transaction.rejected, RejectedExecution):
            raise TypeError("invalid rejected execute transaction")
        execution = confirmed_transaction.rejected.execution
        allow_state_change = False
        if not isinstance(confirmed_transaction.transaction, FeeTransaction):
            raise TypeError("invalid rejected execute transaction")
        fee = confirmed_transaction.transaction.fee
    else:
        raise NotImplementedError
    operations: list[dict[str, Any]] = []
    reject_reason: str | None = None
    for index, transition in enumerate(execution.transitions):
        maybe_future_output = transition.outputs[-1]
        if isinstance(maybe_future_output, FutureTransitionOutput):
            future_option = maybe_future_output.future
            if future_option.value is None:
                raise RuntimeError("invalid future is None")
            # noinspection PyTypeChecker
            future = future_option.value
            # TODO: use program cache
            program = await _load_program(db, str(future.program_id))

            inputs: list[Value] = _load_input_from_arguments(future.arguments)
            try:
                operations.extend(
                    await execute_finalizer(db, cur, finalize_state, transition.id, program, future.function_name, inputs, mapping_cache, allow_state_change)
                )
            except ExecuteError as e:
                reject_reason = f"execute error: {e}, at transition #{index}, instruction \"{e.instruction}\""
                operations = []
                break
    if fee:
        transition = fee.transition
        if transition.function_name == "fee_public":
            output = transition.outputs[0]
            if not isinstance(output, FutureTransitionOutput):
                raise TypeError("invalid fee transition output")
            future = output.future.value
            if future is None:
                raise RuntimeError("invalid fee transition output")
            program = await _load_program(db, str(future.program_id))

            inputs: list[Value] = _load_input_from_arguments(future.arguments)
            operations.extend(
                await execute_finalizer(db, cur, finalize_state, transition.id, program, future.function_name, inputs, mapping_cache, allow_state_change)
            )
    if isinstance(confirmed_transaction, RejectedExecute):
        if reject_reason is None:
            raise RuntimeError("rejected execute transaction should not finalize without ExecuteError")
    return expected_operations, operations, reject_reason

async def finalize_block(db: Database, cur: psycopg.AsyncCursor[dict[str, Any]], block: Block) -> list[Optional[str]]:
    finalize_state = FinalizeState(block)
    reject_reasons: list[Optional[str]] = []
    for confirmed_transaction in block.transactions.transactions:
        confirmed_transaction: ConfirmedTransaction
        CTType = ConfirmedTransaction.Type
        if confirmed_transaction.type in [CTType.AcceptedDeploy, CTType.RejectedDeploy]:
            expected_operations, operations, reject_reason = await finalize_deploy(confirmed_transaction)
        elif confirmed_transaction.type in [CTType.AcceptedExecute, CTType.RejectedExecute]:
            expected_operations, operations, reject_reason = await finalize_execute(db, cur, finalize_state, confirmed_transaction, global_mapping_cache)
        else:
            raise NotImplementedError

        if len(expected_operations) != len(operations):
            raise TypeError("invalid finalize operation length")

        for e, o in zip(expected_operations, operations):
            try:
                if e.type != o["type"]:
                    raise TypeError("invalid finalize operation type")
                if e.mapping_id != o["mapping_id"]:
                    raise TypeError("invalid finalize mapping id")
                if isinstance(e, InitializeMapping):
                    pass
                elif isinstance(e, UpdateKeyValue):
                    if e.index != o["index"] or e.key_id != o["key_id"] or e.value_id != o["value_id"]:
                        raise TypeError("invalid finalize operation")
                elif isinstance(e, RemoveKeyValue):
                    if e.index != o["index"]:
                        raise TypeError("invalid finalize operation")
                else:
                    raise NotImplementedError
            except TypeError:
                from pprint import pprint
                print("expected:", e.__dict__)
                print("actual:", o)
                if e.mapping_id in global_mapping_cache:
                    print("mapping cache:")
                    pprint(global_mapping_cache[e.mapping_id])
                # global_mapping_cache.clear()
                raise

        await execute_operations(db, cur, operations)
        reject_reasons.append(reject_reason)
    return reject_reasons


async def execute_operations(db: Database, cur: psycopg.AsyncCursor[dict[str, Any]], operations: list[dict[str, Any]]):
    for operation in operations:
        match operation["type"]:
            case FinalizeOperation.Type.InitializeMapping:
                mapping_id = operation["mapping_id"]
                program_id = operation["program_id"]
                mapping = operation["mapping"]
                await db.initialize_mapping(cur, str(mapping_id), str(program_id), str(mapping))
            case FinalizeOperation.Type.UpdateKeyValue:
                mapping_id = operation["mapping_id"]
                index = operation["index"]
                key_id = operation["key_id"]
                value_id = operation["value_id"]
                key = operation["key"]
                value = operation["value"]
                program_name = operation["program_name"]
                mapping_name = operation["mapping_name"]
                await db.update_mapping_key_value(cur, program_name, mapping_name, str(mapping_id), index, str(key_id), str(value_id), key.dump(), value.dump(), operation["height"])
            case FinalizeOperation.Type.RemoveKeyValue:
                mapping_id = operation["mapping_id"]
                index = operation["index"]
                program_name = operation["program_name"]
                mapping_name = operation["mapping_name"]
                await db.remove_mapping_key_value(cur, program_name, mapping_name, str(mapping_id), index, operation["height"])
            case _:
                raise NotImplementedError

async def get_mapping_value(db: Database, program_id: str, mapping_name: str, key: str) -> Value:
    mapping_id = Field.loads(cached_get_mapping_id(program_id, mapping_name))
    if mapping_id not in global_mapping_cache:
        global_mapping_cache[mapping_id] = await mapping_cache_read(db, program_id, mapping_name)
    if str(program_id) in global_program_cache:
        program = global_program_cache[str(program_id)]
    else:
        program_bytes = await db.get_program(str(program_id))
        if program_bytes is None:
            raise RuntimeError("program not found")
        program = Program.load(BytesIO(program_bytes))
        global_program_cache[str(program_id)] = program
    mapping = program.mappings[Identifier(value=mapping_name)]
    mapping_key_type = mapping.key.plaintext_type
    if not isinstance(mapping_key_type, LiteralPlaintextType):
        raise TypeError("unsupported key type")
    key_plaintext = LiteralPlaintext(literal=Literal.loads(Literal.Type(mapping_key_type.literal_type.value), key))
    key_id = Field.loads(aleo.get_key_id(program_id, mapping_name, key_plaintext.dump()))
    if key_id not in global_mapping_cache[mapping_id]:
        raise ExecuteError(f"key {key} not found in mapping {mapping_id}", None, "")
    else:
        value = global_mapping_cache[mapping_id][key_id]["value"]
        if not isinstance(value, PlaintextValue):
            raise TypeError("invalid value type")
    return value

async def preview_finalize_execution(db: Database, program: Program, function_name: Identifier, inputs: list[Value]) -> list[dict[str, Any]]:
    block = await db.get_latest_block()
    finalize_state = FinalizeState(block)
    return await execute_finalizer(
        db,
        finalize_state,
        TransitionID.load(BytesIO(b"\x00" * 32)),
        program,
        function_name,
        inputs,
        mapping_cache=None,
        allow_state_change=False,
    )