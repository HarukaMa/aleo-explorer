import psycopg

from aleo_types import *
from db import Database
from interpreter.finalizer import execute_finalizer, ExecuteError, mapping_cache_read, mapping_find_index
from interpreter.utils import FinalizeState, MappingCacheTuple

global_mapping_cache: dict[Field, list[MappingCacheTuple]] = {}
global_program_cache: dict[str, Program] = {}

async def init_builtin_program(db: Database, program: Program):
    for mapping in program.mappings.keys():
        mapping_id = Field.loads(aleo.get_mapping_id(str(program.id), str(mapping)))
        await db.initialize_builtin_mapping(str(mapping_id), str(program.id), str(mapping))


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


async def finalize_execute(db: Database, finalize_state: FinalizeState, confirmed_transaction: ConfirmedTransaction,
                           mapping_cache: dict[Field, list[MappingCacheTuple]]) -> tuple[list[FinalizeOperation], list[dict[str, Any]], Optional[str]]:
    if isinstance(confirmed_transaction, AcceptedExecute):
        transaction = confirmed_transaction.transaction
        if not isinstance(transaction, ExecuteTransaction):
            raise TypeError("invalid execute transaction")
        execution = transaction.execution
        allow_state_change = True
        expected_operations = list(confirmed_transaction.finalize)
    elif isinstance(confirmed_transaction, RejectedExecute):
        if not isinstance(confirmed_transaction.rejected, RejectedExecution):
            raise TypeError("invalid rejected execute transaction")
        execution = confirmed_transaction.rejected.execution
        allow_state_change = False
        expected_operations = []
    else:
        raise NotImplementedError
    operations: list[dict[str, Any]] = []
    reject_reason: str | None = None
    for index, transition in enumerate(execution.transitions):
        finalize = transition.finalize.value
        if finalize is not None:
            if str(transition.program_id) in global_program_cache:
                program = global_program_cache[str(transition.program_id)]
            else:
                program_bytes = await db.get_program(str(transition.program_id))
                if program_bytes is None:
                    raise RuntimeError("program not found")
                program = Program.load(BytesIO(program_bytes))
                global_program_cache[str(transition.program_id)] = program
            inputs: list[Plaintext] = []
            for value in finalize:
                if isinstance(value, PlaintextValue):
                    inputs.append(value.plaintext)
                elif isinstance(value, RecordValue):
                    raise NotImplementedError
            try:
                operations.extend(
                    await execute_finalizer(db, finalize_state, transition.id, program, transition.function_name, inputs, mapping_cache, allow_state_change)
                )
            except ExecuteError as e:
                reject_reason = f"execute error: {e}, at transition #{index}, instruction \"{e.instruction}\""
                operations = []
                break
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
            expected_operations, operations, reject_reason = await finalize_execute(db, finalize_state, confirmed_transaction, global_mapping_cache)
        else:
            raise NotImplementedError

        for e, o in zip(expected_operations, operations):
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
                await db.update_mapping_key_value(cur, str(mapping_id), index, str(key_id), str(value_id), key.dump(), value.dump())
            case FinalizeOperation.Type.RemoveKeyValue:
                mapping_id = operation["mapping_id"]
                index = operation["index"]
                await db.remove_mapping_key_value(cur, str(mapping_id), index)
            case _:
                raise NotImplementedError

async def get_mapping_value(db: Database, program_id: str, mapping_name: str, key: str) -> Value:
    mapping_id = Field.loads(aleo.get_mapping_id(program_id, mapping_name))
    if mapping_id not in global_mapping_cache:
        global_mapping_cache[mapping_id] = await mapping_cache_read(db, mapping_id)
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
    key_id = Field.loads(aleo.get_key_id(str(mapping_id), key_plaintext.dump()))
    index = mapping_find_index(global_mapping_cache[mapping_id], key_id)
    if index == -1:
        raise ExecuteError(f"key {key} not found in mapping {mapping_id}", None, "")
    else:
        value = global_mapping_cache[mapping_id][index][3]
        if not isinstance(value, PlaintextValue):
            raise TypeError("invalid value type")
    return value

async def preview_finalize_execution(db: Database, program: Program, function_name: Identifier, inputs: list[Plaintext]) -> list[dict[str, Any]]:
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