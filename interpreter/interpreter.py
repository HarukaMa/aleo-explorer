from db import Database
from interpreter.finalizer import execute_finalizer
from node.types import *

async def init_builtin_program(db: Database, program: Program):
    for mapping in program.mappings.keys():
        mapping_id = Field.loads(aleo.get_mapping_id(str(program.id), str(mapping)))
        await db.initialize_builtin_mapping(str(mapping_id), str(program.id), str(mapping))


async def finalize_block(db: Database, cur, block: Block):
    for confirmed_transaction in block.transactions.transactions:
        confirmed_transaction: ConfirmedTransaction
        if confirmed_transaction.type == ConfirmedTransaction.Type.AcceptedDeploy:
            confirmed_transaction: AcceptedDeploy
            transaction: Transaction = confirmed_transaction.transaction
            transaction: DeployTransaction
            deployment = transaction.deployment
            program = deployment.program
            expected_operations = confirmed_transaction.finalize
            operations = []
            for mapping in program.mappings.keys():
                mapping_id = Field.loads(aleo.get_mapping_id(str(program.id), str(mapping)))
                operations.append({
                    "type": FinalizeOperation.Type.InitializeMapping,
                    "mapping_id": mapping_id,
                    "program_id": program.id,
                    "mapping": mapping,
                })
        elif confirmed_transaction.type == ConfirmedTransaction.Type.AcceptedExecute:
            confirmed_transaction: AcceptedExecute
            transaction: Transaction = confirmed_transaction.transaction
            transaction: ExecuteTransaction
            execution = transaction.execution
            mapping_cache = {}
            expected_operations = confirmed_transaction.finalize
            operations = []
            for transition in execution.transitions:
                transition: Transition
                finalize: Vec[Value, u8] = transition.finalize.value
                if finalize is not None:
                    program = Program.load(bytearray(await db.get_program(str(transition.program_id))))
                    inputs = list(map(lambda x: x.plaintext, finalize))
                    operations.extend(
                        await execute_finalizer(db, program, transition.function_name, inputs, mapping_cache)
                    )

        else:
            expected_operations = []
            operations = []

        for e, o in zip(expected_operations, operations):
            if e.type != o["type"]:
                raise TypeError("invalid finalize operation type")
            if e.mapping_id != o["mapping_id"]:
                raise TypeError("invalid finalize mapping id")
            match e.type:
                case FinalizeOperation.Type.InitializeMapping:
                    pass
                case FinalizeOperation.Type.UpdateKeyValue:
                    if e.index != o["index"] or e.key_id != o["key_id"] or e.value_id != o["value_id"]:
                        raise TypeError("invalid finalize operation")
                case _:
                    raise NotImplementedError

        await execute_operations(db, cur, operations)


async def execute_operations(db: Database, cur, operations: [dict]):
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
            case _:
                raise NotImplementedError

async def preview_finalize_execution(db: Database, program: Program, function_name: Identifier, inputs: [Value]):
    return await execute_finalizer(db, program, function_name, inputs, {})