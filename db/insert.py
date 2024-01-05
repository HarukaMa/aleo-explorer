
from __future__ import annotations

import os
import signal
from collections import defaultdict
from typing import cast

import psycopg.sql
from redis.asyncio import Redis

from aleo_types import *
from disasm.utils import value_type_to_mode_type_str, plaintext_type_to_str
from explorer.types import Message as ExplorerMessage
from util.global_cache import global_mapping_cache
from .base import DatabaseBase, profile
from .util import DatabaseUtil


class DatabaseInsert(DatabaseBase):

    @staticmethod
    async def _insert_future(conn: psycopg.AsyncConnection[dict[str, Any]], future: Future,
                             transition_output_future_db_id: Optional[int] = None, argument_db_id: Optional[int] = None,):
        async with conn.cursor() as cur:
            if transition_output_future_db_id:
                await cur.execute(
                    "INSERT INTO future (type, transition_output_future_id, program_id, function_name) "
                    "VALUES ('Output', %s, %s, %s) RETURNING id",
                    (transition_output_future_db_id, str(future.program_id), str(future.function_name))
                )
                if (res := await cur.fetchone()) is None:
                    raise RuntimeError("failed to insert row into database")
                future_db_id = res["id"]
                await cur.execute(
                    "SELECT t.id FROM transition t "
                    "JOIN transition_output o on t.id = o.transition_id "
                    "JOIN transition_output_future tof on o.id = tof.transition_output_id "
                    "WHERE tof.id = %s",
                    (transition_output_future_db_id,)
                )
                if (res := await cur.fetchone()) is None:
                    raise RuntimeError("database inconsistent")
                transition_db_id = res["id"]
            elif argument_db_id:
                await cur.execute(
                    "INSERT INTO future (type, future_argument_id, program_id, function_name) "
                    "VALUES ('Argument', %s, %s, %s) RETURNING id",
                    (argument_db_id, str(future.program_id), str(future.function_name))
                )
                if (res := await cur.fetchone()) is None:
                    raise RuntimeError("failed to insert row into database")
                future_db_id = res["id"]
                while True:
                    await cur.execute(
                        "SELECT f.id, f.transition_output_future_id, f.future_argument_id FROM future f "
                        "JOIN future_argument a on f.id = a.future_id "
                        "WHERE a.id = %s",
                        (argument_db_id,)
                    )
                    if (res := await cur.fetchone()) is None:
                        raise RuntimeError("database inconsistent")
                    if res["transition_output_future_id"]:
                        transition_output_future_db_id = res["transition_output_future_id"]
                        break
                    argument_db_id = res["future_argument_id"]
                await cur.execute(
                    "SELECT t.id FROM transition t "
                    "JOIN transition_output o on t.id = o.transition_id "
                    "JOIN transition_output_future tof on o.id = tof.transition_output_id "
                    "WHERE tof.id = %s",
                    (transition_output_future_db_id,)
                )
                if (res := await cur.fetchone()) is None:
                    raise RuntimeError("database inconsistent")
                transition_db_id = res["id"]
            else:
                raise ValueError("transition_output_db_id or argument_db_id must be set")
            for argument in future.arguments:
                if isinstance(argument, PlaintextArgument):
                    plaintext = argument.plaintext
                    await cur.execute(
                        "INSERT INTO future_argument (future_id, type, plaintext) VALUES (%s, %s, %s)",
                        (future_db_id, argument.type.name, plaintext.dump())
                    )
                    if isinstance(plaintext, LiteralPlaintext) and plaintext.literal.type == Literal.Type.Address:
                        address = str(plaintext.literal.primitive)
                        await cur.execute(
                            "INSERT INTO address_transition (address, transition_id) VALUES (%s, %s)",
                            (address, transition_db_id)
                        )
                    elif isinstance(plaintext, StructPlaintext):
                        addresses = DatabaseUtil.get_addresses_from_struct(plaintext)
                        for address in addresses:
                            await cur.execute(
                                "INSERT INTO address_transition (address, transition_id) VALUES (%s, %s)",
                                (address, transition_db_id)
                            )

                elif isinstance(argument, FutureArgument):
                    await cur.execute(
                        "INSERT INTO future_argument (future_id, type) VALUES (%s, %s) RETURNING id",
                        (future_db_id, argument.type.name)
                    )
                    if (res := await cur.fetchone()) is None:
                        raise RuntimeError("failed to insert row into database")
                    argument_db_id = res["id"]
                    await DatabaseInsert._insert_future(conn, argument.future, argument_db_id=argument_db_id)
                else:
                    raise NotImplementedError

    @staticmethod
    async def _insert_transition(conn: psycopg.AsyncConnection[dict[str, Any]], redis_conn: Redis[str],
                                 exe_tx_db_id: Optional[int], fee_db_id: Optional[int],
                                 transition: Transition, ts_index: int, is_rejected: bool = False, should_exist: bool = False):
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT id FROM transition WHERE transition_id = %s", (str(transition.id),)
            )
            if await cur.fetchone() is not None:
                if not is_rejected or not should_exist:
                    raise RuntimeError("transition already exists in database")
                else:
                    return
            await cur.execute(
                "INSERT INTO transition (transition_id, transaction_execute_id, fee_id, program_id, "
                "function_name, tpk, tcm, index) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id",
                (str(transition.id), exe_tx_db_id, fee_db_id, str(transition.program_id),
                 str(transition.function_name), str(transition.tpk), str(transition.tcm), ts_index)
            )
            if (res := await cur.fetchone()) is None:
                raise RuntimeError("failed to insert row into database")
            transition_db_id = res["id"]

            transition_input: TransitionInput
            for input_index, transition_input in enumerate(transition.inputs):
                await cur.execute(
                    "INSERT INTO transition_input (transition_id, type, index) VALUES (%s, %s, %s) RETURNING id",
                    (transition_db_id, transition_input.type.name, input_index)
                )
                if (res := await cur.fetchone()) is None:
                    raise RuntimeError("failed to insert row into database")
                transition_input_db_id = res["id"]
                if isinstance(transition_input, PublicTransitionInput):
                    await cur.execute(
                        "INSERT INTO transition_input_public (transition_input_id, plaintext_hash, plaintext) "
                        "VALUES (%s, %s, %s)",
                        (transition_input_db_id, str(transition_input.plaintext_hash),
                         transition_input.plaintext.dump_nullable())
                    )
                    if transition_input.plaintext.value is not None:
                        plaintext = cast(Plaintext, transition_input.plaintext.value)
                        if isinstance(plaintext, LiteralPlaintext) and plaintext.literal.type == Literal.Type.Address:
                            address = str(plaintext.literal.primitive)
                            await cur.execute(
                                "INSERT INTO address_transition (address, transition_id) VALUES (%s, %s)",
                                (address, transition_db_id)
                            )
                        elif isinstance(plaintext, StructPlaintext):
                            addresses = DatabaseUtil.get_addresses_from_struct(plaintext)
                            for address in addresses:
                                await cur.execute(
                                    "INSERT INTO address_transition (address, transition_id) VALUES (%s, %s)",
                                    (address, transition_db_id)
                                )
                elif isinstance(transition_input, PrivateTransitionInput):
                    await cur.execute(
                        "INSERT INTO transition_input_private (transition_input_id, ciphertext_hash, ciphertext) "
                        "VALUES (%s, %s, %s)",
                        (transition_input_db_id, str(transition_input.ciphertext_hash),
                         transition_input.ciphertext.dumps())
                    )
                elif isinstance(transition_input, RecordTransitionInput):
                    await cur.execute(
                        "INSERT INTO transition_input_record (transition_input_id, serial_number, tag) "
                        "VALUES (%s, %s, %s)",
                        (transition_input_db_id, str(transition_input.serial_number),
                         str(transition_input.tag))
                    )
                elif isinstance(transition_input, ExternalRecordTransitionInput):
                    await cur.execute(
                        "INSERT INTO transition_input_external_record (transition_input_id, commitment) "
                        "VALUES (%s, %s)",
                        (transition_input_db_id, str(transition_input.input_commitment))
                    )

                else:
                    raise NotImplementedError

            transition_output: TransitionOutput
            for output_index, transition_output in enumerate(transition.outputs):
                await cur.execute(
                    "INSERT INTO transition_output (transition_id, type, index) VALUES (%s, %s, %s) RETURNING id",
                    (transition_db_id, transition_output.type.name, output_index)
                )
                if (res := await cur.fetchone()) is None:
                    raise RuntimeError("failed to insert row into database")
                transition_output_db_id = res["id"]
                if isinstance(transition_output, PublicTransitionOutput):
                    await cur.execute(
                        "INSERT INTO transition_output_public (transition_output_id, plaintext_hash, plaintext) "
                        "VALUES (%s, %s, %s)",
                        (transition_output_db_id, str(transition_output.plaintext_hash),
                         transition_output.plaintext.dump_nullable())
                    )
                elif isinstance(transition_output, PrivateTransitionOutput):
                    await cur.execute(
                        "INSERT INTO transition_output_private (transition_output_id, ciphertext_hash, ciphertext) "
                        "VALUES (%s, %s, %s)",
                        (transition_output_db_id, str(transition_output.ciphertext_hash),
                         transition_output.ciphertext.dumps())
                    )
                elif isinstance(transition_output, RecordTransitionOutput):
                    await cur.execute(
                        "INSERT INTO transition_output_record (transition_output_id, commitment, checksum, record_ciphertext) "
                        "VALUES (%s, %s, %s, %s)",
                        (transition_output_db_id, str(transition_output.commitment),
                         str(transition_output.checksum), transition_output.record_ciphertext.dumps())
                    )
                elif isinstance(transition_output, ExternalRecordTransitionOutput):
                    await cur.execute(
                        "INSERT INTO transition_output_external_record (transition_output_id, commitment) "
                        "VALUES (%s, %s)",
                        (transition_output_db_id, str(transition_output.commitment))
                    )
                elif isinstance(transition_output, FutureTransitionOutput):
                    await cur.execute(
                        "INSERT INTO transition_output_future (transition_output_id, future_hash) "
                        "VALUES (%s, %s) RETURNING id",
                        (transition_output_db_id, str(transition_output.future_hash))
                    )
                    if (res := await cur.fetchone()) is None:
                        raise Exception("failed to insert row into database")
                    transition_output_future_db_id = res["id"]
                    if transition_output.future.value is not None:
                        await DatabaseInsert._insert_future(conn, transition_output.future.value, transition_output_future_db_id)
                else:
                    raise NotImplementedError

            await cur.execute(
                "SELECT id FROM program WHERE program_id = %s", (str(transition.program_id),)
            )
            if (res := await cur.fetchone()) is None:
                raise RuntimeError("program in transition does not exist - unconfirmed transaction?")
            program_db_id = res["id"]
            await cur.execute(
                "UPDATE program_function SET called = called + 1 WHERE program_id = %s AND name = %s",
                (program_db_id, str(transition.function_name))
            )

            if not is_rejected and transition.program_id == "credits.aleo":
                transfer_from = None
                transfer_to = None
                fee_from = None
                if transition.function_name == "transfer_public":
                    output = cast(FutureTransitionOutput, transition.outputs[0])
                    future = cast(Future, output.future.value)
                    transfer_from = str(DatabaseUtil.get_primitive_from_argument_unchecked(future.arguments[0]))
                    transfer_to = str(DatabaseUtil.get_primitive_from_argument_unchecked(future.arguments[1]))
                    amount = int(cast(int, DatabaseUtil.get_primitive_from_argument_unchecked(future.arguments[2])))
                elif transition.function_name == "transfer_private_to_public":
                    output = cast(FutureTransitionOutput, transition.outputs[1])
                    future = cast(Future, output.future.value)
                    transfer_to = str(DatabaseUtil.get_primitive_from_argument_unchecked(future.arguments[0]))
                    amount = int(cast(int, DatabaseUtil.get_primitive_from_argument_unchecked(future.arguments[1])))
                elif transition.function_name == "transfer_public_to_private":
                    output = cast(FutureTransitionOutput, transition.outputs[1])
                    future = cast(Future, output.future.value)
                    transfer_from = str(DatabaseUtil.get_primitive_from_argument_unchecked(future.arguments[0]))
                    amount = int(cast(int, DatabaseUtil.get_primitive_from_argument_unchecked(future.arguments[1])))
                elif transition.function_name == "fee_public":
                    output = cast(FutureTransitionOutput, transition.outputs[0])
                    future = cast(Future, output.future.value)
                    fee_from = str(DatabaseUtil.get_primitive_from_argument_unchecked(future.arguments[0]))
                    amount = int(cast(int, DatabaseUtil.get_primitive_from_argument_unchecked(future.arguments[1])))

                if transfer_from != transfer_to:
                    if transfer_from is not None:
                        await redis_conn.hincrby("address_transfer_out", transfer_from, amount) # type: ignore
                    if transfer_to is not None:
                        await redis_conn.hincrby("address_transfer_in", transfer_to, amount) # type: ignore

                if fee_from is not None:
                    await redis_conn.hincrby("address_fee", fee_from, amount) # type: ignore

    @staticmethod
    async def _insert_deploy_transaction(conn: psycopg.AsyncConnection[dict[str, Any]], redis: Redis[str],
                                         deployment: Deployment, owner: ProgramOwner, fee: Fee, transaction_db_id: int,
                                         is_unconfirmed: bool = False, is_rejected: bool = False, fee_should_exist: bool = False):
        async with conn.cursor() as cur:
            if is_unconfirmed or is_rejected:
                program_id = str(deployment.program.id)
                owner = str(owner.address)
            else:
                program_id = None
                owner = None
            await cur.execute(
                "INSERT INTO transaction_deploy (transaction_id, edition, verifying_keys, program_id, owner) "
                "VALUES (%s, %s, %s, %s, %s) RETURNING id",
                (transaction_db_id, deployment.edition, deployment.verifying_keys.dump(), program_id, owner)
            )
            if await cur.fetchone() is None:
                raise RuntimeError("failed to insert row into database")

            await cur.execute(
                "INSERT INTO fee (transaction_id, global_state_root, proof) "
                "VALUES (%s, %s, %s) RETURNING id",
                (transaction_db_id, str(fee.global_state_root), fee.proof.dumps())
            )
            if (res := await cur.fetchone()) is None:
                raise RuntimeError("failed to insert row into database")
            fee_db_id = res["id"]

            await DatabaseInsert._insert_transition(conn, redis, None, fee_db_id, fee.transition, 0, is_rejected, fee_should_exist)

    @staticmethod
    async def _insert_execute_transaction(conn: psycopg.AsyncConnection[dict[str, Any]], redis: Redis[str],
                                          execution: Execution, fee: Optional[Fee], transaction_db_id: int,
                                          is_rejected: bool = False, ts_should_exist: bool = False):
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO transaction_execute (transaction_id, global_state_root, proof) "
                "VALUES (%s, %s, %s) RETURNING id",
                (transaction_db_id, str(execution.global_state_root),
                 execution.proof.dumps())
            )
            if (res := await cur.fetchone()) is None:
                raise RuntimeError("failed to insert row into database")
            execute_transaction_db_id = res["id"]

            for ts_index, transition in enumerate(execution.transitions):
                await DatabaseInsert._insert_transition(conn, redis, execute_transaction_db_id, None, transition, ts_index, is_rejected, ts_should_exist)


            if fee:
                await cur.execute(
                    "INSERT INTO fee (transaction_id, global_state_root, proof) "
                    "VALUES (%s, %s, %s) RETURNING id",
                    (transaction_db_id, str(fee.global_state_root), fee.proof.dumps())
                )
                if (res := await cur.fetchone()) is None:
                    raise RuntimeError("failed to insert row into database")
                fee_db_id = res["id"]
                await DatabaseInsert._insert_transition(conn, redis, None, fee_db_id, fee.transition, 0, is_rejected, ts_should_exist)

    @staticmethod
    async def _insert_transaction(conn: psycopg.AsyncConnection[dict[str, Any]], redis: Redis[str], transaction: Transaction,
                                  confirmed_transaction: Optional[ConfirmedTransaction] = None, ct_index: Optional[int] = None,
                                  ignore_deploy_txids: Optional[list[str]] = None, confirmed_transaction_db_id: Optional[int] = None,
                                  reject_reasons: Optional[list[Optional[str]]] = None):
        async with conn.cursor() as cur:
            optionals = (confirmed_transaction, ct_index, confirmed_transaction_db_id, reject_reasons)
            if not (all(x is None for x in optionals) or all(x is not None for x in optionals)):
                raise ValueError("expected all or none of confirmed_transaction, ct_index, confirmed_transaction_db_id, reject_reasons to be set")

            await cur.execute(
                "SELECT transaction_id FROM transaction WHERE transaction_id = %s",
                (str(transaction.id),)
            )
            if (await cur.fetchone()) is None: # first seen
                prior_tx = False
                transaction_db_id: int = -1
                if isinstance(transaction, FeeTransaction): # check probable rejected unconfirmed transaction
                    if confirmed_transaction is None:
                        raise RuntimeError("expected a confirmed transaction for fee transaction")
                    if isinstance(confirmed_transaction, RejectedDeploy):
                        rejected_deployment = cast(RejectedDeployment, confirmed_transaction.rejected)
                        ref_transition_id = transaction.fee.transition.id
                        await cur.execute(
                            "SELECT tx.id, tx.transaction_id FROM transaction tx "
                            "JOIN fee f on tx.id = f.transaction_id "
                            "JOIN transition t on f.id = t.fee_id "
                            "WHERE t.transition_id = %s",
                            (str(ref_transition_id),)
                        )
                        if (res := await cur.fetchone()) is not None:
                            prior_tx = True
                            transaction_db_id = res["id"]
                            original_transaction_id = res["transaction_id"]
                            await cur.execute(
                                "UPDATE transaction SET transaction_id = %s, original_transaction_id = %s, type = 'Fee' WHERE id = %s",
                                (str(transaction.id), original_transaction_id, transaction_db_id)
                            )
                            await DatabaseInsert._insert_deploy_transaction(conn, redis, rejected_deployment.deploy, rejected_deployment.program_owner, transaction.fee, transaction_db_id, is_rejected=True, fee_should_exist=True)

                    elif isinstance(confirmed_transaction, RejectedExecute):
                        rejected_execution = cast(RejectedExecution, confirmed_transaction.rejected)
                        ref_transition_id = rejected_execution.execution.transitions[0].id
                        await cur.execute(
                            "SELECT tx.id, tx.transaction_id FROM transaction tx "
                            "JOIN transaction_execute te on tx.id = te.transaction_id "
                            "JOIN transition t on te.id = t.transaction_execute_id "
                            "WHERE t.transition_id = %s",
                            (str(ref_transition_id),)
                        )
                        if (res := await cur.fetchone()) is not None:
                            prior_tx = True
                            transaction_db_id = res["id"]
                            original_transaction_id = res["transaction_id"]
                            await cur.execute(
                                "UPDATE transaction SET transaction_id = %s, original_transaction_id = %s, type = 'Fee' WHERE id = %s",
                                (str(transaction.id), original_transaction_id, transaction_db_id)
                            )
                            await DatabaseInsert._insert_execute_transaction(conn, redis, rejected_execution.execution, transaction.fee, transaction_db_id, is_rejected=True, ts_should_exist=True)

                if not prior_tx:
                    await cur.execute(
                        "INSERT INTO transaction (transaction_id, type) "
                        "VALUES (%s, %s) RETURNING id",
                        (str(transaction.id), transaction.type.name)
                    )
                    if (res := await cur.fetchone()) is None:
                        raise RuntimeError("failed to insert row into database")
                    transaction_db_id = res["id"]
                if transaction_db_id == -1:
                    raise RuntimeError("failed to get transaction id")

                if isinstance(transaction, DeployTransaction): # accepted deploy / unconfirmed
                    await DatabaseInsert._insert_deploy_transaction(
                        conn, redis, transaction.deployment, transaction.owner, transaction.fee, transaction_db_id,
                        is_unconfirmed=(confirmed_transaction is None)
                    )

                elif isinstance(transaction, ExecuteTransaction): # accepted execute / unconfirmed
                    await DatabaseInsert._insert_execute_transaction(conn, redis, transaction.execution, transaction.additional_fee.value, transaction_db_id)

                elif isinstance(transaction, FeeTransaction) and not prior_tx: # first seen rejected tx
                    if isinstance(confirmed_transaction, RejectedDeploy):
                        rejected_deployment = cast(RejectedDeployment, confirmed_transaction.rejected)
                        await DatabaseInsert._insert_deploy_transaction(conn, redis, rejected_deployment.deploy, rejected_deployment.program_owner, transaction.fee, transaction_db_id, is_rejected=True)
                    elif isinstance(confirmed_transaction, RejectedExecute):
                        rejected_execution = cast(RejectedExecution, confirmed_transaction.rejected)
                        await DatabaseInsert._insert_execute_transaction(conn, redis, rejected_execution.execution, transaction.fee, transaction_db_id, is_rejected=True)

            # confirming tx
            if confirmed_transaction is not None:
                await cur.execute(
                    "UPDATE transaction SET confimed_transaction_id = %s WHERE transaction_id = %s",
                    (confirmed_transaction_db_id, str(transaction.id))
                )
                reject_reasons = cast(list[Optional[str]], reject_reasons)
                ct_index = cast(int, ct_index)
                ignore_deploy_txids = cast(list[str], ignore_deploy_txids)
                if isinstance(confirmed_transaction, AcceptedDeploy):
                    transaction = cast(DeployTransaction, transaction)
                    if reject_reasons[ct_index] is not None:
                        raise RuntimeError("expected no rejected reason for accepted deploy transaction")
                    # TODO: remove bug workaround
                    if str(transaction.id) not in ignore_deploy_txids:
                        await cur.execute(
                            "SELECT td.id FROM transaction_deploy td "
                            "JOIN transaction t on td.transaction_id = t.id "
                            "WHERE t.transaction_id = %s",
                            (str(transaction.id),)
                        )
                        if (res := await cur.fetchone()) is None:
                            raise RuntimeError("database inconsistent")
                        deploy_transaction_db_id = res["id"]
                        await DatabaseInsert._save_program(cur, transaction.deployment.program, deploy_transaction_db_id, transaction)

                elif isinstance(confirmed_transaction, AcceptedExecute):
                    if reject_reasons[ct_index] is not None:
                        raise RuntimeError("expected no rejected reason for accepted execute transaction")

                elif isinstance(confirmed_transaction, (RejectedDeploy, RejectedExecute)):
                    if reject_reasons[ct_index] is None:
                        raise RuntimeError("expected a rejected reason for rejected transaction")
                    await cur.execute("UPDATE confirmed_transaction SET reject_reason = %s WHERE id = %s",
                                      (reject_reasons[ct_index], confirmed_transaction_db_id))

    async def save_builtin_program(self, program: Program):
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                await self._save_program(cur, program, None, None)

    @staticmethod
    async def _save_program(cur: psycopg.AsyncCursor[dict[str, Any]], program: Program,
                            deploy_transaction_db_id: Optional[int], transaction: Optional[DeployTransaction]) -> None:
        imports = [str(x.program_id) for x in program.imports]
        mappings = list(map(str, program.mappings.keys()))
        interfaces = list(map(str, program.structs.keys()))
        records = list(map(str, program.records.keys()))
        closures = list(map(str, program.closures.keys()))
        functions = list(map(str, program.functions.keys()))
        if transaction:
            await cur.execute(
                "INSERT INTO program "
                "(transaction_deploy_id, program_id, import, mapping, interface, record, "
                "closure, function, raw_data, is_helloworld, feature_hash, owner, signature, address) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id",
                (deploy_transaction_db_id, str(program.id), imports, mappings, interfaces, records,
                 closures, functions, program.dump(), program.is_helloworld(), program.feature_hash(),
                 str(transaction.owner.address), str(transaction.owner.signature),
                 aleo_explorer_rust.program_id_to_address(str(program.id)))
            )
        else:
            await cur.execute(
                "INSERT INTO program "
                "(program_id, import, mapping, interface, record, "
                "closure, function, raw_data, is_helloworld, feature_hash, address) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id",
                (str(program.id), imports, mappings, interfaces, records,
                 closures, functions, program.dump(), program.is_helloworld(), program.feature_hash(),
                 aleo_explorer_rust.program_id_to_address(str(program.id)))
            )
        if (res := await cur.fetchone()) is None:
            raise Exception("failed to insert row into database")
        program_db_id = res["id"]
        for function in program.functions.values():
            inputs: list[str] = []
            input_modes: list[str] = []
            for i in function.inputs:
                mode, _type = value_type_to_mode_type_str(i.value_type)
                inputs.append(_type)
                input_modes.append(mode)
            outputs: list[str] = []
            output_modes: list[str] = []
            for o in function.outputs:
                if isinstance(o.value_type, FutureValueType):
                    continue
                mode, _type = value_type_to_mode_type_str(o.value_type)
                outputs.append(_type)
                output_modes.append(mode)
            finalizes: list[str] = []
            if function.finalize.value is not None:
                for f in function.finalize.value.inputs:
                    if isinstance(f.finalize_type, PlaintextFinalizeType):
                        finalizes.append(plaintext_type_to_str(f.finalize_type.plaintext_type))
            await cur.execute(
                "INSERT INTO program_function (program_id, name, input, input_mode, output, output_mode, finalize) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (program_db_id, str(function.name), inputs, input_modes, outputs, output_modes, finalizes)
            )

    @staticmethod
    @profile
    async def _update_committee_bonded_map(cur: psycopg.AsyncCursor[dict[str, Any]],
                                           redis_conn: Redis[str],
                                           committee_members: dict[Address, tuple[u64, bool_]],
                                           stakers: dict[Address, tuple[Address, u64]],
                                           height: int):
        committee_mapping_id = Field.loads(cached_get_mapping_id("credits.aleo", "committee"))
        bonded_mapping_id = Field.loads(cached_get_mapping_id("credits.aleo", "bonded"))

        global_mapping_cache[committee_mapping_id] = {}
        committee_mapping: dict[str, dict[str, Any]] = {}
        for address, (amount, is_open) in committee_members.items():
            key = LiteralPlaintext(literal=Literal(type_=Literal.Type.Address, primitive=address))
            key_id = Field.loads(cached_get_key_id("credits.aleo", "committee", key.dump()))
            value = PlaintextValue(
                plaintext=StructPlaintext(
                    members=Vec[Tuple[Identifier, Plaintext], u8]([
                        Tuple[Identifier, Plaintext]((
                            Identifier.loads("microcredits"),
                            LiteralPlaintext(literal=Literal(type_=Literal.Type.U64, primitive=amount))
                        )),
                        Tuple[Identifier, Plaintext]((
                            Identifier.loads("is_open"),
                            LiteralPlaintext(literal=Literal(type_=Literal.Type.Boolean, primitive=is_open))
                        ))
                    ])
                )
            )
            committee_mapping[str(key_id)] = {
                "key": key.dump().hex(),
                "value": value.dump().hex(),
            }
            global_mapping_cache[committee_mapping_id][key_id] = {
                "key": key,
                "value": value,
            }
        await redis_conn.execute_command("MULTI")
        await redis_conn.delete("credits.aleo:committee")
        await redis_conn.hset("credits.aleo:committee", mapping={k: json.dumps(v) for k, v in committee_mapping.items()})
        await redis_conn.execute_command("EXEC")

        global_mapping_cache[bonded_mapping_id] = {}
        bonded_mapping: dict[str, dict[str, Any]] = {}
        for address, (validator, amount) in stakers.items():
            key = LiteralPlaintext(literal=Literal(type_=Literal.Type.Address, primitive=address))
            key_id = Field.loads(cached_get_key_id("credits.aleo", "bonded", key.dump()))
            value = PlaintextValue(
                plaintext=StructPlaintext(
                    members=Vec[Tuple[Identifier, Plaintext], u8]([
                        Tuple[Identifier, Plaintext]((
                            Identifier.loads("validator"),
                            LiteralPlaintext(literal=Literal(type_=Literal.Type.Address, primitive=validator))
                        )),
                        Tuple[Identifier, Plaintext]((
                            Identifier.loads("microcredits"),
                            LiteralPlaintext(literal=Literal(type_=Literal.Type.U64, primitive=amount))
                        ))
                    ])
                )
            )
            bonded_mapping[str(key_id)] = {
                "key": key.dump().hex(),
                "value": value.dump().hex(),
            }
            global_mapping_cache[bonded_mapping_id][key_id] = {
                "key": key,
                "value": value,
            }
        await redis_conn.execute_command("MULTI")
        await redis_conn.delete("credits.aleo:bonded")
        await redis_conn.hset("credits.aleo:bonded", mapping={k: json.dumps(v) for k, v in bonded_mapping.items()})
        await redis_conn.execute_command("EXEC")
        # await cur.execute(
        #     "INSERT INTO mapping_bonded_history (height, content) VALUES (%s, %s)",
        #     (height, Jsonb(bonded_mapping))
        # )

    @staticmethod
    async def _save_committee_history(cur: psycopg.AsyncCursor[dict[str, Any]], height: int, committee: Committee):
        await cur.execute(
            "INSERT INTO committee_history (height, starting_round, total_stake) "
            "VALUES (%s, %s, %s) RETURNING id",
            (height, committee.starting_round, committee.total_stake)
        )
        if (res := await cur.fetchone()) is None:
            raise RuntimeError("failed to insert row into database")
        committee_db_id = res["id"]
        for address, stake, is_open in committee.members:
            await cur.execute(
                "INSERT INTO committee_history_member (committee_id, address, stake, is_open) "
                "VALUES (%s, %s, %s, %s)",
                (committee_db_id, str(address), stake, bool(is_open))
            )

    async def _pre_ratify(self, cur: psycopg.AsyncCursor[dict[str, Any]], ratification: GenesisRatify):
        from interpreter.interpreter import global_mapping_cache
        committee = ratification.committee
        await DatabaseInsert._save_committee_history(cur, 0, committee)

        stakers: dict[Address, tuple[Address, u64]] = {}
        for validator, amount, _ in committee.members:
            stakers[validator] = validator, amount
        committee_members = {address: (amount, is_open) for address, amount, is_open in committee.members}
        await DatabaseInsert._update_committee_bonded_map(cur, self.redis, committee_members, stakers, 0)

        account_mapping_id = Field.loads(cached_get_mapping_id("credits.aleo", "account"))
        public_balances = ratification.public_balances
        global_mapping_cache[account_mapping_id] = {}
        operations: list[dict[str, Any]] = []
        for address, balance in public_balances:
            key = LiteralPlaintext(literal=Literal(type_=Literal.Type.Address, primitive=address))
            key_id = Field.loads(cached_get_key_id("credits.aleo", "account", key.dump()))
            value = PlaintextValue(plaintext=LiteralPlaintext(literal=Literal(type_=Literal.Type.U64, primitive=balance)))
            value_id = Field.loads(aleo_explorer_rust.get_value_id(str(key_id), value.dump()))
            global_mapping_cache[account_mapping_id][key_id] = {
                "key": key,
                "value": value,
            }
            operations.append({
                "type": FinalizeOperation.Type.UpdateKeyValue,
                "mapping_id": account_mapping_id,
                "key_id": key_id,
                "value_id": value_id,
                "key": key,
                "value": value,
                "height": 0,
                "program_name": "credits.aleo",
                "mapping_name": "account",
                "from_transaction": False,
            })
        from interpreter.interpreter import execute_operations
        await execute_operations(self, cur, operations)

    @staticmethod
    async def _get_committee_mapping(redis_conn: Redis[str]) -> dict[Address, tuple[u64, bool_]]:
        data = await redis_conn.hgetall("credits.aleo:committee")
        committee_members: dict[Address, tuple[u64, bool_]] = {}
        for d in data.values():
            d = json.loads(d)
            key = Plaintext.load(BytesIO(bytes.fromhex(d["key"])))
            if not isinstance(key, LiteralPlaintext):
                raise RuntimeError("invalid committee key")
            if not isinstance(key.literal.primitive, Address):
                raise RuntimeError("invalid committee key")
            value = Value.load(BytesIO(bytes.fromhex(d["value"])))
            if not isinstance(value, PlaintextValue):
                raise RuntimeError("invalid committee value")
            plaintext = value.plaintext
            if not isinstance(plaintext, StructPlaintext):
                raise RuntimeError("invalid committee value")
            amount = plaintext["microcredits"]
            if not isinstance(amount, LiteralPlaintext):
                raise RuntimeError("invalid committee value")
            if not isinstance(amount.literal.primitive, u64):
                raise RuntimeError("invalid committee value")
            is_open = plaintext["is_open"]
            if not isinstance(is_open, LiteralPlaintext):
                raise RuntimeError("invalid committee value")
            if not isinstance(is_open.literal.primitive, bool_):
                raise RuntimeError("invalid committee value")
            committee_members[key.literal.primitive] = amount.literal.primitive, is_open.literal.primitive
        return committee_members

    async def get_bonded_mapping(self) -> dict[Address, tuple[Address, u64]]:
        data = await self.redis.hgetall("credits.aleo:bonded")

        stakers: dict[Address, tuple[Address, u64]] = {}
        for d in data.values():
            d = json.loads(d)
            key = Plaintext.load(BytesIO(bytes.fromhex(d["key"])))
            if not isinstance(key, LiteralPlaintext):
                raise RuntimeError("invalid bonded key")
            if not isinstance(key.literal.primitive, Address):
                raise RuntimeError("invalid bonded key")
            value = Value.load(BytesIO(bytes.fromhex(d["value"])))
            if not isinstance(value, PlaintextValue):
                raise RuntimeError("invalid bonded value")
            plaintext = value.plaintext
            if not isinstance(plaintext, StructPlaintext):
                raise RuntimeError("invalid bonded value")
            validator = plaintext["validator"]
            if not isinstance(validator, LiteralPlaintext):
                raise RuntimeError("invalid bonded value")
            if not isinstance(validator.literal.primitive, Address):
                raise RuntimeError("invalid bonded value")
            amount = plaintext["microcredits"]
            if not isinstance(amount, LiteralPlaintext):
                raise RuntimeError("invalid bonded value")
            if not isinstance(amount.literal.primitive, u64):
                raise RuntimeError("invalid bonded value")
            stakers[key.literal.primitive] = validator.literal.primitive, amount.literal.primitive
        return stakers

    @staticmethod
    def _check_committee_staker_match(committee_members: dict[Address, tuple[u64, bool_]],
                                      stakers: dict[Address, tuple[Address, u64]]):
        address_stakes: dict[Address, u64] = defaultdict(lambda: u64())
        for _, (validator, amount) in stakers.items():
            address_stakes[validator] += amount # type: ignore[reportGeneralTypeIssues]
        if len(address_stakes) != len(committee_members):
            raise RuntimeError("size mismatch between stakers and committee members")

        committee_total_stake = sum(amount for amount, _ in committee_members.values())
        stakers_total_stake = sum(address_stakes.values())
        if committee_total_stake != stakers_total_stake:
            print(committee_total_stake, stakers_total_stake)
            raise RuntimeError("total stake mismatch between stakers and committee members")

        for address, amount in address_stakes.items():
            if address not in committee_members:
                raise RuntimeError("staked address not in committee members")
            if amount != committee_members[address][0]:
                raise RuntimeError("stake mismatch between stakers and committee members")


    @staticmethod
    def _stake_rewards(committee_members: dict[Address, tuple[u64, bool_]],
                       stakers: dict[Address, tuple[Address, u64]], block_reward: u64):
        total_stake = sum(x[0] for x in committee_members.values())
        stake_rewards: dict[Address, int] = {}
        if not stakers or total_stake == 0 or block_reward == 0:
            return stakers, stake_rewards

        new_stakers: dict[Address, tuple[Address, u64]] = {}

        for staker, (validator, stake) in stakers.items():
            if committee_members[validator][0] > total_stake // 4:
                new_stakers[staker] = validator, stake
                continue
            if stake < 10_000_000:
                new_stakers[staker] = validator, stake
                continue

            reward = int(block_reward) * stake // total_stake
            stake_rewards[staker] = reward

            new_stake = stake + reward
            new_stakers[staker] = validator, u64(new_stake)

        return new_stakers, stake_rewards

    @staticmethod
    def _next_committee_members(committee_members: dict[Address, tuple[u64, bool_]],
                                stakers: dict[Address, tuple[Address, u64]]) -> dict[Address, tuple[u64, bool_]]:
        validators: dict[Address, u64] = defaultdict(lambda: u64())
        for _, (validator, amount) in stakers.items():
            validators[validator] += amount # type: ignore[reportGeneralTypeIssues]
        new_committee_members: dict[Address, tuple[u64, bool_]] = {}
        for validator, amount in validators.items():
            new_committee_members[validator] = amount, committee_members[validator][1]
        return new_committee_members

    @profile
    async def _post_ratify(self, cur: psycopg.AsyncCursor[dict[str, Any]], redis_conn: Redis[str], height: int, round_: int,
                           ratifications: list[Ratify], address_puzzle_rewards: dict[str, int]):
        from interpreter.interpreter import global_mapping_cache

        for ratification in ratifications:
            if isinstance(ratification, BlockRewardRatify):
                committee_members = await DatabaseInsert._get_committee_mapping(redis_conn)
                mapping_id = Field.loads(cached_get_mapping_id("credits.aleo", "bonded"))
                if mapping_id in global_mapping_cache:
                    data = global_mapping_cache[mapping_id]
                    stakers: dict[Address, tuple[Address, u64]] = {}
                    for v in data.values():
                        key = cast(LiteralPlaintext, v["key"])
                        value = v["value"]
                        address = cast(Address, key.literal.primitive)
                        bond_state = cast(StructPlaintext, cast(PlaintextValue, value).plaintext)
                        validator = cast(Address, cast(LiteralPlaintext, bond_state["validator"]).literal.primitive)
                        amount = cast(u64, cast(LiteralPlaintext, bond_state["microcredits"]).literal.primitive)
                        stakers[address] = validator, amount
                else:
                    stakers = await self.get_bonded_mapping()

                DatabaseInsert._check_committee_staker_match(committee_members, stakers)

                stakers, stake_rewards = DatabaseInsert._stake_rewards(committee_members, stakers, ratification.amount)
                committee_members = DatabaseInsert._next_committee_members(committee_members, stakers)

                pipe = self.redis.pipeline()
                for address, amount in stake_rewards.items():
                    pipe.hincrby("address_stake_reward", str(address), amount)
                await pipe.execute()

                await DatabaseInsert._update_committee_bonded_map(cur, self.redis, committee_members, stakers, height)
                await DatabaseInsert._save_committee_history(cur, height, Committee(
                    starting_round=u64(round_),
                    members=Vec[Tuple[Address, u64, bool_], u16]([
                        Tuple[Address, u64, bool_]((address, amount, is_open)) for address, (amount, is_open) in committee_members.items()
                    ]),
                    total_stake=u64(sum(x[0] for x in committee_members.values()))
                ))
            elif isinstance(ratification, PuzzleRewardRatify):
                if ratification.amount == 0:
                    continue
                account_mapping_id = Field.loads(cached_get_mapping_id("credits.aleo", "account"))

                if account_mapping_id not in global_mapping_cache:
                    from interpreter.finalizer import mapping_cache_read
                    global_mapping_cache[account_mapping_id] = await mapping_cache_read(self, "credits.aleo", "account")

                current_balances: dict[Field, dict[str, Any]] = global_mapping_cache[account_mapping_id]

                operations: list[dict[str, Any]] = []
                for address, amount in address_puzzle_rewards.items():
                    key = LiteralPlaintext(literal=Literal(type_=Literal.Type.Address, primitive=Address.loads(address)))
                    key_id = Field.loads(cached_get_key_id("credits.aleo", "account", key.dump()))
                    if key_id not in current_balances:
                        current_balance = u64()
                    else:
                        current_balance_data = current_balances[key_id]
                        value = current_balance_data["value"]
                        if not isinstance(value, PlaintextValue):
                            raise RuntimeError("invalid account value")
                        plaintext = value.plaintext
                        if not isinstance(plaintext, LiteralPlaintext) or not isinstance(plaintext.literal.primitive, u64):
                            raise RuntimeError("invalid account value")
                        current_balance = plaintext.literal.primitive
                    new_value = current_balance + u64(amount)
                    value = PlaintextValue(plaintext=LiteralPlaintext(literal=Literal(type_=Literal.Type.U64, primitive=new_value)))
                    value_id = Field.loads(aleo_explorer_rust.get_value_id(str(key_id), value.dump()))
                    global_mapping_cache[account_mapping_id][key_id] = {
                        "key": key,
                        "value": value,
                    }
                    operations.append({
                        "type": FinalizeOperation.Type.UpdateKeyValue,
                        "mapping_id": account_mapping_id,
                        "key_id": key_id,
                        "value_id": value_id,
                        "program_name": "credits.aleo",
                        "mapping_name": "account",
                        "key": key,
                        "value": value,
                        "height": height,
                        "from_transaction": False,
                    })
                from interpreter.interpreter import execute_operations
                await execute_operations(self, cur, operations)

    @staticmethod
    async def _backup_redis_hash_key(redis_conn: Redis[str], keys: list[str], height: int):
        if height != 0:
            for key in keys:
                backup_key = f"{key}:rollback_backup:{height}"
                if await redis_conn.exists(backup_key) == 0:
                    if await redis_conn.exists(key) == 1:
                        await redis_conn.copy(key, backup_key)
                else:
                    await redis_conn.copy(backup_key, key, replace=True)

    @staticmethod
    async def _redis_cleanup(redis_conn: Redis[str], keys: list[str], height: int, rollback: bool):
        if height != 0:
            for key in keys:
                backup_key = f"{key}:rollback_backup:{height}"
                if rollback:
                    if await redis_conn.exists(backup_key) == 1:
                        await redis_conn.copy(backup_key, key, replace=True)
                else:
                    await redis_conn.delete(backup_key)


    @profile
    async def _save_block(self, block: Block):
        async with self.pool.connection() as conn:
            signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGINT})
            async with conn.transaction():
                async with conn.cursor() as cur:
                    height = block.height
                    # redis is not protected by transaction so manually saving here
                    redis_keys = [
                        "credits.aleo:bonded",
                        "credits.aleo:committee",
                        "address_stake_reward",
                        "address_transfer_in",
                        "address_transfer_out",
                        "address_fee",
                    ]
                    await self._backup_redis_hash_key(self.redis, redis_keys, height)
                    signal.pthread_sigmask(signal.SIG_UNBLOCK, {signal.SIGINT})

                    try:
                        if block.height != 0:
                            block_reward, coinbase_reward = block.compute_rewards(
                                await self.get_latest_coinbase_target(),
                                await self.get_latest_cumulative_proof_target()
                            )
                            puzzle_reward = coinbase_reward // 2
                        else:
                            block_reward, coinbase_reward, puzzle_reward = 0, 0, 0

                        block_reward += await block.get_total_priority_fee(self)

                        for ratification in block.ratifications:
                            if isinstance(ratification, BlockRewardRatify):
                                if ratification.amount != block_reward:
                                    raise RuntimeError("invalid block reward")
                            elif isinstance(ratification, PuzzleRewardRatify):
                                if ratification.amount != puzzle_reward:
                                    raise RuntimeError("invalid puzzle reward")
                            elif isinstance(ratification, GenesisRatify):
                                await self._pre_ratify(cur, ratification)

                        from interpreter.interpreter import finalize_block
                        reject_reasons = await finalize_block(self, cur, block)

                        await cur.execute(
                            "INSERT INTO block (height, block_hash, previous_hash, previous_state_root, transactions_root, "
                            "finalize_root, ratifications_root, solutions_root, subdag_root, round, cumulative_weight, "
                            "cumulative_proof_target, coinbase_target, proof_target, last_coinbase_target, "
                            "last_coinbase_timestamp, timestamp, block_reward, coinbase_reward) "
                            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                            "RETURNING id",
                            (block.height, str(block.block_hash), str(block.previous_hash), str(block.header.previous_state_root),
                             str(block.header.transactions_root), str(block.header.finalize_root), str(block.header.ratifications_root),
                             str(block.header.solutions_root), str(block.header.subdag_root), block.round,
                             block.header.metadata.cumulative_weight, block.header.metadata.cumulative_proof_target,
                             block.header.metadata.coinbase_target, block.header.metadata.proof_target,
                             block.header.metadata.last_coinbase_target, block.header.metadata.last_coinbase_timestamp,
                             block.header.metadata.timestamp, block_reward, coinbase_reward)
                        )
                        if (res := await cur.fetchone()) is None:
                            raise RuntimeError("failed to insert row into database")
                        block_db_id = res["id"]

                        # dag_transmission_ids: tuple[dict[str, int], dict[str, int]] = {}, {}

                        if isinstance(block.authority, BeaconAuthority):
                            await cur.execute(
                                "INSERT INTO authority (block_id, type, signature) VALUES (%s, %s, %s)",
                                (block_db_id, block.authority.type.name, str(block.authority.signature))
                            )
                        elif isinstance(block.authority, QuorumAuthority):
                            await cur.execute(
                                "INSERT INTO authority (block_id, type) VALUES (%s, %s) RETURNING id",
                                (block_db_id, block.authority.type.name)
                            )
                            if (res := await cur.fetchone()) is None:
                                raise RuntimeError("failed to insert row into database")
                            authority_db_id = res["id"]
                            subdag = block.authority.subdag
                            subdag_copy_data: list[tuple[int, int, Optional[str], str, str, int, str, int]] = []
                            for round_, certificates in subdag.subdag.items():
                                for index, certificate in enumerate(certificates):
                                    if round_ != certificate.batch_header.round:
                                        raise ValueError("invalid subdag round")
                                    if isinstance(certificate, BatchCertificate1):
                                        subdag_copy_data.append((
                                            authority_db_id, round_, str(certificate.certificate_id), str(certificate.batch_header.batch_id),
                                            str(certificate.batch_header.author), certificate.batch_header.timestamp,
                                            str(certificate.batch_header.signature), index
                                        ))
                                        # await cur.execute(
                                        #     "INSERT INTO dag_vertex (authority_id, round, batch_certificate_id, batch_id, "
                                        #     "author, timestamp, author_signature, index) "
                                        #     "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id",
                                        #     (authority_db_id, round_, str(certificate.certificate_id), str(certificate.batch_header.batch_id),
                                        #      str(certificate.batch_header.author), certificate.batch_header.timestamp,
                                        #      str(certificate.batch_header.signature), index)
                                        # )
                                    elif isinstance(certificate, BatchCertificate2):
                                        subdag_copy_data.append((
                                            authority_db_id, round_, None, str(certificate.batch_header.batch_id),
                                            str(certificate.batch_header.author), certificate.batch_header.timestamp,
                                            str(certificate.batch_header.signature), index
                                        ))
                                        # await cur.execute(
                                        #     "INSERT INTO dag_vertex (authority_id, round, batch_id, "
                                        #     "author, timestamp, author_signature, index) "
                                        #     "VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id",
                                        #     (authority_db_id, round_, str(certificate.batch_header.batch_id),
                                        #      str(certificate.batch_header.author), certificate.batch_header.timestamp,
                                        #      str(certificate.batch_header.signature), index)
                                        # )

                                    # if (res := await cur.fetchone()) is None:
                                    #     raise RuntimeError("failed to insert row into database")
                                    # vertex_db_id = res["id"]

                                    # if isinstance(certificate, BatchCertificate1):
                                    #     for sig_index, (signature, timestamp) in enumerate(certificate.signatures):
                                    #         await cur.execute(
                                    #             "INSERT INTO dag_vertex_signature (vertex_id, signature, timestamp, index) "
                                    #             "VALUES (%s, %s, %s, %s)",
                                    #             (vertex_db_id, str(signature), timestamp, sig_index)
                                    #         )
                                    # elif isinstance(certificate, BatchCertificate2):
                                    #     for sig_index, signature in enumerate(certificate.signatures):
                                    #         await cur.execute(
                                    #             "INSERT INTO dag_vertex_signature (vertex_id, signature, index) "
                                    #             "VALUES (%s, %s, %s)",
                                    #             (vertex_db_id, str(signature), sig_index)
                                    #         )
                                    #
                                    # prev_cert_ids = certificate.batch_header.previous_certificate_ids
                                    # await cur.execute(
                                    #     "SELECT v.id, batch_certificate_id FROM dag_vertex v "
                                    #     "JOIN UNNEST(%s::text[]) WITH ORDINALITY c(id, ord) ON v.batch_certificate_id = c.id "
                                    #     "ORDER BY ord",
                                    #     (list(map(str, prev_cert_ids)),)
                                    # )
                                    # res = await cur.fetchall()
                                    # temp allow
                                    # if len(res) != len(prev_cert_ids):
                                    #     raise RuntimeError("dag referenced unknown previous certificate")
                                    # prev_vertex_db_ids = {x["batch_certificate_id"]: x["id"] for x in res}
                                    # adj_copy_data: list[tuple[int, int, int]] = []
                                    # for prev_index, prev_cert_id in enumerate(prev_cert_ids):
                                    #     if str(prev_cert_id) in prev_vertex_db_ids:
                                    #         adj_copy_data.append((vertex_db_id, prev_vertex_db_ids[str(prev_cert_id)], prev_index))
                                    # async with cur.copy("COPY dag_vertex_adjacency (vertex_id, previous_vertex_id, index) FROM STDIN") as copy:
                                    #     for row in adj_copy_data:
                                    #         await copy.write_row(row)

                                    # tid_copy_data: list[tuple[int, str, int, Optional[str], Optional[str]]] = []
                                    # for tid_index, transmission_id in enumerate(certificate.batch_header.transmission_ids):
                                    #     if isinstance(transmission_id, SolutionTransmissionID):
                                    #         tid_copy_data.append((vertex_db_id, transmission_id.type.name, tid_index, str(transmission_id.id), None))
                                    #         dag_transmission_ids[0][str(transmission_id.id)] = vertex_db_id
                                    #     elif isinstance(transmission_id, TransactionTransmissionID):
                                    #         tid_copy_data.append((vertex_db_id, transmission_id.type.name, tid_index, None, str(transmission_id.id)))
                                    #         dag_transmission_ids[1][str(transmission_id.id)] = vertex_db_id
                                    #     elif isinstance(transmission_id, RatificationTransmissionID):
                                    #         tid_copy_data.append((vertex_db_id, transmission_id.type.name, tid_index, None, None))
                                    #     else:
                                    #         raise NotImplementedError
                                    # async with cur.copy("COPY dag_vertex_transmission_id (vertex_id, type, index, commitment, transaction_id) FROM STDIN") as copy:
                                    #     for row in tid_copy_data:
                                    #         await copy.write_row(row)

                        async with cur.copy(
                            "COPY dag_vertex (authority_id, round, batch_certificate_id, batch_id, "
                            "author, timestamp, author_signature, index) FROM STDIN"
                        ) as copy:
                            for row in subdag_copy_data:
                                await copy.write_row(row)

                        ignore_deploy_txids: list[str] = []
                        program_name_seen: dict[str, str] = {}
                        for confirmed_transaction in block.transactions:
                            if isinstance(confirmed_transaction, AcceptedDeploy):
                                transaction_id = str(confirmed_transaction.transaction.id)
                                transaction = confirmed_transaction.transaction
                                if isinstance(transaction, DeployTransaction):
                                    program_name = str(transaction.deployment.program.id)
                                    if program_name in program_name_seen:
                                        ignore_deploy_txids.append(program_name_seen[program_name])
                                    program_name_seen[program_name] = transaction_id
                                else:
                                    raise ValueError("expected deploy transaction")

                        for ct_index, confirmed_transaction in enumerate(block.transactions):
                            await cur.execute(
                                "INSERT INTO confirmed_transaction (block_id, index, type) VALUES (%s, %s, %s) RETURNING id",
                                (block_db_id, confirmed_transaction.index, confirmed_transaction.type.name)
                            )
                            if (res := await cur.fetchone()) is None:
                                raise RuntimeError("failed to insert row into database")
                            confirmed_transaction_db_id = res["id"]

                            transaction = confirmed_transaction.transaction

                            await self._insert_transaction(conn, self.redis, transaction, confirmed_transaction, ct_index, ignore_deploy_txids,
                                                           confirmed_transaction_db_id, reject_reasons)

                            update_copy_data: list[tuple[int, str, str, str]] = []
                            for index, finalize_operation in enumerate(confirmed_transaction.finalize):
                                await cur.execute(
                                    "INSERT INTO finalize_operation (confirmed_transaction_id, type, index) "
                                    "VALUES (%s, %s, %s) RETURNING id",
                                    (confirmed_transaction_db_id, finalize_operation.type.name, index)
                                )
                                if (res := await cur.fetchone()) is None:
                                    raise RuntimeError("failed to insert row into database")
                                finalize_operation_db_id = res["id"]
                                if isinstance(finalize_operation, InitializeMapping):
                                    await cur.execute(
                                        "INSERT INTO finalize_operation_initialize_mapping (finalize_operation_id, "
                                        "mapping_id) VALUES (%s, %s)",
                                        (finalize_operation_db_id, str(finalize_operation.mapping_id))
                                    )
                                elif isinstance(finalize_operation, InsertKeyValue):
                                    await cur.execute(
                                        "INSERT INTO finalize_operation_insert_kv (finalize_operation_id, "
                                        "mapping_id, key_id, value_id) VALUES (%s, %s, %s, %s)",
                                        (finalize_operation_db_id, str(finalize_operation.mapping_id),
                                         str(finalize_operation.key_id), str(finalize_operation.value_id))
                                    )
                                elif isinstance(finalize_operation, UpdateKeyValue):
                                    update_copy_data.append((
                                        finalize_operation_db_id, str(finalize_operation.mapping_id),
                                        str(finalize_operation.key_id), str(finalize_operation.value_id)
                                    ))
                                elif isinstance(finalize_operation, RemoveKeyValue):
                                    await cur.execute(
                                        "INSERT INTO finalize_operation_remove_kv (finalize_operation_id, "
                                        "mapping_id) VALUES (%s, %s)",
                                        (finalize_operation_db_id, str(finalize_operation.mapping_id))
                                    )
                                elif isinstance(finalize_operation, RemoveMapping):
                                    await cur.execute(
                                        "INSERT INTO finalize_operation_remove_mapping (finalize_operation_id, "
                                        "mapping_id) VALUES (%s, %s)",
                                        (finalize_operation_db_id, str(finalize_operation.mapping_id))
                                    )
                            if update_copy_data:
                                async with cur.copy("COPY finalize_operation_update_kv (finalize_operation_id, mapping_id, key_id, value_id) FROM STDIN") as copy:
                                    for row in update_copy_data:
                                        await copy.write_row(row)

                        for index, ratify in enumerate(block.ratifications):
                            if isinstance(ratify, GenesisRatify):
                                await cur.execute(
                                    "INSERT INTO ratification (block_id, index, type) VALUES (%s, %s, %s)",
                                    (block_db_id, index, ratify.type.name)
                                )
                                public_balances = ratify.public_balances
                                for address, balance in public_balances:
                                    await cur.execute(
                                        "INSERT INTO ratification_genesis_balance (address, amount) VALUES (%s, %s)",
                                        (str(address), balance)
                                    )
                            elif isinstance(ratify, (BlockRewardRatify, PuzzleRewardRatify)):
                                await cur.execute(
                                    "INSERT INTO ratification (block_id, index, type, amount) VALUES (%s, %s, %s, %s)",
                                    (block_db_id, index, ratify.type.name, ratify.amount)
                                )
                            else:
                                raise NotImplementedError

                        address_puzzle_rewards: dict[str, int] = defaultdict(int)

                        if block.solutions.value is not None:
                            prover_solutions = block.solutions.value.solutions
                            solutions: list[tuple[ProverSolution, int, int]] = []
                            prover_solutions_target = list(zip(
                                prover_solutions,
                                [prover_solution.partial_solution.commitment.to_target() for prover_solution
                                 in prover_solutions]
                            ))
                            target_sum = sum(target for _, target in prover_solutions_target)
                            for prover_solution, target in prover_solutions_target:
                                solutions.append((prover_solution, target, puzzle_reward * target // target_sum))

                            await cur.execute(
                                "INSERT INTO coinbase_solution (block_id, target_sum) "
                                "VALUES (%s, %s) RETURNING id",
                                (block_db_id, target_sum)
                            )
                            if (res := await cur.fetchone()) is None:
                                raise RuntimeError("failed to insert row into database")
                            coinbase_solution_db_id = res["id"]
                            await cur.execute("SELECT total_credit FROM leaderboard_total")
                            current_total_credit = await cur.fetchone()
                            if current_total_credit is None:
                                await cur.execute("INSERT INTO leaderboard_total (total_credit) VALUES (0)")
                                current_total_credit = 0
                            else:
                                current_total_credit = current_total_credit["total_credit"]
                            copy_data: list[tuple[None, int, str, u64, str, int, int, str, bool]] = []
                            for prover_solution, target, reward in solutions:
                                partial_solution = prover_solution.partial_solution
                                # dag_vertex_db_id = dag_transmission_ids[0][str(partial_solution.commitment)]
                                copy_data.append(
                                    (None, coinbase_solution_db_id, str(partial_solution.address), partial_solution.nonce,
                                     str(partial_solution.commitment), partial_solution.commitment.to_target(), reward,
                                     str(prover_solution.proof.w.x), prover_solution.proof.w.y_is_positive)
                                )
                                if reward > 0:
                                    address_puzzle_rewards[str(partial_solution.address)] += reward
                            if not os.environ.get("DEBUG_SKIP_COINBASE"):
                                async with cur.copy("COPY prover_solution (dag_vertex_id, coinbase_solution_id, address, nonce, commitment, target, reward, proof_x, proof_y_is_positive) FROM STDIN") as copy:
                                    for row in copy_data:
                                        await copy.write_row(row)
                                if block.header.metadata.height >= 130888 and block.header.metadata.timestamp < 1675209600 and current_total_credit < 37_500_000_000_000:
                                    await cur.execute(
                                        "UPDATE leaderboard_total SET total_credit = leaderboard_total.total_credit + %s",
                                        (sum(reward for _, _, reward in solutions),)
                                    )
                                for address, reward in address_puzzle_rewards.items():
                                    await cur.execute(
                                        "INSERT INTO leaderboard (address, total_reward) VALUES (%s, %s) "
                                        "ON CONFLICT (address) DO UPDATE SET total_reward = leaderboard.total_reward + %s",
                                        (address, reward, reward)
                                    )
                                    if block.header.metadata.height >= 130888 and block.header.metadata.timestamp < 1675209600 and current_total_credit < 37_500_000_000_000:
                                        await cur.execute(
                                            "UPDATE leaderboard SET total_incentive = leaderboard.total_incentive + %s WHERE address = %s",
                                            (reward, address)
                                        )

                        for aborted in block.aborted_transactions_ids:
                            await cur.execute(
                                "INSERT INTO block_aborted_transaction_id (block_id, transaction_id) VALUES (%s, %s)",
                                (block_db_id, str(aborted))
                            )

                        await self._post_ratify(cur, self.redis, block.height, block.round, block.ratifications.ratifications, address_puzzle_rewards)

                        signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGINT})
                        await self._redis_cleanup(self.redis, redis_keys, block.height, False)

                        await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseBlockAdded, block.header.metadata.height))
                    except Exception as e:
                        signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGINT})
                        await self._redis_cleanup(self.redis, redis_keys, block.height, True)
                        signal.pthread_sigmask(signal.SIG_UNBLOCK, {signal.SIGINT})
                        await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                        raise
            signal.pthread_sigmask(signal.SIG_UNBLOCK, {signal.SIGINT})

    async def save_block(self, block: Block):
        await self._save_block(block)

    async def save_unconfirmed_transaction(self, transaction: Transaction):
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                if isinstance(transaction, FeeTransaction):
                    raise RuntimeError("rejected transaction cannot be unconfirmed")
                await self._insert_transaction(conn, self.redis, transaction)

    async def save_feedback(self, contact: str, content: str):
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("INSERT INTO feedback (contact, content) VALUES (%s, %s)", (contact, content))
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise