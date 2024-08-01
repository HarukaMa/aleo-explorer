
from __future__ import annotations

import os
import signal
import time
from collections import defaultdict

import psycopg.sql
from psycopg.rows import DictRow
from redis.asyncio import Redis

from aleo_types import *
from aleo_types.cached import cached_get_key_id, cached_get_mapping_id, cached_compute_key_to_address
from disasm.utils import value_type_to_mode_type_str, plaintext_type_to_str
from explorer.types import Message as ExplorerMessage
from util.global_cache import global_mapping_cache
from .base import DatabaseBase, profile
from .util import DatabaseUtil


class _SupplyTracker:

    def __init__(self, previous_supply: int):
        self.supply = previous_supply
        self.actual_block_reward = 0
        self.actual_puzzle_reward = 0

    def mint(self, delta: int):
        self.supply += delta

    def tally_block_reward(self, reward: int):
        self.actual_block_reward += reward

    def tally_puzzle_reward(self, reward: int):
        self.actual_puzzle_reward += reward

    def burn(self, delta: int):
        self.supply -= delta


class DatabaseInsert(DatabaseBase):

    def __init__(self, *args, **kwargs): # type: ignore
        super().__init__(*args, **kwargs)
        self.redis_last_history_time = time.monotonic() - 21600
        self.redis_keys = [
            "credits.aleo:bonded",
            "credits.aleo:delegated",
            "credits.aleo:committee",
            "address_stake_reward",
            "address_puzzle_reward",
            "address_transfer_in",
            "address_transfer_out",
            "address_fee",
        ]

    @staticmethod
    async def _insert_future(conn: psycopg.AsyncConnection[DictRow], future: Future,
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

    async def _update_address_stats(self, transaction: Transaction):

        if isinstance(transaction, DeployTransaction):
            transitions = [cast(Fee, transaction.fee).transition]
        elif isinstance(transaction, ExecuteTransaction):
            transitions = list(transaction.execution.transitions)
            fee = cast(Option[Fee], transaction.fee)
            if fee.value is not None:
                transitions.append(fee.value.transition)
        elif isinstance(transaction, FeeTransaction):
            transitions = [cast(Fee, transaction.fee).transition]
        else:
            raise NotImplementedError

        for transition in transitions:
            if transition.program_id == "credits.aleo":
                transfer_from = None
                transfer_to = None
                fee_from = None
                if str(transition.function_name) in ("transfer_public", "transfer_public_as_signer"):
                    output = cast(FutureTransitionOutput, transition.outputs[0])
                    future = cast(Future, output.future.value)
                    transfer_from = str(DatabaseUtil.get_primitive_from_argument_unchecked(future.arguments[0]))
                    transfer_to = str(DatabaseUtil.get_primitive_from_argument_unchecked(future.arguments[1]))
                    amount = int(cast(u64, DatabaseUtil.get_primitive_from_argument_unchecked(future.arguments[2])))
                elif transition.function_name == "transfer_private_to_public":
                    output = cast(FutureTransitionOutput, transition.outputs[1])
                    future = cast(Future, output.future.value)
                    transfer_to = str(DatabaseUtil.get_primitive_from_argument_unchecked(future.arguments[0]))
                    amount = int(cast(u64, DatabaseUtil.get_primitive_from_argument_unchecked(future.arguments[1])))
                elif transition.function_name == "transfer_public_to_private":
                    output = cast(FutureTransitionOutput, transition.outputs[1])
                    future = cast(Future, output.future.value)
                    transfer_from = str(DatabaseUtil.get_primitive_from_argument_unchecked(future.arguments[0]))
                    amount = int(cast(u64, DatabaseUtil.get_primitive_from_argument_unchecked(future.arguments[1])))
                elif transition.function_name == "fee_public":
                    output = cast(FutureTransitionOutput, transition.outputs[0])
                    future = cast(Future, output.future.value)
                    fee_from = str(DatabaseUtil.get_primitive_from_argument_unchecked(future.arguments[0]))
                    amount = int(cast(u64, DatabaseUtil.get_primitive_from_argument_unchecked(future.arguments[1])))
                elif transition.function_name == "bond_validator":
                    output = cast(FutureTransitionOutput, transition.outputs[0])
                    future = cast(Future, output.future.value)
                    transfer_from = str(DatabaseUtil.get_primitive_from_argument_unchecked(future.arguments[0]))
                    amount = int(cast(u64, DatabaseUtil.get_primitive_from_argument_unchecked(future.arguments[2])))
                elif transition.function_name == "bond_public":
                    output = cast(FutureTransitionOutput, transition.outputs[0])
                    future = cast(Future, output.future.value)
                    transfer_from = str(DatabaseUtil.get_primitive_from_argument_unchecked(future.arguments[0]))
                    amount = int(cast(u64, DatabaseUtil.get_primitive_from_argument_unchecked(future.arguments[3])))
                elif transition.function_name == "claim_unbond_public":
                    output = cast(FutureTransitionOutput, transition.outputs[0])
                    future = cast(Future, output.future.value)
                    staker_plaintext = cast(LiteralPlaintext, cast(PlaintextArgument, future.arguments[0]).plaintext)
                    unbonding_key_id = cached_get_key_id("credits.aleo", "unbonding", staker_plaintext.dump())
                    withdraw_key_id = cached_get_key_id("credits.aleo", "withdraw", staker_plaintext.dump())
                    from db.mapping import DatabaseMapping
                    unbonding_bytes = await cast(DatabaseMapping, self).get_mapping_value("credits.aleo", "unbonding", unbonding_key_id)
                    if unbonding_bytes is None:
                        raise RuntimeError("unbonding key not found")
                    unbonding = cast(StructPlaintext, cast(PlaintextValue, Value.load(BytesIO(unbonding_bytes))).plaintext)
                    withdraw_bytes = await cast(DatabaseMapping, self).get_mapping_value("credits.aleo", "withdraw", withdraw_key_id)
                    if withdraw_bytes is None:
                        raise RuntimeError("withdraw key not found")
                    withdraw = cast(LiteralPlaintext, cast(PlaintextValue, Value.load(BytesIO(withdraw_bytes))).plaintext)
                    transfer_to = str(withdraw.literal.primitive)
                    amount = int(cast(u64, cast(LiteralPlaintext, unbonding["microcredits"]).literal.primitive))
                else:
                    return

                if transfer_from != transfer_to:
                    if transfer_from is not None:
                        await self.redis.hincrby("address_transfer_out", transfer_from, amount) # type: ignore
                    if transfer_to is not None:
                        await self.redis.hincrby("address_transfer_in", transfer_to, amount) # type: ignore

                if fee_from is not None:
                    await self.redis.hincrby("address_fee", fee_from, amount) # type: ignore

    @staticmethod
    async def _insert_transition(conn: psycopg.AsyncConnection[DictRow], redis_conn: Redis[str],
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
                "function_name, tpk, tcm, index, scm) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id",
                (str(transition.id), exe_tx_db_id, fee_db_id, str(transition.program_id),
                 str(transition.function_name), str(transition.tpk), str(transition.tcm), ts_index, str(transition.scm))
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
                        plaintext = transition_input.plaintext.value
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


    @staticmethod
    async def _insert_deploy_transaction(conn: psycopg.AsyncConnection[DictRow], redis: Redis[str],
                                         deployment: Deployment, owner: ProgramOwner, fee: Fee, transaction_db_id: int,
                                         is_unconfirmed: bool = False, is_rejected: bool = False, fee_should_exist: bool = False):
        async with conn.cursor() as cur:
            if is_unconfirmed or is_rejected:
                program_id = str(deployment.program.id)
                owner_db = str(owner.address)
            else:
                program_id = None
                owner_db = None
            await cur.execute(
                "SELECT id FROM transaction_deploy WHERE transaction_id = %s", (transaction_db_id,)
            )
            if await cur.fetchone() is not None:
                if not fee_should_exist:
                    raise RuntimeError("transaction deploy already exists in database")
                else:
                    return
            await cur.execute(
                "INSERT INTO transaction_deploy (transaction_id, edition, verifying_keys, program_id, owner) "
                "VALUES (%s, %s, %s, %s, %s) RETURNING id",
                (transaction_db_id, deployment.edition, deployment.verifying_keys.dump(), program_id, owner_db)
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
    async def _insert_execute_transaction(conn: psycopg.AsyncConnection[DictRow], redis: Redis[str],
                                          execution: Execution, fee: Optional[Fee], transaction_db_id: int,
                                          is_rejected: bool = False, ts_should_exist: bool = False):
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT id FROM transaction_execute WHERE transaction_id = %s", (transaction_db_id,)
            )
            if await cur.fetchone() is not None:
                if not ts_should_exist:
                    raise RuntimeError("transaction execute already exists in database")
                else:
                    return
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

    async def _insert_transaction(self, conn: psycopg.AsyncConnection[DictRow], redis: Redis[str], transaction: Transaction,
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
                # check for existing transactions and remove unconfirmed transactions
                # wasteful for now, just a strange edge case avoidance
                # TODO: refactor
                if confirmed_transaction is not None:
                    if isinstance(confirmed_transaction, AcceptedDeploy):
                        if not isinstance(transaction, DeployTransaction):
                            raise RuntimeError("expected a deploy transaction for accepted deploy")
                        fee = cast(Fee, transaction.fee)
                        find_transition_id = fee.transition.id
                        await cur.execute(
                            "SELECT tx.id, tx.transaction_id FROM transaction tx "
                            "JOIN fee f on tx.id = f.transaction_id "
                            "JOIN transition t on f.id = t.fee_id "
                            "WHERE t.transition_id = %s AND tx.confirmed_transaction_id IS NULL",
                            (str(find_transition_id),)
                        )
                        res = await cur.fetchall()
                    elif isinstance(confirmed_transaction, AcceptedExecute):
                        if not isinstance(transaction, ExecuteTransaction):
                            raise RuntimeError("expected an execute transaction for accepted execute")
                        find_transition_ids = list(map(lambda x: str(x.id), transaction.execution.transitions))
                        await cur.execute(
                            "SELECT tx.id, tx.transaction_id FROM transaction tx "
                            "JOIN transaction_execute te on tx.id = te.transaction_id "
                            "JOIN transition t on te.id = t.transaction_execute_id "
                            "WHERE t.transition_id = ANY(%s::text[]) AND tx.confirmed_transaction_id IS NULL",
                            (find_transition_ids,)
                        )
                        res = await cur.fetchall()
                        fee = cast(Option[Fee], transaction.fee)
                        if (fee := fee.value) is not None:
                            await cur.execute(
                                "SELECT tx.id, tx.transaction_id FROM transaction tx "
                                "JOIN fee f on tx.id = f.transaction_id "
                                "JOIN transition t on f.id = t.fee_id "
                                "WHERE t.transition_id = %s AND tx.confirmed_transaction_id IS NULL",
                                (str(fee.transition.id),)
                            )
                            for row in await cur.fetchall():
                                if row not in res:
                                    res.append(row)
                    else:
                        res = []
                    for row in res:
                        print("removing strange unconfirmed transaction:", row["transaction_id"])
                        await cur.execute(
                            "DELETE FROM transaction WHERE id = %s",
                            (row["id"],)
                        )

                if isinstance(transaction, FeeTransaction): # check probable rejected unconfirmed transaction
                    if confirmed_transaction is None:
                        raise RuntimeError("expected a confirmed transaction for fee transaction")
                    if isinstance(confirmed_transaction, RejectedDeploy):
                        rejected_deployment = cast(RejectedDeployment, confirmed_transaction.rejected)
                        fee = cast(Fee, transaction.fee)
                        ref_transition_id = fee.transition.id
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
                            await DatabaseInsert._insert_deploy_transaction(conn, redis, rejected_deployment.deploy, rejected_deployment.program_owner, fee, transaction_db_id, is_rejected=True, fee_should_exist=True)

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
                            await DatabaseInsert._insert_execute_transaction(conn, redis, rejected_execution.execution,
                                                                             cast(Fee, transaction.fee),
                                                                             transaction_db_id, is_rejected=True,
                                                                             ts_should_exist=True)

                if not prior_tx:
                    original_transaction_id = None
                    if confirmed_transaction is not None:
                        original_transaction_id = aleo_explorer_rust.rejected_tx_original_id(confirmed_transaction.dump())
                    await cur.execute(
                        "INSERT INTO transaction (transaction_id, type, original_transaction_id) "
                        "VALUES (%s, %s, %s) RETURNING id",
                        (str(transaction.id), transaction.type.name, original_transaction_id)
                    )
                    if (res := await cur.fetchone()) is None:
                        raise RuntimeError("failed to insert row into database")
                    transaction_db_id = res["id"]
                if transaction_db_id == -1:
                    raise RuntimeError("failed to get transaction id")

                if isinstance(transaction, DeployTransaction): # accepted deploy / unconfirmed
                    await DatabaseInsert._insert_deploy_transaction(
                        conn, redis, transaction.deployment, transaction.owner, cast(Fee, transaction.fee), transaction_db_id,
                        is_unconfirmed=(confirmed_transaction is None)
                    )

                elif isinstance(transaction, ExecuteTransaction): # accepted execute / unconfirmed
                    await DatabaseInsert._insert_execute_transaction(conn, redis, transaction.execution,
                                                                     cast(Option[Fee], transaction.fee).value,
                                                                     transaction_db_id)

                elif isinstance(transaction, FeeTransaction) and not prior_tx: # first seen rejected tx
                    if isinstance(confirmed_transaction, RejectedDeploy):
                        rejected_deployment = cast(RejectedDeployment, confirmed_transaction.rejected)
                        await DatabaseInsert._insert_deploy_transaction(conn, redis, rejected_deployment.deploy, rejected_deployment.program_owner, cast(Fee, transaction.fee), transaction_db_id, is_rejected=True)
                    elif isinstance(confirmed_transaction, RejectedExecute):
                        rejected_execution = cast(RejectedExecution, confirmed_transaction.rejected)
                        await DatabaseInsert._insert_execute_transaction(conn, redis, rejected_execution.execution,
                                                                         cast(Fee, transaction.fee), transaction_db_id,
                                                                         is_rejected=True)

            # confirming tx
            if confirmed_transaction is not None:
                await cur.execute(
                    "UPDATE transaction SET confirmed_transaction_id = %s WHERE transaction_id = %s",
                    (confirmed_transaction_db_id, str(transaction.id))
                )
                reject_reasons = cast(list[Optional[str]], reject_reasons)
                ct_index = cast(int, ct_index)
                if isinstance(confirmed_transaction, AcceptedDeploy):
                    transaction = cast(DeployTransaction, transaction)
                    if reject_reasons[ct_index] is not None:
                        raise RuntimeError("expected no rejected reason for accepted deploy transaction")
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

                await self._update_address_stats(transaction)

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

    @profile
    async def _update_committee_bonded_delegated_map(
        self,
        committee_members: dict[Address, tuple[u64, bool_, u8]],
        stakers: dict[Address, tuple[Address, u64]],
        delegated: dict[Address, u64],
    ):
        committee_mapping_id = Field.loads(cached_get_mapping_id("credits.aleo", "committee"))
        bonded_mapping_id = Field.loads(cached_get_mapping_id("credits.aleo", "bonded"))
        delegated_mapping_id = Field.loads(cached_get_mapping_id("credits.aleo", "delegated"))

        global_mapping_cache[committee_mapping_id] = {}
        committee_mapping: dict[str, dict[str, str]] = {}
        for address, (_, is_open, commission) in committee_members.items():
            key = LiteralPlaintext(literal=Literal(type_=Literal.Type.Address, primitive=address))
            key_id = Field.loads(cached_get_key_id("credits.aleo", "committee", key.dump()))
            value = PlaintextValue(
                plaintext=StructPlaintext(
                    members=Vec[Tuple[Identifier, Plaintext], u8]([
                        Tuple[Identifier, Plaintext]((
                            Identifier.loads("is_open"),
                            LiteralPlaintext(literal=Literal(type_=Literal.Type.Boolean, primitive=is_open))
                        )),
                        Tuple[Identifier, Plaintext]((
                            Identifier.loads("commission"),
                            LiteralPlaintext(literal=Literal(type_=Literal.Type.U8, primitive=commission))
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
        await self.redis.execute_command("MULTI") # type: ignore
        await self.redis.delete("credits.aleo:committee")
        await self.redis.hset("credits.aleo:committee", mapping={k: json.dumps(v) for k, v in committee_mapping.items()})
        await self.redis.execute_command("EXEC") # type: ignore

        global_mapping_cache[bonded_mapping_id] = {}
        bonded_mapping: dict[str, dict[str, str]] = {}
        for address, (validator, amount) in stakers.items():
            key = LiteralPlaintext(literal=Literal(type_=Literal.Type.Address, primitive=address))
            key_id = Field.loads(cached_get_key_id("credits.aleo", "bonded", key.dump()))
            k = Tuple[Identifier, Plaintext]((
                Identifier.loads("validator"),
                LiteralPlaintext(literal=Literal(type_=Literal.Type.Address, primitive=validator))
            ))
            v = Tuple[Identifier, Plaintext]((
                Identifier.loads("microcredits"),
                LiteralPlaintext(literal=Literal(type_=Literal.Type.U64, primitive=amount))
            ))
            value = PlaintextValue(
                plaintext=StructPlaintext(
                    members=Vec[Tuple[Identifier, Plaintext], u8]([k, v])
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
        await self.redis.execute_command("MULTI") # type: ignore
        await self.redis.delete("credits.aleo:bonded")
        await self.redis.hset("credits.aleo:bonded", mapping={k: json.dumps(v) for k, v in bonded_mapping.items()})
        await self.redis.execute_command("EXEC") # type: ignore

        global_mapping_cache[delegated_mapping_id] = {}
        delegated_mapping: dict[str, dict[str, str]] = {}
        for validator, amount in delegated.items():
            key = LiteralPlaintext(literal=Literal(type_=Literal.Type.Address, primitive=validator))
            key_id = Field.loads(cached_get_key_id("credits.aleo", "delegated", key.dump()))
            value = PlaintextValue(plaintext=LiteralPlaintext(literal=Literal(type_=Literal.Type.U64, primitive=amount)))
            delegated_mapping[str(key_id)] = {
                "key": key.dump().hex(),
                "value": value.dump().hex(),
            }
            global_mapping_cache[delegated_mapping_id][key_id] = {
                "key": key,
                "value": value,
            }
        await self.redis.execute_command("MULTI") # type: ignore
        await self.redis.delete("credits.aleo:delegated")
        await self.redis.hset("credits.aleo:delegated", mapping={k: json.dumps(v) for k, v in delegated_mapping.items()})
        await self.redis.execute_command("EXEC") # type: ignore

    @staticmethod
    async def _save_committee_history(cur: psycopg.AsyncCursor[dict[str, Any]], height: int, committee: Committee):
        await cur.execute(
            "INSERT INTO committee_history (height, starting_round, total_stake, committee_id) "
            "VALUES (%s, %s, %s, %s) RETURNING id",
            (height, committee.starting_round, committee.total_stake, str(committee.id))
        )
        if (res := await cur.fetchone()) is None:
            raise RuntimeError("failed to insert row into database")
        committee_db_id = res["id"]
        for address, stake, is_open, commission in committee.members:
            await cur.execute(
                "INSERT INTO committee_history_member (committee_id, address, stake, is_open, commission) "
                "VALUES (%s, %s, %s, %s, %s)",
                (committee_db_id, str(address), stake, bool(is_open), commission)
            )

    @staticmethod
    def _stakers_to_delegated(stakers: dict[Address, tuple[Address, u64]]):
        delegated: dict[Address, u64] = {}
        for validator, amount in stakers.values():
            if validator in delegated:
                delegated[validator] += amount
            else:
                delegated[validator] = amount
        return delegated

    async def _pre_ratify(self, cur: psycopg.AsyncCursor[dict[str, Any]], ratification: GenesisRatify,
                          supply_tracker: _SupplyTracker):
        from interpreter.interpreter import global_mapping_cache
        committee = ratification.committee
        await DatabaseInsert._save_committee_history(cur, 0, committee)

        account_mapping_id = Field.loads(cached_get_mapping_id("credits.aleo", "account"))
        global_mapping_cache[account_mapping_id] = {}
        bonded_mapping_id = Field.loads(cached_get_mapping_id("credits.aleo", "bonded"))
        global_mapping_cache[bonded_mapping_id] = {}
        withdraw_mapping_id = Field.loads(cached_get_mapping_id("credits.aleo", "withdraw"))
        global_mapping_cache[withdraw_mapping_id] = {}
        metadata_mapping_id = Field.loads(cached_get_mapping_id("credits.aleo", "metadata"))
        global_mapping_cache[metadata_mapping_id] = {}

        bonded_balances = ratification.bonded_balances
        stakers: dict[Address, tuple[Address, u64]] = {}
        for staker, validator, _, amount in bonded_balances:
            stakers[staker] = validator, amount
        delegated: dict[Address, u64] = self._stakers_to_delegated(stakers)

        committee_members = {address: (amount, is_open, commission) for address, amount, is_open, commission in committee.members}
        await self._update_committee_bonded_delegated_map(committee_members, stakers, delegated)

        public_balances = ratification.public_balances
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
            supply_tracker.mint(balance)

        for staker, validator, withdrawal, amount in bonded_balances:
            key = LiteralPlaintext(literal=Literal(type_=Literal.Type.Address, primitive=staker))
            key_id = Field.loads(cached_get_key_id("credits.aleo", "withdraw", key.dump()))
            value = PlaintextValue(plaintext=LiteralPlaintext(literal=Literal(type_=Literal.Type.Address, primitive=withdrawal)))
            value_id = Field.loads(aleo_explorer_rust.get_value_id(str(key_id), value.dump()))
            global_mapping_cache[withdraw_mapping_id][key_id] = {
                "key": key,
                "value": value,
            }
            operations.append({
                "type": FinalizeOperation.Type.UpdateKeyValue,
                "mapping_id": withdraw_mapping_id,
                "key_id": key_id,
                "value_id": value_id,
                "key": key,
                "value": value,
                "height": 0,
                "program_name": "credits.aleo",
                "mapping_name": "withdraw",
                "from_transaction": False,
            })

            supply_tracker.mint(amount)

        key = LiteralPlaintext(
            literal=Literal(
                type_=Literal.Type.Address,
                primitive=Address.loads("aleo1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq3ljyzc")
            )
        )
        key_id = Field.loads(cached_get_key_id("credits.aleo", "metadata", key.dump()))
        value = PlaintextValue(plaintext=LiteralPlaintext(literal=Literal(type_=Literal.Type.U32, primitive=u32(len(committee_members)))))
        value_id = Field.loads(aleo_explorer_rust.get_value_id(str(key_id), value.dump()))
        global_mapping_cache[metadata_mapping_id][key_id] = {
            "key": key,
            "value": value,
        }
        operations.append({
            "type": FinalizeOperation.Type.UpdateKeyValue,
            "mapping_id": metadata_mapping_id,
            "key_id": key_id,
            "value_id": value_id,
            "key": key,
            "value": value,
            "height": 0,
            "program_name": "credits.aleo",
            "mapping_name": "metadata",
            "from_transaction": False,
        })

        key = LiteralPlaintext(
            literal=Literal(
                type_=Literal.Type.Address,
                primitive=Address.loads("aleo1qgqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqanmpl0")
            )
        )
        key_id = Field.loads(cached_get_key_id("credits.aleo", "metadata", key.dump()))
        value = PlaintextValue(plaintext=LiteralPlaintext(literal=Literal(type_=Literal.Type.U32, primitive=u32(len(bonded_balances) - len(committee_members)))))
        value_id = Field.loads(aleo_explorer_rust.get_value_id(str(key_id), value.dump()))
        global_mapping_cache[metadata_mapping_id][key_id] = {
            "key": key,
            "value": value,
        }
        operations.append({
            "type": FinalizeOperation.Type.UpdateKeyValue,
            "mapping_id": metadata_mapping_id,
            "key_id": key_id,
            "value_id": value_id,
            "key": key,
            "value": value,
            "height": 0,
            "program_name": "credits.aleo",
            "mapping_name": "metadata",
            "from_transaction": False,
        })

        from interpreter.interpreter import execute_operations
        await execute_operations(cast("Database", self), cur, operations)

    @staticmethod
    async def _get_committee_mapping_unchecked(redis_conn: Redis[str]) -> dict[Address, tuple[bool_, u8]]:
        data = await redis_conn.hgetall("credits.aleo:committee")
        committee_members: dict[Address, tuple[bool_, u8]] = {}
        for d in data.values():
            d = json.loads(d)
            key = cast(LiteralPlaintext, Plaintext.load(BytesIO(bytes.fromhex(d["key"]))))
            value = cast(PlaintextValue, Value.load(BytesIO(bytes.fromhex(d["value"]))))
            plaintext = cast(StructPlaintext, value.plaintext)
            is_open = cast(LiteralPlaintext, plaintext["is_open"])
            commission = cast(LiteralPlaintext, plaintext["commission"])
            committee_members[cast(Address, key.literal.primitive)] = (
                cast(bool_, is_open.literal.primitive),
                cast(u8, commission.literal.primitive),
            )
        return committee_members

    @staticmethod
    async def _get_delegated_mapping_unchecked(redis_conn: Redis[str]) -> dict[Address, u64]:
        data = await redis_conn.hgetall("credits.aleo:delegated")
        delegators: dict[Address, u64] = {}
        for d in data.values():
            d = json.loads(d)
            key = cast(LiteralPlaintext, Plaintext.load(BytesIO(bytes.fromhex(d["key"]))))
            value = cast(PlaintextValue, Value.load(BytesIO(bytes.fromhex(d["value"]))))
            plaintext = cast(LiteralPlaintext, value.plaintext)
            delegators[cast(Address, key.literal.primitive)] = cast(u64, plaintext.literal.primitive)
        return delegators

    async def get_bonded_mapping_unchecked(self) -> dict[Address, tuple[Address, u64]]:
        data = await self.redis.hgetall("credits.aleo:bonded")

        stakers: dict[Address, tuple[Address, u64]] = {}
        for d in data.values():
            d = json.loads(d)
            key = Plaintext.load(BytesIO(bytes.fromhex(d["key"])))
            value = Value.load(BytesIO(bytes.fromhex(d["value"])))
            plaintext = cast(PlaintextValue, value).plaintext
            validator = cast(StructPlaintext, plaintext)["validator"]
            amount = cast(StructPlaintext, plaintext)["microcredits"]
            stakers[cast(Address, cast(LiteralPlaintext, key).literal.primitive)] = (
                cast(Address, cast(LiteralPlaintext, validator).literal.primitive),
                cast(u64, cast(LiteralPlaintext, amount).literal.primitive)
            )
        return stakers

    @staticmethod
    @profile
    def _check_committee_staker_match(committee_members: dict[Address, tuple[u64, bool_, u8]],
                                      stakers: dict[Address, tuple[Address, u64]]):
        address_stakes: dict[Address, u64] = defaultdict(lambda: u64())
        for _, (validator, amount) in stakers.items():
            address_stakes[validator] += amount # type: ignore[reportGeneralTypeIssues]
        if len(address_stakes) != len(committee_members):
            raise RuntimeError("size mismatch between stakers and committee members")

        committee_total_stake = sum(amount for amount, _, _ in committee_members.values())
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
    @profile
    def _stake_rewards(committee_members: dict[Address, tuple[u64, bool_, u8]],
                       stakers: dict[Address, tuple[Address, u64]], block_reward: u64):
        total_stake = sum(x[0] for x in committee_members.values())
        stake_rewards: dict[Address, int] = {}
        if not stakers or total_stake == 0 or block_reward == 0:
            return stakers, stake_rewards

        new_stakers: dict[Address, tuple[Address, u64]] = {}

        for staker, (validator, stake) in stakers.items():
            if validator not in committee_members:
                new_stakers[staker] = validator, stake
                continue
            if committee_members[validator][0] > total_stake // 4:
                new_stakers[staker] = validator, stake
                continue
            if stake < 10_000_000_000 and staker != validator:
                new_stakers[staker] = validator, stake
                continue

            reward = int(block_reward) * stake // total_stake
            if staker == validator:
                delegated_stake = committee_members[validator][0] - stake
                commission = int(block_reward) * delegated_stake // total_stake * committee_members[validator][2] // 100
                reward += commission
            else:
                commission = reward * committee_members[validator][2] // 100
                reward -= commission
            stake_rewards[staker] = reward

            new_stake = stake + reward
            new_stakers[staker] = validator, u64(new_stake)

        return new_stakers, stake_rewards

    @staticmethod
    @profile
    def _next_committee_members(committee_members: dict[Address, tuple[u64, bool_, u8]],
                                stakers: dict[Address, tuple[Address, u64]]) -> dict[Address, tuple[u64, bool_, u8]]:
        validators: dict[Address, u64] = defaultdict(lambda: u64())
        for _, (validator, amount) in stakers.items():
            validators[validator] += amount # type: ignore[reportGeneralTypeIssues]
        new_committee_members: dict[Address, tuple[u64, bool_, u8]] = {}
        for validator, amount in validators.items():
            if validator in committee_members:
                new_committee_members[validator] = amount, committee_members[validator][1], committee_members[validator][2]
        return new_committee_members

    @staticmethod
    def _committee_delegated_to_members(committee: dict[Address, tuple[bool_, u8]],
                                        delegated: dict[Address, u64]) -> dict[Address, tuple[u64, bool_, u8]]:
        committee_members: dict[Address, tuple[u64, bool_, u8]] = {}
        for address, (is_open, commission) in committee.items():
            committee_members[address] = delegated[address], is_open, commission

        return committee_members

    @staticmethod
    def _next_delegated(stakers: dict[Address, tuple[Address, u64]]) -> dict[Address, u64]:
        delegated: dict[Address, u64] = defaultdict(u64)
        for _, (validator, amount) in stakers.items():
            delegated[validator] += amount
        return delegated

    @profile
    async def _post_ratify(self, cur: psycopg.AsyncCursor[dict[str, Any]], redis_conn: Redis[str], height: int, round_: int,
                           ratifications: list[Ratify], address_puzzle_rewards: dict[str, int], supply_tracker: _SupplyTracker):
        from interpreter.interpreter import global_mapping_cache

        for ratification in ratifications:
            if isinstance(ratification, BlockRewardRatify):
                committee = await self._get_committee_mapping_unchecked(redis_conn)
                delegated = await self._get_delegated_mapping_unchecked(redis_conn)
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
                    stakers = await self.get_bonded_mapping_unchecked()

                committee_members = self._committee_delegated_to_members(committee, delegated)

                stakers, stake_rewards = self._stake_rewards(committee_members, stakers, ratification.amount)
                delegated = self._next_delegated(stakers)
                committee_members = self._next_committee_members(committee_members, stakers)

                pipe = self.redis.pipeline()
                for address, amount in stake_rewards.items():
                    pipe.hincrby("address_stake_reward", str(address), amount)
                    supply_tracker.mint(amount)
                    supply_tracker.tally_block_reward(amount)
                await pipe.execute() # type: ignore

                await self._update_committee_bonded_delegated_map(committee_members, stakers, delegated)
                starting_round = u64(round_)
                members = Vec[Tuple[Address, u64, bool_, u8], u16]([
                    Tuple[Address, u64, bool_, u8]((address, amount, is_open, commission)) for address, (amount, is_open, commission) in committee_members.items()
                ])
                total_stake = u64(sum(x[0] for x in committee_members.values()))
                await self._save_committee_history(cur, height, Committee(
                    id_=Committee.compute_committee_id(starting_round, members, total_stake),
                    starting_round=starting_round,
                    members=members,
                    total_stake=total_stake,
                ))
            elif isinstance(ratification, PuzzleRewardRatify):
                if ratification.amount == 0:
                    continue
                account_mapping_id = Field.loads(cached_get_mapping_id("credits.aleo", "account"))

                if account_mapping_id not in global_mapping_cache:
                    from interpreter.finalizer import mapping_cache_read
                    global_mapping_cache[account_mapping_id] = await mapping_cache_read(cast("Database", self), "credits.aleo", "account")

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
                    supply_tracker.mint(amount)
                    supply_tracker.tally_puzzle_reward(amount)
                from interpreter.interpreter import execute_operations
                await execute_operations(cast("Database", self), cur, operations)

    @staticmethod
    async def _backup_redis_hash_key(redis_conn: Redis[str], keys: list[str], height: int):
        if height != 0:
            for key in keys:
                backup_key = f"{key}:rollback_backup:{height}"
                if await redis_conn.exists(backup_key) == 0:
                    if await redis_conn.exists(key) == 1:
                        await redis_conn.copy(key, backup_key) # type: ignore[arg-type]
                else:
                    await redis_conn.copy(backup_key, key, replace=True) # type: ignore[arg-type]

    async def _redis_cleanup(self, redis_conn: Redis[str], keys: list[str], height: int, rollback: bool):
        if height != 0:
            now = time.monotonic()
            history = False
            if self.redis_last_history_time + 43200 < now:
                self.redis_last_history_time = now
                history = True
            for key in keys:
                backup_key = f"{key}:rollback_backup:{height}"
                if rollback:
                    if await redis_conn.exists(backup_key) == 1:
                        await redis_conn.copy(backup_key, key, replace=True) # type: ignore[arg-type]
                else:
                    if history:
                        history_key = f"{key}:history:{height - 1}"
                        await redis_conn.rename(backup_key, history_key) # type: ignore[arg-type]
                        await redis_conn.expire(history_key, 60 * 60 * 24 * 3)
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
                    await self._backup_redis_hash_key(self.redis, self.redis_keys, height)
                    signal.pthread_sigmask(signal.SIG_UNBLOCK, {signal.SIGINT})

                    try:
                        if block.height != 0:
                            block_reward, coinbase_reward = block.compute_rewards(
                                await cast("Database", self).get_latest_coinbase_target(),
                                await cast("Database", self).get_latest_cumulative_proof_target()
                            )
                            puzzle_reward = coinbase_reward * 2 // 3

                            await cur.execute("SELECT total_supply FROM block ORDER BY id DESC LIMIT 1")
                            if (res := await cur.fetchone()) is None:
                                raise RuntimeError("failed to retrieve total supply")
                            supply_tracker = _SupplyTracker(res["total_supply"])
                        else:
                            block_reward, coinbase_reward, puzzle_reward = 0, 0, 0
                            supply_tracker = _SupplyTracker(0)

                        # TODO: use data from proper fee calculation
                        # supply_tracker.burn(await block.get_total_burnt_fee(cast("Database", self)))
                        for ct in block.transactions:
                            ct: ConfirmedTransaction
                            fee = ct.transaction.fee
                            if isinstance(fee, Fee):
                                supply_tracker.burn(fee.amount[0])
                            elif fee.value is not None:
                                supply_tracker.burn(fee.value.amount[0])

                        # TODO: use data from fee calculation
                        # block_reward += await block.get_total_priority_fee(cast("Database", self))

                        for ratification in block.ratifications:
                            if isinstance(ratification, BlockRewardRatify):
                                # TODO: remove this
                                block_reward = ratification.amount
                                if ratification.amount != block_reward:
                                    raise RuntimeError("invalid block reward")
                            elif isinstance(ratification, PuzzleRewardRatify):
                                if ratification.amount != puzzle_reward:
                                    raise RuntimeError("invalid puzzle reward")
                            elif isinstance(ratification, GenesisRatify):
                                await self._pre_ratify(cur, ratification, supply_tracker)

                        from interpreter.interpreter import finalize_block
                        reject_reasons = await finalize_block(cast("Database", self), cur, block)

                        await cur.execute(
                            "INSERT INTO block (height, block_hash, previous_hash, previous_state_root, transactions_root, "
                            "finalize_root, ratifications_root, solutions_root, subdag_root, round, cumulative_weight, "
                            "cumulative_proof_target, coinbase_target, proof_target, last_coinbase_target, "
                            "last_coinbase_timestamp, timestamp, block_reward, coinbase_reward, total_supply) "
                            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                            "RETURNING id",
                            (block.height, str(block.block_hash), str(block.previous_hash), str(block.header.previous_state_root),
                             str(block.header.transactions_root), str(block.header.finalize_root), str(block.header.ratifications_root),
                             str(block.header.solutions_root), str(block.header.subdag_root), block.round,
                             block.header.metadata.cumulative_weight, block.header.metadata.cumulative_proof_target,
                             block.header.metadata.coinbase_target, block.header.metadata.proof_target,
                             block.header.metadata.last_coinbase_target, block.header.metadata.last_coinbase_timestamp,
                             block.header.metadata.timestamp, block_reward, coinbase_reward, supply_tracker.supply)
                        ) # total supply will be rewritten after everything
                        if (res := await cur.fetchone()) is None:
                            raise RuntimeError("failed to insert row into database")
                        block_db_id = res["id"]

                        # dag_transmission_ids: tuple[dict[str, int], dict[str, int]] = {}, {}

                        if isinstance(block.authority, BeaconAuthority):
                            await cur.execute(
                                "INSERT INTO authority (block_id, type, signature) VALUES (%s, %s, %s)",
                                (block_db_id, block.authority.type.name, str(block.authority.signature))
                            )
                            subdag_copy_data = []
                            validators_copy_data = []
                        elif isinstance(block.authority, QuorumAuthority):
                            await cur.execute(
                                "INSERT INTO authority (block_id, type) VALUES (%s, %s) RETURNING id",
                                (block_db_id, block.authority.type.name)
                            )
                            if (res := await cur.fetchone()) is None:
                                raise RuntimeError("failed to insert row into database")
                            authority_db_id = res["id"]
                            subdag = block.authority.subdag
                            subdag_copy_data: list[tuple[int, int, str, str, int, str, int, str]] = []
                            committee = await self._get_committee_mapping_unchecked(self.redis)
                            validators: set[str] = set()
                            validators_copy_data: list[tuple[int, str]] = []
                            for round_, certificates in subdag.subdag.items():
                                for index, certificate in enumerate(certificates):
                                    if round_ != certificate.batch_header.round:
                                        raise ValueError("invalid subdag round")
                                    subdag_copy_data.append((
                                        authority_db_id, round_, str(certificate.batch_header.batch_id),
                                        str(certificate.batch_header.author), certificate.batch_header.timestamp,
                                        str(certificate.batch_header.signature), index, str(certificate.batch_header.committee_id)
                                    ))
                                    if len(validators) != len(committee):
                                        for signature in certificate.signatures:
                                            validators.add(cached_compute_key_to_address(signature.compute_key))
                                        validators.add(str(certificate.batch_header.author))

                            for validator in validators:
                                validators_copy_data.append((block_db_id, validator))
                                        # await cur.execute(
                                        #     "INSERT INTO dag_vertex (authority_id, round, batch_certificate_id, batch_id, "
                                        #     "author, timestamp, author_signature, index) "
                                        #     "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id",
                                        #     (authority_db_id, round_, str(certificate.certificate_id), str(certificate.batch_header.batch_id),
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
                        else:
                            raise NotImplementedError
                        if subdag_copy_data:
                            async with cur.copy(
                                "COPY dag_vertex (authority_id, round, batch_id, "
                                "author, timestamp, author_signature, index, committee_id) FROM STDIN"
                            ) as copy:
                                for row in subdag_copy_data:
                                    await copy.write_row(row)
                        if validators_copy_data:
                            async with cur.copy("COPY block_validator (block_id, validator) FROM STDIN") as copy:
                                for row in validators_copy_data:
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
                            confirmed_transaction: ConfirmedTransaction
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
                                finalize_operation_db_id: int = res["id"]
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
                                        "mapping_id, key_id) VALUES (%s, %s, %s)",
                                        (finalize_operation_db_id, str(finalize_operation.mapping_id),
                                         str(finalize_operation.key_id))
                                    )
                                elif isinstance(finalize_operation, ReplaceMapping):
                                    await cur.execute(
                                        "INSERT INTO finalize_operation_replace_mapping (finalize_operation_id, "
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
                            solutions: list[tuple[Solution, int, int]] = []
                            prover_solutions_target = list(zip(
                                prover_solutions,
                                [solution.target for solution in prover_solutions]
                            ))
                            target_sum = sum(target for _, target in prover_solutions_target)
                            for prover_solution, target in prover_solutions_target:
                                solutions.append((prover_solution, target, puzzle_reward * target // target_sum))

                            await cur.execute(
                                "INSERT INTO puzzle_solution (block_id, target_sum) "
                                "VALUES (%s, %s) RETURNING id",
                                (block_db_id, target_sum)
                            )
                            if (res := await cur.fetchone()) is None:
                                raise RuntimeError("failed to insert row into database")
                            puzzle_solution_db_id = res["id"]
                            copy_data: list[tuple[int, str, u64, int, int, str, str]] = []
                            for solution, target, reward in solutions:
                                solution: Solution
                                # dag_vertex_db_id = dag_transmission_ids[0][str(partial_solution.commitment)]
                                copy_data.append(
                                    (puzzle_solution_db_id, str(solution.partial_solution.address), solution.partial_solution.counter,
                                     solution.target, reward, str(solution.partial_solution.epoch_hash), str(solution.partial_solution.solution_id))
                                )
                                if reward > 0:
                                    address_puzzle_rewards[str(solution.partial_solution.address)] += reward
                            if not os.environ.get("DEBUG_SKIP_COINBASE"):
                                async with cur.copy("COPY solution (puzzle_solution_id, address, counter, target, reward, epoch_hash, solution_id) FROM STDIN") as copy:
                                    for row in copy_data:
                                        await copy.write_row(row)
                                for address, reward in address_puzzle_rewards.items():
                                    pipe = self.redis.pipeline()
                                    pipe.hincrby("address_puzzle_reward", address, reward)
                                    await pipe.execute() # type: ignore

                        for aborted in block.aborted_transactions_ids:
                            await cur.execute(
                                "INSERT INTO block_aborted_transaction_id (block_id, transaction_id) VALUES (%s, %s)",
                                (block_db_id, str(aborted))
                            )

                        for aborted in block.aborted_solution_ids:
                            await cur.execute(
                                "INSERT INTO block_aborted_solution_id (block_id, solution_id) VALUES (%s, %s)",
                                (block_db_id, str(aborted))
                            )

                        await self._post_ratify(
                            cur, self.redis, block.height, block.round, block.ratifications.ratifications,
                            address_puzzle_rewards, supply_tracker
                        )

                        if os.environ.get("DEBUG_MAPPING_DUMP", False):
                            async def read_redis_mapping(key: str) -> list[tuple[str, str]]:
                                data = await self.redis.hgetall(key)
                                r: list[tuple[str, str]] = []
                                for d in data.values():
                                    d = json.loads(d)
                                    key = str(Plaintext.load(BytesIO(bytes.fromhex(d["key"]))))
                                    value = Value.load(BytesIO(bytes.fromhex(d["value"])))
                                    if isinstance(value, PlaintextValue):
                                        plaintext = value.plaintext
                                        if isinstance(plaintext, StructPlaintext):
                                            s = ""
                                            members = plaintext.members
                                            for k, v in members:
                                                if not s:
                                                    s += f"{{\n  {str(k)}: {str(v)}"
                                                else:
                                                    s += f",\n  {str(k)}: {str(v)}"
                                            s += "\n}"
                                        else:
                                            s = str(plaintext)
                                    else:
                                        s = str(value)
                                    r.append((key, s))
                                return sorted(r, key=lambda x: x[0])

                            def write_mapping_debug(data: list[tuple[str, str]], path: str):
                                with open(path, "w") as f:
                                    for key, value in data:
                                        f.write(f"{key} -> {value}\n")

                            os.makedirs(f"/tmp/mapping_debug/{block.height}/self", exist_ok=True)
                            committee_data = await read_redis_mapping("credits.aleo:committee")
                            write_mapping_debug(committee_data, f"/tmp/mapping_debug/{block.height}/self/committee")
                            delegated_data = await read_redis_mapping("credits.aleo:delegated")
                            write_mapping_debug(delegated_data, f"/tmp/mapping_debug/{block.height}/self/delegated")
                            bonded_data = await read_redis_mapping("credits.aleo:bonded")
                            write_mapping_debug(bonded_data, f"/tmp/mapping_debug/{block.height}/self/bonded")
                            await cur.execute(
                                "SELECT key, value FROM mapping_value mv "
                                "JOIN mapping m ON mv.mapping_id = m.id "
                                "WHERE m.program_id = 'credits.aleo' AND m.mapping = 'account'"
                            )
                            account_data = await cur.fetchall()
                            values: list[tuple[str, str]] = []
                            for ad in account_data:
                                key = str(Plaintext.load(BytesIO(ad["key"])))
                                value = Value.load(BytesIO(ad["value"]))
                                if isinstance(value, PlaintextValue):
                                    plaintext = value.plaintext
                                    if isinstance(plaintext, StructPlaintext):
                                        s = ""
                                        members = plaintext.members
                                        for k, v in members:
                                            if not s:
                                                s += f"{{\n  {str(k)}: {str(v)}"
                                            else:
                                                s += f",\n  {str(k)}: {str(v)}"
                                        s += "\n}"
                                    else:
                                        s = str(plaintext)
                                else:
                                    s = str(value)
                                values.append((key, s))

                            write_mapping_debug(sorted(values, key=lambda x: x[0]), f"/tmp/mapping_debug/{block.height}/self/account")


                        await cur.execute(
                            "UPDATE block SET total_supply = %s WHERE id = %s",
                            (supply_tracker.supply, block_db_id)
                        )

                        puzzle_diff = puzzle_reward - supply_tracker.actual_puzzle_reward
                        if puzzle_diff != 0:
                            await cur.execute(
                                "INSERT INTO stats (name, value) VALUES ('puzzle_reward_diff', %s) "
                                "ON CONFLICT (name) DO UPDATE SET value = stats.value + %s",
                                (puzzle_diff, puzzle_diff)
                            )

                        block_diff = int(block_reward) - supply_tracker.actual_block_reward
                        if block_diff != 0:
                            await cur.execute(
                                "INSERT INTO stats (name, value) VALUES ('block_reward_diff', %s) "
                                "ON CONFLICT (name) DO UPDATE SET value = stats.value + %s",
                                (block_diff, block_diff)
                            )

                        if block.height % 100 == 0:
                            # temporarily disable this as it seems we don't have lingering unconfirmed tx anymore
                            pass
                            # await self.cleanup_unconfirmed_transactions()

                        signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGINT})
                        await self._redis_cleanup(self.redis, self.redis_keys, block.height, False)

                        await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseBlockAdded, block.header.metadata.height))
                    except Exception as e:
                        signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGINT})
                        await self._redis_cleanup(self.redis, self.redis_keys, block.height, True)
                        signal.pthread_sigmask(signal.SIG_UNBLOCK, {signal.SIGINT})
                        await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                        raise
            signal.pthread_sigmask(signal.SIG_UNBLOCK, {signal.SIGINT})

    async def cleanup_unconfirmed_transactions(self):
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "DELETE FROM transaction WHERE first_seen < %s AND confirmed_transaction_id IS NULL",
                    (int(time.time()) - 86400 * 7,)
                )

    async def save_block(self, block: Block):
        await self._save_block(block)

    async def save_unconfirmed_transaction(self, transaction: Transaction):
        async with self.pool.connection() as conn:
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