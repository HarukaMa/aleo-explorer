
from __future__ import annotations

import contextlib
import os
import time
from collections import defaultdict

import psycopg.sql
from psycopg.rows import DictRow

from aleo_types import *
from aleo_types.cached import cached_get_key_id, cached_get_mapping_id, cached_compute_key_to_address
from disasm.utils import value_type_to_mode_type_str, plaintext_type_to_str
from explorer.types import Message as ExplorerMessage
from util.global_cache import MappingCache
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


class BlockTimer:

    enabled = True

    def __init__(self):
        self.block_start_time = time.perf_counter_ns()
        self.block_end_time: Optional[int] = None
        self.sections: dict[str, tuple[int, Optional[int]]] = {}

    def start_block(self):
        self.block_start_time = time.perf_counter_ns()
        self.block_end_time = None
        self.sections = {}

    def start_section(self, name: str):
        self.sections[name] = (time.perf_counter_ns(), None)

    def end_section(self, name: str):
        self.sections[name] = (self.sections[name][0], time.perf_counter_ns())

    def end_block(self):
        self.block_end_time = time.perf_counter_ns()

    @contextlib.contextmanager
    def section(self, name: str):
        self.start_section(name)
        try:
            yield
        finally:
            self.end_section(name)

    def __str__(self):
        if self.block_end_time is None:
            return "Block time: not finished"
        res = f"Block time: {(self.block_end_time - self.block_start_time) / 1_000_000} ms"
        for name, (start, end) in self.sections.items():
            if end is None:
                res += f"\n  {name}: not finished"
            else:
                # in ms
                res += f"\n  {name}: {(end - start) / 1_000_000} ms"
        return res

class DummyBlockTimer:

    enabled = False

    def start_block(self):
        pass

    def start_section(self, _: str):
        pass

    def end_section(self, _: str):
        pass

    def end_block(self):
        pass

    @contextlib.contextmanager
    def section(self, _: str):
        yield

    def __str__(self):
        return "Block timing not enabled"

GlobalBlockTimer: BlockTimer | DummyBlockTimer = BlockTimer() if os.getenv("BLOCK_TIMING") else DummyBlockTimer()

class DatabaseInsert(DatabaseBase):

    @staticmethod
    async def _insert_future(cur: psycopg.AsyncCursor[DictRow], future: Future,
                             transition_output_future_db_id: Optional[int] = None, argument_db_id: Optional[int] = None):
        GlobalBlockTimer.start_section(f"          insert future {transition_output_future_db_id} {argument_db_id} {future.program_id} {future.function_name}")
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
        for index, argument in enumerate(future.arguments):
            GlobalBlockTimer.start_section(f"            insert future input {transition_output_future_db_id} {argument_db_id} {future.program_id} {future.function_name} {index}")
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
                await DatabaseInsert._insert_future(cur, argument.future, argument_db_id=argument_db_id)
            else:
                raise NotImplementedError
            GlobalBlockTimer.end_section(f"            insert future input {transition_output_future_db_id} {argument_db_id} {future.program_id} {future.function_name} {index}")
        GlobalBlockTimer.end_section(f"          insert future {transition_output_future_db_id} {argument_db_id} {future.program_id} {future.function_name}")

    async def _update_address_stats(self, cur: psycopg.AsyncCursor[DictRow], height: int, transaction: Transaction):

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
                    continue

                if transfer_from != transfer_to:
                    if transfer_from is not None:
                        await cur.execute(
                            "SELECT id, transfer_out FROM address_transfer_out_history WHERE address = %s "
                            "ORDER BY id DESC LIMIT 1",
                            (transfer_from,)
                        )
                        if (res := await cur.fetchone()) is None:
                            last_id = None
                            last_amount = 0
                        else:
                            last_id = res["id"]
                            last_amount = res["transfer_out"]
                        await cur.execute(
                            "INSERT INTO address_transfer_out_history (address, transfer_out, height, previous_id) "
                            "VALUES (%s, %s, %s, %s) RETURNING id",
                            (transfer_from, last_amount + amount, height, last_id)
                        )
                    if transfer_to is not None:
                        await cur.execute(
                            "SELECT id, transfer_in FROM address_transfer_in_history WHERE address = %s "
                            "ORDER BY id DESC LIMIT 1",
                            (transfer_to,)
                        )
                        if (res := await cur.fetchone()) is None:
                            last_id = None
                            last_amount = 0
                        else:
                            last_id = res["id"]
                            last_amount = res["transfer_in"]
                        await cur.execute(
                            "INSERT INTO address_transfer_in_history (address, transfer_in, height, previous_id) "
                            "VALUES (%s, %s, %s, %s) RETURNING id",
                            (transfer_to, last_amount + amount, height, last_id)
                        )

                if fee_from is not None:
                    await cur.execute(
                        "SELECT id, fee FROM address_fee_history WHERE address = %s "
                        "ORDER BY id DESC LIMIT 1",
                        (fee_from,)
                    )
                    if (res := await cur.fetchone()) is None:
                        last_id = None
                        last_amount = 0
                    else:
                        last_id = res["id"]
                        last_amount = res["fee"]
                    await cur.execute(
                        "INSERT INTO address_fee_history (address, fee, height, previous_id) "
                        "VALUES (%s, %s, %s, %s) RETURNING id",
                        (fee_from, last_amount + amount, height, last_id)
                    )

    @staticmethod
    async def _insert_transition(cur: psycopg.AsyncCursor[DictRow],
                                 exe_tx_db_id: Optional[int], fee_db_id: Optional[int],
                                 transition: Transition, ts_index: int, is_rejected: bool = False, should_exist: bool = False):
        GlobalBlockTimer.start_section(f"      insert transition {transition.id}")
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
            GlobalBlockTimer.start_section(f"        insert transition input {transition.id} {input_index}")
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
            GlobalBlockTimer.end_section(f"        insert transition input {transition.id} {input_index}")

        transition_output: TransitionOutput
        for output_index, transition_output in enumerate(transition.outputs):
            GlobalBlockTimer.start_section(f"        insert transition output {transition.id} {output_index}")
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
                    await DatabaseInsert._insert_future(cur, transition_output.future.value, transition_output_future_db_id)
            else:
                raise NotImplementedError
            GlobalBlockTimer.end_section(f"        insert transition output {transition.id} {output_index}")

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
        GlobalBlockTimer.end_section(f"      insert transition {transition.id}")


    @staticmethod
    async def _insert_deploy_transaction(cur: psycopg.AsyncCursor[DictRow],
                                         deployment: Deployment, owner: ProgramOwner, fee: Fee, transaction_db_id: int,
                                         is_unconfirmed: bool = False, is_rejected: bool = False, fee_should_exist: bool = False):
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

        await DatabaseInsert._insert_transition(cur, None, fee_db_id, fee.transition, 0, is_rejected, fee_should_exist)

    @staticmethod
    async def _insert_execute_transaction(cur: psycopg.AsyncCursor[DictRow],
                                          execution: Execution, fee: Optional[Fee], transaction_db_id: int,
                                          is_rejected: bool = False, ts_should_exist: bool = False):
        GlobalBlockTimer.start_section(f"    insert execute transaction {transaction_db_id}")
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
            await DatabaseInsert._insert_transition(cur, execute_transaction_db_id, None, transition, ts_index, is_rejected, ts_should_exist)

        if fee:
            await cur.execute(
                "INSERT INTO fee (transaction_id, global_state_root, proof) "
                "VALUES (%s, %s, %s) RETURNING id",
                (transaction_db_id, str(fee.global_state_root), fee.proof.dumps())
            )
            if (res := await cur.fetchone()) is None:
                raise RuntimeError("failed to insert row into database")
            fee_db_id = res["id"]
            await DatabaseInsert._insert_transition(cur, None, fee_db_id, fee.transition, 0, is_rejected, ts_should_exist)
        GlobalBlockTimer.end_section(f"    insert execute transaction {transaction_db_id}")

    async def _insert_transaction(self, cur: psycopg.AsyncCursor[DictRow], height: Optional[int], transaction: Transaction,
                                  confirmed_transaction: Optional[ConfirmedTransaction] = None, ct_index: Optional[int] = None,
                                  ignore_deploy_txids: Optional[list[str]] = None, confirmed_transaction_db_id: Optional[int] = None,
                                  reject_reasons: Optional[list[Optional[str]]] = None):
        GlobalBlockTimer.start_section(f"  insert transaction {transaction.id}")
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
                        await DatabaseInsert._insert_deploy_transaction(cur, rejected_deployment.deploy, rejected_deployment.program_owner, fee, transaction_db_id, is_rejected=True, fee_should_exist=True)

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
                        await DatabaseInsert._insert_execute_transaction(cur, rejected_execution.execution,
                                                                         cast(Fee, transaction.fee),
                                                                         transaction_db_id, is_rejected=True,
                                                                         ts_should_exist=True)

            if not prior_tx:
                original_transaction_id = None
                if confirmed_transaction is not None:
                    if isinstance(confirmed_transaction, (AcceptedDeploy, AcceptedExecute)):
                        original_transaction_id = str(confirmed_transaction.transaction.id)
                    else:
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
                    cur, transaction.deployment, transaction.owner, cast(Fee, transaction.fee), transaction_db_id,
                    is_unconfirmed=(confirmed_transaction is None)
                )

            elif isinstance(transaction, ExecuteTransaction): # accepted execute / unconfirmed
                await DatabaseInsert._insert_execute_transaction(cur, transaction.execution,
                                                                 cast(Option[Fee], transaction.fee).value,
                                                                 transaction_db_id)

            elif isinstance(transaction, FeeTransaction) and not prior_tx: # first seen rejected tx
                if isinstance(confirmed_transaction, RejectedDeploy):
                    rejected_deployment = cast(RejectedDeployment, confirmed_transaction.rejected)
                    await DatabaseInsert._insert_deploy_transaction(cur, rejected_deployment.deploy, rejected_deployment.program_owner, cast(Fee, transaction.fee), transaction_db_id, is_rejected=True)
                elif isinstance(confirmed_transaction, RejectedExecute):
                    rejected_execution = cast(RejectedExecution, confirmed_transaction.rejected)
                    await DatabaseInsert._insert_execute_transaction(cur, rejected_execution.execution,
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
            if height is None:
                raise RuntimeError("expected height to be set for confirmed transaction")
            await self._update_address_stats(cur, height, transaction)
        else:
            # check if tx is already aborted
            await cur.execute(
                "SELECT id FROM block_aborted_transaction_id WHERE transaction_id = %s",
                (str(transaction.id),)
            )
            if (await cur.fetchone()) is not None:
                await self._process_aborted_transaction(cur, transaction.id)
        GlobalBlockTimer.end_section(f"  insert transaction {transaction.id}")

    @staticmethod
    async def _process_aborted_transaction(cur: psycopg.AsyncCursor[DictRow], aborted_transaction_id: TransactionID):
        await cur.execute(
            "SELECT id FROM transaction WHERE transaction_id = %s",
            (str(aborted_transaction_id),)
        )
        if (res := await cur.fetchone()) is None:
            return
        transaction_db_id = res["id"]
        await cur.execute(
            "UPDATE transaction SET aborted = TRUE WHERE id = %s",
            (transaction_db_id,)
        )


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
        cur: psycopg.AsyncCursor[DictRow],
        committee_members: dict[Address, tuple[u64, bool_, u8]],
        stakers: dict[Address, tuple[Address, u64]],
        delegated: dict[Address, u64],
        height: int
    ):
        committee_mapping_id = Field.loads(cached_get_mapping_id("credits.aleo", "committee"))
        bonded_mapping_id = Field.loads(cached_get_mapping_id("credits.aleo", "bonded"))
        delegated_mapping_id = Field.loads(cached_get_mapping_id("credits.aleo", "delegated"))
        MappingCache()[committee_mapping_id].clear()
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
            MappingCache()[committee_mapping_id][key_id] = {
                "key": key,
                "value": value,
            }

        data: dict[str, dict[str, str]] = {}
        for k, v in MappingCache()[committee_mapping_id]:
            if v is None:
                continue
            data[cached_get_key_id("credits.aleo", "committee", v["key"].dump())] = {
                "key": v["key"].dump().hex(),
                "value": v["value"].dump().hex()
            }
        await cur.execute(
            "INSERT INTO mapping_committee_history (height, content) VALUES (%s, %s) RETURNING id",
            (height, json.dumps(data))
        )

        MappingCache()[bonded_mapping_id].clear()
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
            MappingCache()[bonded_mapping_id][key_id] = {
                "key": key,
                "value": value,
            }


        data2: dict[str, dict[str, bytes]] = {}
        for k, v in MappingCache()[bonded_mapping_id]:
            if v is None:
                continue
            data2[cached_get_key_id("credits.aleo", "bonded", v["key"].dump())] = {
                "key": v["key"].dump(),
                "value": v["value"].dump(),
            }

        await cur.executemany(
            "INSERT INTO mapping_bonded_value (key_id, key, value) VALUES (%s, %s, %s) "
            "ON CONFLICT (key_id) DO UPDATE SET value = EXCLUDED.value",
            [(k, v["key"], v["value"]) for k, v in data2.items()]
        )

        MappingCache()[delegated_mapping_id].clear()
        delegated_mapping: dict[str, dict[str, str]] = {}
        for validator, amount in delegated.items():
            key = LiteralPlaintext(literal=Literal(type_=Literal.Type.Address, primitive=validator))
            key_id = Field.loads(cached_get_key_id("credits.aleo", "delegated", key.dump()))
            value = PlaintextValue(plaintext=LiteralPlaintext(literal=Literal(type_=Literal.Type.U64, primitive=amount)))
            delegated_mapping[str(key_id)] = {
                "key": key.dump().hex(),
                "value": value.dump().hex(),
            }
            MappingCache()[delegated_mapping_id][key_id] = {
                "key": key,
                "value": value,
            }

        data = {}
        for k, v in MappingCache()[delegated_mapping_id]:
            if v is None:
                continue
            data[cached_get_key_id("credits.aleo", "delegated", v["key"].dump())] = {
                "key": v["key"].dump().hex(),
                "value": v["value"].dump().hex()
            }
        await cur.execute(
            "INSERT INTO mapping_delegated_history (height, content) VALUES (%s, %s) RETURNING id",
            (height, json.dumps(data))
        )

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
        committee = ratification.committee
        await DatabaseInsert._save_committee_history(cur, 0, committee)

        account_mapping_id = Field.loads(cached_get_mapping_id("credits.aleo", "account"))
        MappingCache()[account_mapping_id].clear()
        bonded_mapping_id = Field.loads(cached_get_mapping_id("credits.aleo", "bonded"))
        MappingCache()[bonded_mapping_id].clear()
        withdraw_mapping_id = Field.loads(cached_get_mapping_id("credits.aleo", "withdraw"))
        MappingCache()[withdraw_mapping_id].clear()
        metadata_mapping_id = Field.loads(cached_get_mapping_id("credits.aleo", "metadata"))
        MappingCache()[metadata_mapping_id].clear()

        bonded_balances = ratification.bonded_balances
        stakers: dict[Address, tuple[Address, u64]] = {}
        for staker, validator, _, amount in bonded_balances:
            stakers[staker] = validator, amount
        delegated: dict[Address, u64] = self._stakers_to_delegated(stakers)

        committee_members = {address: (amount, is_open, commission) for address, amount, is_open, commission in committee.members}
        await self._update_committee_bonded_delegated_map(cur, committee_members, stakers, delegated, 0)

        public_balances = ratification.public_balances
        operations: list[dict[str, Any]] = []
        for address, balance in public_balances:
            key = LiteralPlaintext(literal=Literal(type_=Literal.Type.Address, primitive=address))
            key_id = Field.loads(cached_get_key_id("credits.aleo", "account", key.dump()))
            value = PlaintextValue(plaintext=LiteralPlaintext(literal=Literal(type_=Literal.Type.U64, primitive=balance)))
            MappingCache()[account_mapping_id][key_id] = {
                "key": key,
                "value": value,
            }
            operations.append({
                "type": FinalizeOperation.Type.UpdateKeyValue,
                "mapping_id": account_mapping_id,
                "key_id": key_id,
                "value_id": None,
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
            MappingCache()[withdraw_mapping_id][key_id] = {
                "key": key,
                "value": value,
            }
            operations.append({
                "type": FinalizeOperation.Type.UpdateKeyValue,
                "mapping_id": withdraw_mapping_id,
                "key_id": key_id,
                "value_id": None,
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
        MappingCache()[metadata_mapping_id][key_id] = {
            "key": key,
            "value": value,
        }
        operations.append({
            "type": FinalizeOperation.Type.UpdateKeyValue,
            "mapping_id": metadata_mapping_id,
            "key_id": key_id,
            "value_id": None,
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
        MappingCache()[metadata_mapping_id][key_id] = {
            "key": key,
            "value": value,
        }
        operations.append({
            "type": FinalizeOperation.Type.UpdateKeyValue,
            "mapping_id": metadata_mapping_id,
            "key_id": key_id,
            "value_id": None,
            "key": key,
            "value": value,
            "height": 0,
            "program_name": "credits.aleo",
            "mapping_name": "metadata",
            "from_transaction": False,
        })

        from interpreter.interpreter import execute_operations
        await execute_operations(cur, operations)

    @staticmethod
    async def _get_committee_mapping_unchecked(cur: psycopg.AsyncCursor[DictRow]) -> dict[Address, tuple[bool_, u8]]:
        await cur.execute(
            "SELECT content FROM mapping_committee_history ORDER BY height DESC LIMIT 1"
        )
        if (res := await cur.fetchone()) is None:
            return {}
        data = res["content"]

        committee_members: dict[Address, tuple[bool_, u8]] = {}
        for d in data.values():
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
    async def _get_delegated_mapping_unchecked(cur: psycopg.AsyncCursor[DictRow]) -> dict[Address, u64]:
        await cur.execute(
            "SELECT content FROM mapping_delegated_history ORDER BY height DESC LIMIT 1"
        )
        if (res := await cur.fetchone()) is None:
            return {}
        data = res["content"]

        delegators: dict[Address, u64] = {}
        for d in data.values():
            key = cast(LiteralPlaintext, Plaintext.load(BytesIO(bytes.fromhex(d["key"]))))
            value = cast(PlaintextValue, Value.load(BytesIO(bytes.fromhex(d["value"]))))
            plaintext = cast(LiteralPlaintext, value.plaintext)
            delegators[cast(Address, key.literal.primitive)] = cast(u64, plaintext.literal.primitive)
        return delegators

    @staticmethod
    async def _get_bonded_mapping_unchecked(cur: psycopg.AsyncCursor[DictRow]) -> dict[Address, tuple[Address, u64]]:
        await cur.execute(
            "SELECT key, value FROM mapping_bonded_value"
        )

        stakers: dict[Address, tuple[Address, u64]] = {}
        for d in await cur.fetchall():
            key = cast(LiteralPlaintext, Plaintext.load(BytesIO(d["key"])))
            value = cast(PlaintextValue, Value.load(BytesIO(d["value"])))
            plaintext = cast(StructPlaintext, value.plaintext)
            validator = cast(LiteralPlaintext, plaintext["validator"])
            amount = cast(LiteralPlaintext, plaintext["microcredits"])
            stakers[cast(Address, key.literal.primitive)] = (
                cast(Address, validator.literal.primitive),
                cast(u64, amount.literal.primitive)
            )
        return stakers

    async def get_bonded_mapping_unchecked(self) -> dict[Address, tuple[Address, u64]]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                return await self._get_bonded_mapping_unchecked(cur)

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
    async def _post_ratify(self, cur: psycopg.AsyncCursor[dict[str, Any]], height: int, round_: int,
                           ratifications: list[Ratify], address_puzzle_rewards: dict[str, int], supply_tracker: _SupplyTracker):

        for ratification in ratifications:
            if isinstance(ratification, BlockRewardRatify):

                mapping_id = Field.loads(cached_get_mapping_id("credits.aleo", "committee"))
                if mapping_id in MappingCache():
                    committee: dict[Address, tuple[bool_, u8]] = {}
                    for _, v in MappingCache()[mapping_id]:
                        if v is None:
                            continue
                        key = cast(LiteralPlaintext, v["key"])
                        value = cast(PlaintextValue, v["value"])
                        plaintext = cast(StructPlaintext, value.plaintext)
                        is_open = cast(LiteralPlaintext, plaintext["is_open"])
                        commission = cast(LiteralPlaintext, plaintext["commission"])
                        committee[cast(Address, key.literal.primitive)] = (
                            cast(bool_, is_open.literal.primitive),
                            cast(u8, commission.literal.primitive),
                        )
                else:
                    committee = await self._get_committee_mapping_unchecked(cur)

                mapping_id = Field.loads(cached_get_mapping_id("credits.aleo", "delegated"))
                if mapping_id in MappingCache():
                    delegated: dict[Address, u64] = {}
                    for _, v in MappingCache()[mapping_id]:
                        if v is None:
                            continue
                        key = cast(LiteralPlaintext, v["key"])
                        value = cast(PlaintextValue, v["value"])
                        plaintext = cast(LiteralPlaintext, value.plaintext)
                        delegated[cast(Address, key.literal.primitive)] = cast(u64, plaintext.literal.primitive)
                else:
                    delegated = await self._get_delegated_mapping_unchecked(cur)

                mapping_id = Field.loads(cached_get_mapping_id("credits.aleo", "bonded"))
                if mapping_id in MappingCache():
                    stakers: dict[Address, tuple[Address, u64]] = {}
                    for _, v in MappingCache()[mapping_id]:
                        if v is None:
                            continue
                        key = cast(LiteralPlaintext, v["key"])
                        value = cast(PlaintextValue, v["value"])
                        address = cast(Address, key.literal.primitive)
                        bond_state = cast(StructPlaintext, value.plaintext)
                        validator = cast(Address, cast(LiteralPlaintext, bond_state["validator"]).literal.primitive)
                        amount = cast(u64, cast(LiteralPlaintext, bond_state["microcredits"]).literal.primitive)
                        stakers[address] = validator, amount
                else:
                    stakers = await self._get_bonded_mapping_unchecked(cur)

                committee_members = self._committee_delegated_to_members(committee, delegated)

                stakers, stake_rewards = self._stake_rewards(committee_members, stakers, ratification.amount)
                delegated = self._next_delegated(stakers)
                committee_members = self._next_committee_members(committee_members, stakers)

                await cur.executemany(
                    "INSERT INTO address_stake_reward (address, stake_reward) VALUES (%s, %s) "
                    "ON CONFLICT (address) DO UPDATE SET stake_reward = address_stake_reward.stake_reward + EXCLUDED.stake_reward",
                    [(str(address), amount) for address, amount in stake_rewards.items()]
                )

                total_stake_reward = sum(stake_rewards.values())
                supply_tracker.mint(total_stake_reward)
                supply_tracker.tally_block_reward(total_stake_reward)

                await self._update_committee_bonded_delegated_map(cur, committee_members, stakers, delegated, height)
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

                operations: list[dict[str, Any]] = []
                for address, amount in address_puzzle_rewards.items():
                    key = LiteralPlaintext(literal=Literal(type_=Literal.Type.Address, primitive=Address.loads(address)))
                    key_id = Field.loads(cached_get_key_id("credits.aleo", "account", key.dump()))
                    current_balance_data = await MappingCache()[account_mapping_id][key_id]
                    if current_balance_data is None:
                        current_balance = u64()
                    else:
                        value = current_balance_data["value"]
                        if not isinstance(value, PlaintextValue):
                            raise RuntimeError("invalid account value")
                        plaintext = value.plaintext
                        if not isinstance(plaintext, LiteralPlaintext) or not isinstance(plaintext.literal.primitive, u64):
                            raise RuntimeError("invalid account value")
                        current_balance = plaintext.literal.primitive
                    new_value = current_balance + u64(amount)
                    value = PlaintextValue(plaintext=LiteralPlaintext(literal=Literal(type_=Literal.Type.U64, primitive=new_value)))
                    MappingCache()[account_mapping_id][key_id] = {
                        "key": key,
                        "value": value,
                    }
                    operations.append({
                        "type": FinalizeOperation.Type.UpdateKeyValue,
                        "mapping_id": account_mapping_id,
                        "key_id": key_id,
                        "value_id": None,
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
                await execute_operations(cur, operations)

    @profile
    async def _save_block(self, block: Block):
        try:
            async with self.pool.connection() as conn:
                async with conn.cursor() as cur:
                    # noinspection SqlWithoutWhere
                    await cur.execute("UPDATE _dirty_flag SET dirty = true")

                    GlobalBlockTimer.start_block()
                    if block.height != 0:
                        from db import Database
                        last_block_timestamp = await cast(Database, self).get_latest_block_timestamp()
                        time_since_last_block = block.header.metadata.timestamp - last_block_timestamp
                        block_reward, coinbase_reward = block.compute_rewards(
                            time_since_last_block,
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
                            if ratification.amount != block_reward:
                                raise RuntimeError("invalid block reward")
                        elif isinstance(ratification, PuzzleRewardRatify):
                            if ratification.amount != puzzle_reward:
                                raise RuntimeError("invalid puzzle reward")
                        elif isinstance(ratification, GenesisRatify):
                            await self._pre_ratify(cur, ratification, supply_tracker)

                    GlobalBlockTimer.start_section("finalize")
                    from interpreter.interpreter import finalize_block
                    reject_reasons = await finalize_block(cast("Database", self), cur, block)
                    GlobalBlockTimer.end_section("finalize")

                    GlobalBlockTimer.start_section("insert block")
                    await cur.execute(
                        "INSERT INTO block (height, block_hash, previous_hash, previous_state_root, transactions_root, "
                        "finalize_root, ratifications_root, solutions_root, subdag_root, round, cumulative_weight, "
                        "cumulative_proof_target, coinbase_target, proof_target, last_coinbase_target, "
                        "last_coinbase_timestamp, timestamp, block_reward, coinbase_reward, total_supply, confirm_timestamp) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                        "RETURNING id",
                        (block.height, str(block.block_hash), str(block.previous_hash), str(block.header.previous_state_root),
                         str(block.header.transactions_root), str(block.header.finalize_root), str(block.header.ratifications_root),
                         str(block.header.solutions_root), str(block.header.subdag_root), block.round,
                         block.header.metadata.cumulative_weight, block.header.metadata.cumulative_proof_target,
                         block.header.metadata.coinbase_target, block.header.metadata.proof_target,
                         block.header.metadata.last_coinbase_target, block.header.metadata.last_coinbase_timestamp,
                         block.header.metadata.timestamp, block_reward, coinbase_reward, supply_tracker.supply, 0)
                    ) # total supply will be rewritten after everything
                    if (res := await cur.fetchone()) is None:
                        raise RuntimeError("failed to insert row into database")
                    block_db_id = res["id"]
                    GlobalBlockTimer.end_section("insert block")

                    # dag_transmission_ids: tuple[dict[str, int], dict[str, int]] = {}, {}

                    GlobalBlockTimer.start_section("authority")
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
                        # authority_db_id = res["id"]
                        subdag = block.authority.subdag
                        subdag_copy_data: list[tuple[int, int, str, str, int, str, int, str]] = []
                        committee = await self._get_committee_mapping_unchecked(cur)
                        validators: set[str] = set()
                        validators_copy_data: list[tuple[int, str]] = []
                        max_timestamp = 0
                        for round_, certificates in subdag.subdag.items():
                            for index, certificate in enumerate(certificates):
                                if certificate.batch_header.timestamp > max_timestamp:
                                    max_timestamp = certificate.batch_header.timestamp
                                if round_ != certificate.batch_header.round:
                                    raise ValueError("invalid subdag round")
                                # Wow, so now we stopped storing the subdags altogether as we are not really reusing them
                                #
                                # subdag_copy_data.append((
                                #     authority_db_id, round_, str(certificate.batch_header.batch_id),
                                #     str(certificate.batch_header.author), certificate.batch_header.timestamp,
                                #     str(certificate.batch_header.signature), index, str(certificate.batch_header.committee_id)
                                # ))
                                if len(validators) != len(committee):
                                    for signature in certificate.signatures:
                                        validators.add(cached_compute_key_to_address(signature.compute_key))
                                    validators.add(str(certificate.batch_header.author))
                        await cur.execute("UPDATE block SET confirm_timestamp = %s WHERE id = %s", (max_timestamp, block_db_id))
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
                    GlobalBlockTimer.end_section("authority")

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

                    GlobalBlockTimer.start_section("transactions")
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

                        # track supply for credit split fee
                        if isinstance(transaction, ExecuteTransaction):
                            transitions = transaction.execution.transitions
                            for transition in transitions:
                                if transition.program_id == "credits.aleo" and transition.function_name == "split":
                                    supply_tracker.burn(10000)

                        await self._insert_transaction(cur, block.height, transaction, confirmed_transaction, ct_index, ignore_deploy_txids,
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
                    GlobalBlockTimer.end_section("transactions")

                    GlobalBlockTimer.start_section("ratifications")
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
                            bonded_balances = ratify.bonded_balances
                            for address, validator, withdrawal, amount in bonded_balances:
                                await cur.execute(
                                    "INSERT INTO ratification_genesis_bonded (staker, validator, withdrawal, amount) "
                                    "VALUES (%s, %s, %s, %s)",
                                    (str(address), str(validator), str(withdrawal), amount)
                                )
                        elif isinstance(ratify, (BlockRewardRatify, PuzzleRewardRatify)):
                            await cur.execute(
                                "INSERT INTO ratification (block_id, index, type, amount) VALUES (%s, %s, %s, %s)",
                                (block_db_id, index, ratify.type.name, ratify.amount)
                            )
                        else:
                            raise NotImplementedError
                    GlobalBlockTimer.end_section("ratifications")

                    address_puzzle_rewards: dict[str, int] = defaultdict(int)

                    GlobalBlockTimer.start_section("solutions")
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
                                await cur.execute("SELECT id, puzzle_reward FROM address_puzzle_reward_history WHERE address = %s ORDER BY id DESC LIMIT 1", (address,))
                                if (res := await cur.fetchone()) is None:
                                    last_reward = 0
                                    last_id = None
                                else:
                                    last_reward = res["puzzle_reward"]
                                    last_id = res["id"]
                                await cur.execute(
                                    "INSERT INTO address_puzzle_reward_history (address, height, puzzle_reward, previous_id) "
                                    "VALUES (%s, %s, %s, %s) RETURNING id",
                                    (address, block.height, last_reward + reward, last_id)
                                )
                    GlobalBlockTimer.end_section("solutions")

                    for aborted in block.aborted_transaction_ids:
                        await cur.execute(
                            "INSERT INTO block_aborted_transaction_id (block_id, transaction_id) VALUES (%s, %s)",
                            (block_db_id, str(aborted))
                        )
                        await self._process_aborted_transaction(cur, aborted)

                    for aborted in block.aborted_solution_ids:
                        await cur.execute(
                            "INSERT INTO block_aborted_solution_id (block_id, solution_id) VALUES (%s, %s)",
                            (block_db_id, str(aborted))
                        )

                    GlobalBlockTimer.start_section("post ratify")
                    await self._post_ratify(
                        cur, block.height, block.round, block.ratifications.ratifications,
                        address_puzzle_rewards, supply_tracker
                    )
                    GlobalBlockTimer.end_section("post ratify")

                    # if os.environ.get("DEBUG_MAPPING_DUMP", False):
                    #     async def read_redis_mapping(key: str) -> list[tuple[str, str]]:
                    #         data = await self.redis.hgetall(key)
                    #         r: list[tuple[str, str]] = []
                    #         for d in data.values():
                    #             d = json.loads(d)
                    #             key = str(Plaintext.load(BytesIO(bytes.fromhex(d["key"]))))
                    #             value = Value.load(BytesIO(bytes.fromhex(d["value"])))
                    #             if isinstance(value, PlaintextValue):
                    #                 plaintext = value.plaintext
                    #                 if isinstance(plaintext, StructPlaintext):
                    #                     s = ""
                    #                     members = plaintext.members
                    #                     for k, v in members:
                    #                         if not s:
                    #                             s += f"{{\n  {str(k)}: {str(v)}"
                    #                         else:
                    #                             s += f",\n  {str(k)}: {str(v)}"
                    #                     s += "\n}"
                    #                 else:
                    #                     s = str(plaintext)
                    #             else:
                    #                 s = str(value)
                    #             r.append((key, s))
                    #         return sorted(r, key=lambda x: x[0])
                    #
                    #     def write_mapping_debug(data: list[tuple[str, str]], path: str):
                    #         with open(path, "w") as f:
                    #             for key, value in data:
                    #                 f.write(f"{key} -> {value}\n")
                    #
                    #     os.makedirs(f"/tmp/mapping_debug/{block.height}/self", exist_ok=True)
                    #     committee_data = await read_redis_mapping("credits.aleo:committee")
                    #     write_mapping_debug(committee_data, f"/tmp/mapping_debug/{block.height}/self/committee")
                    #     delegated_data = await read_redis_mapping("credits.aleo:delegated")
                    #     write_mapping_debug(delegated_data, f"/tmp/mapping_debug/{block.height}/self/delegated")
                    #     bonded_data = await read_redis_mapping("credits.aleo:bonded")
                    #     write_mapping_debug(bonded_data, f"/tmp/mapping_debug/{block.height}/self/bonded")
                    #     await cur.execute(
                    #         "SELECT key, value FROM mapping_value mv "
                    #         "JOIN mapping m ON mv.mapping_id = m.id "
                    #         "WHERE m.program_id = 'credits.aleo' AND m.mapping = 'account'"
                    #     )
                    #     account_data = await cur.fetchall()
                    #     values: list[tuple[str, str]] = []
                    #     for ad in account_data:
                    #         key = str(Plaintext.load(BytesIO(ad["key"])))
                    #         value = Value.load(BytesIO(ad["value"]))
                    #         if isinstance(value, PlaintextValue):
                    #             plaintext = value.plaintext
                    #             if isinstance(plaintext, StructPlaintext):
                    #                 s = ""
                    #                 members = plaintext.members
                    #                 for k, v in members:
                    #                     if not s:
                    #                         s += f"{{\n  {str(k)}: {str(v)}"
                    #                     else:
                    #                         s += f",\n  {str(k)}: {str(v)}"
                    #                 s += "\n}"
                    #             else:
                    #                 s = str(plaintext)
                    #         else:
                    #             s = str(value)
                    #         values.append((key, s))
                    #
                    #     write_mapping_debug(sorted(values, key=lambda x: x[0]), f"/tmp/mapping_debug/{block.height}/self/account")


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

                    GlobalBlockTimer.start_section("history")
                    if block.height % 100 == 0:
                        # temporarily disable this as it seems we don't have lingering unconfirmed tx anymore
                        # await self.cleanup_unconfirmed_transactions()
                        await self.save_history(cur, block.height)
                    GlobalBlockTimer.end_section("history")

                    await cur.execute("UPDATE _dirty_flag SET dirty = false")
                    GlobalBlockTimer.end_block()
                    if GlobalBlockTimer.enabled:
                        print(GlobalBlockTimer)

                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseBlockAdded, block.header.metadata.height))
        except Exception as e:
            await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
            raise

    @staticmethod
    async def save_history(cur: psycopg.AsyncCursor[DictRow], height: int):
        network_history_interval = {
            0: 100,
            1: 100,
            2: 1000,
        }
        from node import Network
        interval = network_history_interval[Network.network_id]
        if height % interval == 0:
            await cur.execute("SELECT * FROM address_stake_reward")
            address_stake_rewards = await cur.fetchall()
            stake_history: dict[str, int] = {}
            for address_stake_reward in address_stake_rewards:
                address = address_stake_reward["address"]
                reward = address_stake_reward["stake_reward"]
                stake_history[address] = int(reward)
            await cur.execute(
                "INSERT INTO address_stake_reward_history (height, content) VALUES (%s, %s)",
                (height, json.dumps(stake_history))
            )

            await cur.execute("SELECT * FROM mapping_bonded_value")
            bonded_values = await cur.fetchall()
            bonded_history: dict[str, dict[str, str]] = {}
            for bonded_value in bonded_values:
                bonded_history[bonded_value["key_id"]] = {
                    "key": bonded_value["key"].hex(),
                    "value": bonded_value["value"].hex(),
                }
            await cur.execute(
                "INSERT INTO mapping_bonded_history (height, content) VALUES (%s, %s)",
                (height, json.dumps(bonded_history))
            )


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
        if isinstance(transaction, FeeTransaction):
            raise RuntimeError("rejected transaction cannot be unconfirmed")
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                await self._insert_transaction(cur, None, transaction)

    async def save_feedback(self, contact: str, content: str):
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("INSERT INTO feedback (contact, content) VALUES (%s, %s)", (contact, content))
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise