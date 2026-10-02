from __future__ import annotations

import signal

from aleo_types import *
from explorer.types import Message as ExplorerMessage
from .base import DatabaseBase
from .block import DatabaseBlock


class DatabaseUtil(DatabaseBase):

    @staticmethod
    def get_addresses_from_struct(plaintext: StructPlaintext):
        addresses: set[str] = set()
        for _, p in plaintext.members:
            if isinstance(p, LiteralPlaintext) and p.literal.type == Literal.Type.Address:
                addresses.add(str(p.literal.primitive))
            elif isinstance(p, StructPlaintext):
                addresses.update(DatabaseUtil.get_addresses_from_struct(p))
        return addresses

    @staticmethod
    def get_primitive_from_argument_unchecked(argument: Argument):
        plaintext = cast(PlaintextArgument, cast(PlaintextArgument, argument).plaintext)
        literal = cast(LiteralPlaintext, plaintext).literal
        return literal.primitive

    # debug method
    async def clear_database(self):
        async with self.pool.connection() as conn:
            try:
                await conn.execute("TRUNCATE TABLE block RESTART IDENTITY CASCADE")
                await conn.execute("TRUNCATE TABLE mapping RESTART IDENTITY CASCADE")
                await conn.execute("TRUNCATE TABLE mapping_history_last_id RESTART IDENTITY CASCADE")
                await conn.execute("TRUNCATE TABLE committee_history RESTART IDENTITY CASCADE")
                await conn.execute("TRUNCATE TABLE committee_history_member RESTART IDENTITY CASCADE")
                await conn.execute("TRUNCATE TABLE mapping_bonded_history RESTART IDENTITY CASCADE")
                await conn.execute("TRUNCATE TABLE mapping_bonded_value RESTART IDENTITY CASCADE")
                await conn.execute("TRUNCATE TABLE mapping_committee_history RESTART IDENTITY CASCADE")
                await conn.execute("TRUNCATE TABLE mapping_delegated_history RESTART IDENTITY CASCADE")
                await conn.execute("TRUNCATE TABLE address_fee_history RESTART IDENTITY CASCADE")
                await conn.execute("TRUNCATE TABLE address_fee_history_last_id RESTART IDENTITY CASCADE")
                await conn.execute("TRUNCATE TABLE address_puzzle_reward_history RESTART IDENTITY CASCADE")
                await conn.execute("TRUNCATE TABLE address_puzzle_reward_history_last_id RESTART IDENTITY CASCADE")
                await conn.execute("TRUNCATE TABLE address_stake_reward RESTART IDENTITY CASCADE")
                await conn.execute("TRUNCATE TABLE address_stake_reward_history RESTART IDENTITY CASCADE")
                await conn.execute("TRUNCATE TABLE address_transfer_in_history RESTART IDENTITY CASCADE")
                await conn.execute("TRUNCATE TABLE address_transfer_in_history_last_id RESTART IDENTITY CASCADE")
                await conn.execute("TRUNCATE TABLE address_transfer_out_history RESTART IDENTITY CASCADE")
                await conn.execute("TRUNCATE TABLE address_transfer_out_history_last_id RESTART IDENTITY CASCADE")
                await conn.execute("TRUNCATE TABLE ratification_genesis_balance RESTART IDENTITY CASCADE")
            except Exception as e:
                await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                raise

    async def revert_to_last_backup(self, height: Optional[int]):
        signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGINT})
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    latest_height = await (cast(DatabaseBlock, self)).get_latest_height()
                    if latest_height is None:
                        raise ValueError("database is empty")
                    if height is None:
                        height = latest_height - 1
                    if height >= latest_height:
                        raise ValueError("revert height is not less than latest height")
                    await cur.execute("select height, content from address_stake_reward_history where height <= %s order by height desc limit 1", (height,))
                    if (res := await cur.fetchone()) is None:
                        raise ValueError("no data to revert")
                    height = res["height"]
                    if height is None:
                        raise ValueError("no data to revert")
                    print(f"reverting to height {height}")

                    print("reverting address stats")
                    await cur.execute("delete from address_fee_history where height > %s", (height,))
                    await cur.execute("delete from address_puzzle_reward_history where height > %s", (height,))
                    await cur.execute("delete from address_stake_reward_history where height > %s", (height,))
                    await cur.execute("truncate table address_stake_reward restart identity")
                    await cur.execute("delete from address_transfer_in_history where height > %s", (height,))
                    await cur.execute("delete from address_transfer_out_history where height > %s", (height,))

                    for address, stake_reward in res["content"].items():
                        await cur.execute(
                            "insert into address_stake_reward (address, stake_reward) values (%s, %s)",
                            (address, stake_reward)
                        )

                    print("reverting affected mapping values")
                    # The first reverted delta points to the state at the target height.
                    await cur.execute("""
                        WITH first_reverted AS (
                            SELECT DISTINCT ON (mapping_id, key_id) mapping_id, key_id, previous_id
                            FROM mapping_history
                            WHERE height > %s
                            ORDER BY mapping_id, key_id, id
                        ), restored AS (
                            SELECT r.mapping_id, r.key_id, p.id AS history_id, p.key, p.value
                            FROM first_reverted r
                            LEFT JOIN mapping_history p ON p.id = r.previous_id
                        ), deleted_values AS (
                            DELETE FROM mapping_value mv USING restored r
                            WHERE mv.mapping_id = r.mapping_id AND mv.key_id = r.key_id
                                AND r.value IS NULL
                        ), restored_values AS (
                            INSERT INTO mapping_value (mapping_id, key_id, key, value)
                            SELECT mapping_id, key_id, key, value FROM restored
                            WHERE value IS NOT NULL
                            ON CONFLICT (mapping_id, key_id) DO UPDATE
                            SET key = EXCLUDED.key, value = EXCLUDED.value
                        ), deleted_heads AS (
                            DELETE FROM mapping_history_last_id mh USING restored r
                            WHERE mh.key_id = r.key_id AND r.history_id IS NULL
                        )
                        INSERT INTO mapping_history_last_id (key_id, last_history_id)
                        SELECT key_id, history_id FROM restored WHERE history_id IS NOT NULL
                        ON CONFLICT (key_id) DO UPDATE
                        SET last_history_id = EXCLUDED.last_history_id
                    """, (height,))
                    await cur.execute(
                        "DELETE FROM mapping_history WHERE height > %s",
                        (height,)
                    )
                    await cur.execute(
                        "DELETE FROM mapping_committee_history WHERE height > %s",
                        (height,)
                    )
                    await cur.execute(
                        "DELETE FROM mapping_delegated_history WHERE height > %s",
                        (height,)
                    )
                    await cur.execute(
                        "DELETE FROM mapping_bonded_history WHERE height > %s",
                        (height,)
                    )

                    await cur.execute("truncate table mapping_bonded_value restart identity")

                    await cur.execute(
                        "select content from mapping_bonded_history where height = %s",
                        (height,)
                    )
                    if (res := await cur.fetchone()) is not None:
                        for key_id, data in res["content"].items():
                            await cur.execute(
                                "insert into mapping_bonded_value (key_id, key, value) values (%s, %s, %s)",
                                (key_id, bytes.fromhex(data["key"]), bytes.fromhex(data["value"]))
                            )

                    # do in 1000 batch so huge rollback is still possible
                    current_height = latest_height
                    while current_height > height:
                        print("fetching blocks to revert")
                        blocks_to_revert = await DatabaseBlock.get_full_block_range(current_height, max(current_height - 1000, height), conn)
                        for block in blocks_to_revert:
                            print("reverting block", block.height)
                            for ct in sorted(block.transactions.transactions, key=lambda ct: ct.index, reverse=True):
                                t = ct.transaction
                                # revert to unconfirmed transactions
                                if isinstance(ct, (RejectedDeploy, RejectedExecute)):
                                    await cur.execute(
                                        "SELECT original_transaction_id FROM transaction WHERE transaction_id = %s",
                                        (str(t.id),)
                                    )
                                    if (res := await cur.fetchone()) is None:
                                        raise RuntimeError(f"missing transaction: {t.id}")
                                    original_transaction_id = res["original_transaction_id"]
                                    if original_transaction_id is not None:
                                        if isinstance(ct, RejectedDeploy):
                                            original_type = "Deploy"
                                        else:
                                            original_type = "Execute"
                                        await cur.execute(
                                            "UPDATE transaction SET "
                                            "transaction_id = %s, "
                                            "original_transaction_id = NULL, "
                                            "confirmed_transaction_id = NULL,"
                                            "type = %s "
                                            "WHERE transaction_id = %s",
                                            (original_transaction_id, original_type, str(t.id))
                                        )
                                else:
                                    await cur.execute(
                                        "UPDATE transaction SET confirmed_transaction_id = NULL WHERE transaction_id = %s",
                                        (str(t.id),)
                                    )
                                # decrease program called counter
                                if isinstance(t, ExecuteTransaction):
                                    transitions = list(t.execution.transitions)
                                    fee = cast(Option[Fee], t.fee)
                                    if fee.value is not None:
                                        transitions.append(fee.value.transition)
                                elif isinstance(t, DeployTransaction):
                                    fee = cast(Fee, t.fee)
                                    transitions = [fee.transition]
                                    program = t.deployment.program
                                    if not isinstance(t.deployment, DeploymentV3):
                                        # V3 amendments don't create program rows, so nothing to delete
                                        await cur.execute(
                                            "DELETE FROM program WHERE program_id = %s AND edition = %s",
                                            (str(program.id), int(t.deployment.edition))
                                        )
                                        for operation in ct.finalize:
                                            if isinstance(operation, InitializeMapping):
                                                await cur.execute(
                                                    "DELETE FROM mapping WHERE mapping_id = %s",
                                                    (str(operation.mapping_id),)
                                                )
                                elif isinstance(t, FeeTransaction):
                                    fee = cast(Fee, t.fee)
                                    if isinstance(ct, RejectedDeploy):
                                        transitions = [fee.transition]
                                    elif isinstance(ct, RejectedExecute):
                                        rejected = ct.rejected
                                        if not isinstance(rejected, RejectedExecution):
                                            raise RuntimeError("wrong transaction data")
                                        transitions = list(rejected.execution.transitions)
                                        transitions.append(fee.transition)
                                    else:
                                        raise RuntimeError("wrong transaction type")
                                else:
                                    raise NotImplementedError
                                for ts in transitions:
                                    if ts.program_id == "credits.aleo":
                                        from node import Network
                                        edition = int(block.height >= Network.consensus_v8_height)
                                    else:
                                        await cur.execute(
                                            "SELECT MAX(edition) as edition FROM program WHERE program_id = %s",
                                            (str(ts.program_id),)
                                        )
                                        if (res := await cur.fetchone()) is None:
                                            raise RuntimeError(f"missing program: {ts.program_id}")
                                        edition = res["edition"]
                                        if edition is None:
                                            raise RuntimeError(f"missing program: {ts.program_id}")
                                    await cur.execute(
                                        "UPDATE program_function pf SET called = called - 1 "
                                        "FROM program p "
                                        "WHERE p.program_id = %s AND p.id = pf.program_id AND pf.name = %s AND p.edition = %s",
                                        (str(ts.program_id), str(ts.function_name), edition)
                                    )
                        current_height -= 1000
                    await cur.execute(
                        "DELETE FROM block WHERE height > %s",
                        (height,)
                    )
                    await cur.execute(
                        "DELETE FROM committee_history WHERE height > %s",
                        (height,)
                    )
                    # noinspection SqlWithoutWhere
                    await cur.execute("UPDATE _dirty_flag SET dirty = FALSE")

                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    signal.pthread_sigmask(signal.SIG_UNBLOCK, {signal.SIGINT})
                    raise
        signal.pthread_sigmask(signal.SIG_UNBLOCK, {signal.SIGINT})

    async def check_dirty(self) -> bool:
        async with self.pool.connection() as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    await cur.execute("SELECT dirty FROM _dirty_flag")
                    if (res := await cur.fetchone()) is None:
                        await cur.execute("INSERT INTO _dirty_flag (dirty) VALUES (FALSE)")
                        return False
                    return res["dirty"]