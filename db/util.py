from __future__ import annotations

import signal

from aleo_explorer_rust import get_value_id

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
                await conn.execute("TRUNCATE TABLE committee_history RESTART IDENTITY CASCADE")
                await conn.execute("TRUNCATE TABLE committee_history_member RESTART IDENTITY CASCADE")
                await conn.execute("TRUNCATE TABLE mapping_bonded_history RESTART IDENTITY CASCADE")
                await conn.execute("TRUNCATE TABLE ratification_genesis_balance RESTART IDENTITY CASCADE")
                await self.redis.flushall()
            except Exception as e:
                await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                raise

    async def revert_to_last_backup(self):
        signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGINT})
        async with self.pool.connection() as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    try:
                        redis_keys = [
                            "credits.aleo:bonded",
                            "credits.aleo:committee",
                            "address_stake_reward",
                            "address_puzzle_reward",
                            "address_transfer_in",
                            "address_transfer_out",
                            "address_fee",
                        ]
                        cursor, keys = await self.redis.scan(0, f"{redis_keys[0]}:history:*", 100)
                        if cursor != 0:
                            raise RuntimeError("unsupported configuration")
                        if not keys:
                            raise RuntimeError("no backup found")
                        keys = sorted(keys, key=lambda x: int(x.split(":")[-1]))
                        last_backup = keys[-1]
                        last_backup_height = int(last_backup.split(":")[-1])
                        for redis_key in redis_keys:
                            backup_key = f"{redis_key}:history:{last_backup_height}"
                            if not await self.redis.exists(backup_key):
                                raise RuntimeError(f"backup key not found: {backup_key}")
                            await self.redis.persist(backup_key)
                        print(f"reverting to last backup: {last_backup_height}")

                        print("fetching old mapping values from mapping history")
                        await cur.execute(
                            "select distinct on (mapping_id, key_id) id, mapping_id, key_id, key, value from mapping_history "
                            "where height <= %s "
                            "order by mapping_id, key_id, id desc",
                            (last_backup_height,)
                        )
                        mapping_snapshot = await cur.fetchall()
                        print("truncating mapping values")
                        await cur.execute(
                            "TRUNCATE TABLE mapping_value RESTART IDENTITY"
                        )
                        await cur.execute(
                            "TRUNCATE TABLE mapping_history_last_id"
                        )
                        mapping_value_copy_data: list[tuple[str, str, str, bytes, bytes]] = []
                        mapping_history_last_id_copy_data: list[tuple[str, int]] = []
                        print("processing old mapping values")
                        total = len(mapping_snapshot)
                        count = 0
                        for item in mapping_snapshot:
                            id_ = item["id"]
                            mapping_id = item["mapping_id"]
                            key_id = item["key_id"]
                            key = item["key"]
                            value = item["value"]
                            if value is not None:
                                value_id = get_value_id(key_id, value)
                                mapping_value_copy_data.append((mapping_id, key_id, value_id, key, value))
                            mapping_history_last_id_copy_data.append((key_id, id_))
                            count += 1
                            if count % 10000 == 0:
                                print(f"{count}/{total}")
                        print("saving mapping values")
                        if mapping_value_copy_data:
                            async with cur.copy("COPY mapping_value (mapping_id, key_id, value_id, key, value) FROM STDIN") as copy:
                                for item in mapping_value_copy_data:
                                    await copy.write_row(item)
                            async with cur.copy("COPY mapping_history_last_id (key_id, last_history_id) FROM STDIN") as copy:
                                for item in mapping_history_last_id_copy_data:
                                    await copy.write_row(item)
                        await cur.execute(
                            "DELETE FROM mapping_history WHERE height > %s",
                            (last_backup_height,)
                        )

                        print("fetching blocks to revert")
                        blocks_to_revert = await DatabaseBlock.get_full_block_range(u32.max, last_backup_height, conn)
                        for block in blocks_to_revert:
                            print("reverting block", block.height)
                            for ct in block.transactions:
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
                                    await cur.execute(
                                        "DELETE FROM program WHERE program_id = %s",
                                        (str(program.id),)
                                    )
                                    await cur.execute(
                                        "DELETE FROM mapping WHERE program_id = %s",
                                        (str(program.id),)
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
                                    await cur.execute(
                                        "UPDATE program_function pf SET called = called - 1 "
                                        "FROM program p "
                                        "WHERE p.program_id = %s AND p.id = pf.program_id AND pf.name = %s",
                                        (str(ts.program_id), str(ts.function_name))
                                    )
                        await cur.execute(
                            "DELETE FROM block WHERE height > %s",
                            (last_backup_height,)
                        )
                        await cur.execute(
                            "DELETE FROM committee_history WHERE height > %s",
                            (last_backup_height,)
                        )

                        for redis_key in redis_keys:
                            backup_key = f"{redis_key}:history:{last_backup_height}"
                            await self.redis.copy(backup_key, redis_key, replace=True)
                            await self.redis.persist(redis_key)
                            await self.redis.expire(backup_key, 259200)

                            # remove rollback backup as well
                            _, keys = await self.redis.scan(0, f"{redis_key}:rollback_backup:*", 100)
                            for key in keys:
                                await self.redis.delete(key)

                    except Exception as e:
                        await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                        signal.pthread_sigmask(signal.SIG_UNBLOCK, {signal.SIGINT})
                        raise
        signal.pthread_sigmask(signal.SIG_UNBLOCK, {signal.SIGINT})