import os
import time
from collections import defaultdict

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from disasm.utils import value_type_to_mode_type_str, plaintext_type_to_str
from explorer.types import Message as ExplorerMessage
from node.types import *


class Database:

    def __init__(self, *, server: str, user: str, password: str, database: str, schema: str,
                 message_callback: callable):
        self.server = server
        self.user = user
        self.password = password
        self.database = database
        self.schema = schema
        self.message_callback = message_callback
        self.pool: AsyncConnectionPool | None = None

    async def connect(self):
        try:
            self.pool = AsyncConnectionPool(
                f"host={self.server} user={self.user} password={self.password} dbname={self.database} "
                f"options=-csearch_path={self.schema}",
                kwargs={
                    "row_factory": dict_row,
                    "autocommit": True,
                },
                max_size=16,
            )
        except Exception as e:
            await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseConnectError, e))
            return
        await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseConnected, None))

    @staticmethod
    async def _insert_transition(conn: psycopg.AsyncConnection, exe_tx_db_id: int | None, fee_db_id: int | None,
                                 transition: Transition, ts_index: int):
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO transition (transition_id, transaction_execute_id, fee_id, program_id, "
                "function_name, tpk, tcm, index) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id",
                (str(transition.id), exe_tx_db_id, fee_db_id, str(transition.program_id),
                str(transition.function_name), str(transition.tpk), str(transition.tcm), ts_index)
            )
            transition_db_id = (await cur.fetchone())["id"]

            transition_input: TransitionInput
            for input_index, transition_input in enumerate(transition.inputs):
                await cur.execute(
                    "INSERT INTO transition_input (transition_id, type, index) VALUES (%s, %s, %s) RETURNING id",
                    (transition_db_id, transition_input.type.name, input_index)
                )
                transition_input_db_id = (await cur.fetchone())["id"]
                match transition_input.type:
                    case TransitionInput.Type.Public:
                        transition_input: PublicTransitionInput
                        await cur.execute(
                            "INSERT INTO transition_input_public (transition_input_id, plaintext_hash, plaintext) "
                            "VALUES (%s, %s, %s)",
                            (transition_input_db_id, str(transition_input.plaintext_hash),
                            transition_input.plaintext.dump_nullable())
                        )
                    case TransitionInput.Type.Private:
                        transition_input: PrivateTransitionInput
                        await cur.execute(
                            "INSERT INTO transition_input_private (transition_input_id, ciphertext_hash, ciphertext) "
                            "VALUES (%s, %s, %s)",
                            (transition_input_db_id, str(transition_input.ciphertext_hash),
                            transition_input.ciphertext.dumps())
                        )
                    case TransitionInput.Type.Record:
                        transition_input: RecordTransitionInput
                        await cur.execute(
                            "INSERT INTO transition_input_record (transition_input_id, serial_number, tag) "
                            "VALUES (%s, %s, %s)",
                            (transition_input_db_id, str(transition_input.serial_number),
                            str(transition_input.tag))
                        )
                    case TransitionInput.Type.ExternalRecord:
                        transition_input: ExternalRecordTransitionInput
                        await cur.execute(
                            "INSERT INTO transition_input_external_record (transition_input_id, commitment) "
                            "VALUES (%s, %s)",
                            (transition_input_db_id, str(transition_input.input_commitment))
                        )

                    case _:
                        raise NotImplementedError

            transition_output: TransitionOutput
            for output_index, transition_output in enumerate(transition.outputs):
                await cur.execute(
                    "INSERT INTO transition_output (transition_id, type, index) VALUES (%s, %s, %s) RETURNING id",
                    (transition_db_id, transition_output.type.name, output_index)
                )
                transition_output_db_id = (await cur.fetchone())["id"]
                match transition_output.type:
                    case TransitionOutput.Type.Public:
                        transition_output: PublicTransitionOutput
                        await cur.execute(
                            "INSERT INTO transition_output_public (transition_output_id, plaintext_hash, plaintext) "
                            "VALUES (%s, %s, %s)",
                            (transition_output_db_id, str(transition_output.plaintext_hash),
                            transition_output.plaintext.dump_nullable())
                        )
                    case TransitionOutput.Type.Private:
                        transition_output: PrivateTransitionOutput
                        await cur.execute(
                            "INSERT INTO transition_output_private (transition_output_id, ciphertext_hash, ciphertext) "
                            "VALUES (%s, %s, %s)",
                            (transition_output_db_id, str(transition_output.ciphertext_hash),
                            transition_output.ciphertext.dumps())
                        )
                    case TransitionOutput.Type.Record:
                        transition_output: RecordTransitionOutput
                        await cur.execute(
                            "INSERT INTO transition_output_record (transition_output_id, commitment, checksum, record_ciphertext) "
                            "VALUES (%s, %s, %s, %s)",
                            (transition_output_db_id, str(transition_output.commitment),
                            str(transition_output.checksum), transition_output.record_ciphertext.dumps())
                        )
                    case TransitionOutput.Type.ExternalRecord:
                        transition_output: ExternalRecordTransitionOutput
                        await cur.execute(
                            "INSERT INTO transition_output_external_record (transition_output_id, commitment) "
                            "VALUES (%s, %s)",
                            (transition_output_db_id, str(transition_output.commitment))
                        )
                    case _:
                        raise NotImplementedError

            if transition.finalize.value is not None:
                for finalize_index, finalize in enumerate(transition.finalize.value):
                    await cur.execute(
                       "INSERT INTO transition_finalize (transition_id, type, index) VALUES (%s, %s, %s) "
                       "RETURNING id",
                        (transition_db_id, finalize.type.name, finalize_index)
                    )
                    transition_finalize_db_id = (await cur.fetchone())["id"]
                    match finalize.type:
                        case Value.Type.Plaintext:
                            finalize: PlaintextValue
                            await cur.execute(
                                "INSERT INTO transition_finalize_plaintext (transition_finalize_id, plaintext) "
                                "VALUES (%s, %s)",
                                (transition_finalize_db_id, finalize.plaintext.dump())
                            )
                        case Value.Type.Record:
                            finalize: RecordValue
                            await cur.execute(
                                "INSERT INTO transition_finalize_record (transition_finalize_id, record) "
                                "VALUES (%s, %s)",
                                (transition_finalize_db_id, str(finalize.record))
                            )

            await cur.execute(
                "SELECT id FROM program WHERE program_id = %s", (str(transition.program_id),)
            )
            program_db_id = (await cur.fetchone())["id"]
            await cur.execute(
                "UPDATE program_function SET called = called + 1 WHERE program_id = %s AND name = %s",
                (program_db_id, str(transition.function_name))
            )

    async def save_builtin_program(self, program: Program):
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                await self._save_program(cur, program, None, None)

    # noinspection PyMethodMayBeStatic
    async def _save_program(self, cur, program: Program, deploy_transaction_db_id, transaction) -> None:
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
                "closure, function, raw_data, is_helloworld, feature_hash, owner, signature) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id",
                (deploy_transaction_db_id, str(program.id), imports, mappings, interfaces, records,
                 closures, functions, program.dump(), program.is_helloworld(), program.feature_hash(),
                 str(transaction.owner.address), str(transaction.owner.signature))
            )
        else:
            await cur.execute(
                "INSERT INTO program "
                "(program_id, import, mapping, interface, record, "
                "closure, function, raw_data, is_helloworld, feature_hash) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id",
                (str(program.id), imports, mappings, interfaces, records,
                 closures, functions, program.dump(), program.is_helloworld(), program.feature_hash())
            )
        program_db_id = (await cur.fetchone())["id"]
        for function in program.functions.values():
            inputs = []
            input_modes = []
            i: FunctionInput
            for i in function.inputs:
                mode, _type = value_type_to_mode_type_str(i.value_type)
                inputs.append(_type)
                input_modes.append(mode)
            outputs = []
            output_modes = []
            o: FunctionOutput
            for o in function.outputs:
                mode, _type = value_type_to_mode_type_str(o.value_type)
                outputs.append(_type)
                output_modes.append(mode)
            finalizes = []
            if function.finalize.value is not None:
                f: FinalizeInput
                for f in function.finalize.value[1].inputs:
                    finalizes.append(plaintext_type_to_str(f.plaintext_type))
            await cur.execute(
                "INSERT INTO program_function (program_id, name, input, input_mode, output, output_mode, finalize) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (program_db_id, str(function.name), inputs, input_modes, outputs, output_modes, finalizes)
            )

    async def _save_block(self, block: Block):
        async with self.pool.connection() as conn:
            conn: psycopg.AsyncConnection
            async with conn.transaction():
                async with conn.cursor() as cur:
                    try:
                        from interpreter.interpreter import finalize_block
                        await finalize_block(self, cur, block)
                        await cur.execute(
                            "INSERT INTO block (height, block_hash, previous_hash, previous_state_root, transactions_root, "
                            "coinbase_accumulator_point, round, coinbase_target, proof_target, last_coinbase_target, "
                            "last_coinbase_timestamp, timestamp, signature, total_supply, cumulative_weight, "
                            "finalize_root, cumulative_proof_target, ratifications_root) "
                            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                            "RETURNING id",
                            (block.header.metadata.height, str(block.block_hash), str(block.previous_hash),
                             str(block.header.previous_state_root), str(block.header.transactions_root),
                             str(block.header.coinbase_accumulator_point), block.header.metadata.round,
                             block.header.metadata.coinbase_target, block.header.metadata.proof_target,
                             block.header.metadata.last_coinbase_target, block.header.metadata.last_coinbase_timestamp,
                             block.header.metadata.timestamp, str(block.signature),
                             block.header.metadata.total_supply_in_microcredits,
                             block.header.metadata.cumulative_weight, str(block.header.finalize_root),
                             block.header.metadata.cumulative_proof_target, str(block.header.ratifications_root))
                        )
                        block_db_id = (await cur.fetchone())["id"]

                        confirmed_transaction: ConfirmedTransaction
                        for confirmed_transaction in block.transactions:
                            # noinspection PyUnresolvedReferences
                            await cur.execute(
                                "INSERT INTO confirmed_transaction (block_id, index, type) VALUES (%s, %s, %s) RETURNING id",
                                (block_db_id, confirmed_transaction.index, confirmed_transaction.type.name)
                            )
                            confirmed_transaction_db_id = (await cur.fetchone())["id"]
                            match confirmed_transaction.type:
                                case ConfirmedTransaction.Type.AcceptedDeploy:
                                    confirmed_transaction: AcceptedDeploy
                                    transaction: Transaction = confirmed_transaction.transaction
                                    if transaction.type != Transaction.Type.Deploy:
                                        raise ValueError("expected deploy transaction")
                                    transaction: DeployTransaction
                                    transaction_id = transaction.id
                                    await cur.execute(
                                        "INSERT INTO transaction (confimed_transaction_id, transaction_id, type) VALUES (%s, %s, %s) RETURNING id",
                                        (confirmed_transaction_db_id, str(transaction_id), transaction.type.name)
                                    )
                                    transaction_db_id = (await cur.fetchone())["id"]
                                    await cur.execute(
                                        "INSERT INTO transaction_deploy (transaction_id, edition, verifying_keys) "
                                        "VALUES (%s, %s, %s) RETURNING id",
                                        (transaction_db_id, transaction.deployment.edition, transaction.deployment.verifying_keys.dump())
                                    )
                                    deploy_transaction_db_id = (await cur.fetchone())["id"]

                                    await self._save_program(cur, transaction.deployment.program, deploy_transaction_db_id, transaction)

                                    await cur.execute(
                                        "INSERT INTO fee (transaction_id, global_state_root, proof) "
                                        "VALUES (%s, %s, %s) RETURNING id",
                                        (transaction_db_id, str(transaction.fee.global_state_root), transaction.fee.proof.dumps())
                                    )
                                    fee_db_id = (await cur.fetchone())["id"]
                                    await self._insert_transition(conn, None, fee_db_id, transaction.fee.transition, 0)

                                case ConfirmedTransaction.Type.AcceptedExecute:
                                    confirmed_transaction: AcceptedExecute
                                    transaction: Transaction = confirmed_transaction.transaction
                                    if transaction.type != Transaction.Type.Execute:
                                        raise ValueError("expected execute transaction")
                                    transaction: ExecuteTransaction
                                    transaction_id = transaction.id
                                    await cur.execute(
                                        "INSERT INTO transaction (confimed_transaction_id, transaction_id, type) VALUES (%s, %s, %s) RETURNING id",
                                        (confirmed_transaction_db_id, str(transaction_id), transaction.type.name)
                                    )
                                    transaction_db_id = (await cur.fetchone())["id"]
                                    await cur.execute(
                                        "INSERT INTO transaction_execute (transaction_id, global_state_root, proof) "
                                        "VALUES (%s, %s, %s) RETURNING id",
                                        (transaction_db_id, str(transaction.execution.global_state_root),
                                         transaction.execution.proof.dumps())
                                    )
                                    execute_transaction_db_id = (await cur.fetchone())["id"]

                                    transition: Transition
                                    for ts_index, transition in enumerate(transaction.execution.transitions):
                                        await self._insert_transition(conn, execute_transaction_db_id, None, transition, ts_index)

                                    if transaction.additional_fee.value is not None:
                                        fee: Fee = transaction.additional_fee.value
                                        await cur.execute(
                                            "INSERT INTO fee (transaction_id, global_state_root, proof) "
                                            "VALUES (%s, %s, %s) RETURNING id",
                                            (transaction_db_id, str(fee.global_state_root), fee.proof.dumps())
                                        )
                                        fee_db_id = (await cur.fetchone())["id"]
                                        await self._insert_transition(conn, None, fee_db_id, fee.transition, 0)

                                case ConfirmedTransaction.Type.RejectedDeploy:
                                    raise ValueError("transaction type not implemented")

                                case ConfirmedTransaction.Type.RejectedExecute:
                                    confirmed_transaction: RejectedExecute
                                    transaction: Transaction = confirmed_transaction.transaction
                                    if transaction.type != Transaction.Type.Fee:
                                        raise ValueError("expected fee transaction")
                                    transaction: FeeTransaction
                                    transaction_id = transaction.id
                                    await cur.execute(
                                        "INSERT INTO transaction (confimed_transaction_id, transaction_id, type) VALUES (%s, %s, %s) RETURNING id",
                                        (confirmed_transaction_db_id, str(transaction_id), transaction.type.name)
                                    )
                                    transaction_db_id = (await cur.fetchone())["id"]
                                    fee = transaction.fee
                                    await cur.execute(
                                        "INSERT INTO fee (transaction_id, global_state_root, proof) "
                                        "VALUES (%s, %s, %s) RETURNING id",
                                        (transaction_db_id, str(fee.global_state_root), fee.proof.dumps())
                                    )
                                    fee_db_id = (await cur.fetchone())["id"]
                                    await self._insert_transition(conn, None, fee_db_id, fee.transition, 0)

                                    rejected: Rejected = confirmed_transaction.rejected
                                    rejected: RejectedExecution
                                    await cur.execute(
                                        "INSERT INTO transaction_execute (transaction_id, global_state_root, proof) "
                                        "VALUES (%s, %s, %s) RETURNING id",
                                        (transaction_db_id, str(rejected.execution.global_state_root),
                                         rejected.execution.proof.dumps())
                                    )
                                    execute_transaction_db_id = (await cur.fetchone())["id"]
                                    for ts_index, transition in enumerate(rejected.execution.transitions):
                                        await self._insert_transition(conn, execute_transaction_db_id, None, transition, ts_index)

                            if confirmed_transaction.type in [ConfirmedTransaction.Type.AcceptedDeploy, ConfirmedTransaction.Type.AcceptedExecute]:
                                for finalize_operation in confirmed_transaction.finalize:
                                    finalize_operation: FinalizeOperation
                                    await cur.execute(
                                        "INSERT INTO finalize_operation (confirmed_transaction_id, type) "
                                        "VALUES (%s, %s) RETURNING id",
                                        (confirmed_transaction_db_id, finalize_operation.type.name)
                                    )
                                    finalize_operation_db_id = (await cur.fetchone())["id"]
                                    match finalize_operation.type:
                                        case FinalizeOperation.Type.InitializeMapping:
                                            finalize_operation: InitializeMapping
                                            await cur.execute(
                                                "INSERT INTO finalize_operation_initialize_mapping (finalize_operation_id, "
                                                "mapping_id) VALUES (%s, %s)",
                                                (finalize_operation_db_id, str(finalize_operation.mapping_id))
                                            )
                                        case FinalizeOperation.Type.InsertKeyValue:
                                            finalize_operation: InsertKeyValue
                                            await cur.execute(
                                                "INSERT INTO finalize_operation_insert_kv (finalize_operation_id, "
                                                "mapping_id, key_id, value_id) VALUES (%s, %s, %s, %s)",
                                                (finalize_operation_db_id, str(finalize_operation.mapping_id),
                                                str(finalize_operation.key_id), str(finalize_operation.value_id))
                                            )
                                        case FinalizeOperation.Type.UpdateKeyValue:
                                            finalize_operation: UpdateKeyValue
                                            await cur.execute(
                                                "INSERT INTO finalize_operation_update_kv (finalize_operation_id, "
                                                "mapping_id, index, key_id, value_id) VALUES (%s, %s, %s, %s, %s)",
                                                (finalize_operation_db_id, str(finalize_operation.mapping_id),
                                                finalize_operation.index, str(finalize_operation.key_id),
                                                str(finalize_operation.value_id))
                                            )
                                        case FinalizeOperation.Type.RemoveKeyValue:
                                            finalize_operation: RemoveKeyValue
                                            await cur.execute(
                                                "INSERT INTO finalize_operation_remove_kv (finalize_operation_id, "
                                                "mapping_id, index) VALUES (%s, %s, %s)",
                                                (finalize_operation_db_id, str(finalize_operation.mapping_id),
                                                finalize_operation.index)
                                            )
                                        case FinalizeOperation.Type.RemoveMapping:
                                            finalize_operation: RemoveMapping
                                            await cur.execute(
                                                "INSERT INTO finalize_operation_remove_mapping (finalize_operation_id, "
                                                "mapping_id) VALUES (%s, %s)",
                                                (finalize_operation_db_id, str(finalize_operation.mapping_id))
                                            )

                        ratification_map = defaultdict(lambda: defaultdict(list))

                        copy_data = []
                        for index, ratify in enumerate(block.ratifications):
                            ratify: Ratify
                            # noinspection PyUnresolvedReferences
                            copy_data.append((block_db_id, ratify.type.name, str(ratify.address), ratify.amount, index))
                        if copy_data:
                            await cur.executemany(
                                "INSERT INTO ratification (block_id, type, address, amount, index) "
                                "VALUES (%s, %s, %s, %s, %s) RETURNING id",
                                copy_data,
                                returning=True,
                            )
                            ratify_db_id = []
                            while True:
                                ratify_db_id.append((await cur.fetchone())["id"])
                                if not cur.nextset():
                                    break
                            for index, data in enumerate(copy_data):
                                #                addr     amount
                                ratification_map[data[2]][data[3]].append(ratify_db_id[index])

                        if block.coinbase.value is not None and not os.environ.get("DEBUG_SKIP_COINBASE"):
                            coinbase_reward = block.get_coinbase_reward((await self.get_latest_block()).header.metadata.last_coinbase_timestamp)
                            await cur.execute(
                                "UPDATE block SET coinbase_reward = %s WHERE id = %s",
                                (coinbase_reward, block_db_id)
                            )
                            partial_solutions = list(block.coinbase.value.partial_solutions)
                            solutions = []
                            partial_solutions = list(zip(partial_solutions,
                                                    [partial_solution.commitment.to_target() for partial_solution in
                                                     partial_solutions]))
                            target_sum = sum(target for _, target in partial_solutions)
                            partial_solution: PartialSolution
                            for partial_solution, target in partial_solutions:
                                solutions.append((partial_solution, target, coinbase_reward * target // (2 * target_sum)))

                            await cur.execute(
                                "INSERT INTO coinbase_solution (block_id, proof_x, proof_y_positive, target_sum) "
                                "VALUES (%s, %s, %s, %s) RETURNING id",
                                (block_db_id, str(block.coinbase.value.proof.w.x), block.coinbase.value.proof.w.flags, target_sum)
                            )
                            coinbase_solution_db_id = (await cur.fetchone())["id"]
                            await cur.execute("SELECT total_credit FROM leaderboard_total")
                            current_total_credit = await cur.fetchone()
                            if current_total_credit is None:
                                await cur.execute("INSERT INTO leaderboard_total (total_credit) VALUES (0)")
                                current_total_credit = 0
                            else:
                                current_total_credit = current_total_credit["total_credit"]
                            partial_solution: PartialSolution
                            copy_data = []
                            for partial_solution, target, reward in solutions:
                                try:
                                    ratify_id = ratification_map[str(partial_solution.address)][reward].pop()
                                except:
                                    raise RuntimeError(f"could not find ratification for address {partial_solution.address} and reward {reward}")
                                copy_data.append(
                                    (coinbase_solution_db_id, str(partial_solution.address), partial_solution.nonce,
                                     str(partial_solution.commitment), partial_solution.commitment.to_target(), reward, ratify_id)
                                )
                                if reward > 0:
                                    await cur.execute(
                                        "INSERT INTO leaderboard (address, total_reward) VALUES (%s, %s) "
                                        "ON CONFLICT (address) DO UPDATE SET total_reward = leaderboard.total_reward + %s",
                                        (str(partial_solution.address), reward, reward)
                                    )
                                    if block.header.metadata.height >= 130888 and block.header.metadata.timestamp < 1675209600 and current_total_credit < 37_500_000_000_000:
                                        await cur.execute(
                                            "UPDATE leaderboard SET total_incentive = leaderboard.total_incentive + %s WHERE address = %s",
                                            (reward, str(partial_solution.address))
                                        )
                            async with cur.copy("COPY partial_solution (coinbase_solution_id, address, nonce, commitment, target, reward, ratification_id) FROM STDIN") as copy:
                                for row in copy_data:
                                    await copy.write_row(row)
                            if block.header.metadata.height >= 130888 and block.header.metadata.timestamp < 1675209600 and current_total_credit < 37_500_000_000_000:
                                await cur.execute(
                                    "UPDATE leaderboard_total SET total_credit = leaderboard_total.total_credit + %s",
                                    (sum(reward for _, _, reward in solutions),)
                                )

                        await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseBlockAdded, block.header.metadata.height))
                    except Exception as e:
                        await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                        raise

    async def save_block(self, block: Block):
        await self._save_block(block)

    @staticmethod
    def _get_block_header(block: dict):
        return BlockHeader(
            previous_state_root=Field.loads(block["previous_state_root"]),
            transactions_root=Field.loads(block["transactions_root"]),
            finalize_root=Field.loads(block["finalize_root"]),
            ratifications_root=Field.loads(block["ratifications_root"]),
            coinbase_accumulator_point=Field.loads(block["coinbase_accumulator_point"]),
            metadata=BlockHeaderMetadata(
                network=u16(3),
                round_=u64(block["round"]),
                height=u32(block["height"]),
                total_supply_in_microcredits=u64(block["total_supply"]),
                cumulative_weight=u128(block["cumulative_weight"]),
                cumulative_proof_target=u128(block["cumulative_proof_target"]),
                coinbase_target=u64(block["coinbase_target"]),
                proof_target=u64(block["proof_target"]),
                last_coinbase_target=u64(block["last_coinbase_target"]),
                last_coinbase_timestamp=i64(block["last_coinbase_timestamp"]),
                timestamp=i64(block["timestamp"]),
            )
        )

    @staticmethod
    async def _get_transition(transition: dict, conn: psycopg.AsyncConnection):
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT * FROM transition_input WHERE transition_id = %s",
                (transition["id"],)
            )
            transition_inputs = await cur.fetchall()
            tis = []
            for transition_input in transition_inputs:
                match transition_input["type"]:
                    case TransitionInput.Type.Public.name:
                        await cur.execute(
                            "SELECT * FROM transition_input_public WHERE transition_input_id = %s",
                            (transition_input["id"],)
                        )
                        transition_input_public = await cur.fetchone()
                        if transition_input_public["plaintext"] is None:
                            plaintext = None
                        else:
                            plaintext = Plaintext.load(bytearray(transition_input_public["plaintext"]))
                        tis.append((PublicTransitionInput(
                            plaintext_hash=Field.loads(transition_input_public["plaintext_hash"]),
                            plaintext=Option[Plaintext](plaintext)
                        ), transition_input["index"]))

                    case TransitionInput.Type.Private.name:
                        await cur.execute(
                            "SELECT * FROM transition_input_private WHERE transition_input_id = %s",
                            (transition_input["id"],)
                        )
                        transition_input_private = await cur.fetchone()
                        if transition_input_private["ciphertext"] is None:
                            ciphertext = None
                        else:
                            ciphertext = Ciphertext.loads(transition_input_private["ciphertext"])
                        tis.append((PrivateTransitionInput(
                            ciphertext_hash=Field.loads(transition_input_private["ciphertext_hash"]),
                            ciphertext=Option[Ciphertext](ciphertext)
                        ), transition_input["index"]))

                    case TransitionInput.Type.Record.name:
                        await cur.execute(
                            "SELECT * FROM transition_input_record WHERE transition_input_id = %s",
                            (transition_input["id"],)
                        )
                        transition_input_record = await cur.fetchone()
                        tis.append((RecordTransitionInput(
                            serial_number=Field.loads(transition_input_record["serial_number"]),
                            tag=Field.loads(transition_input_record["tag"])
                        ), transition_input["index"]))

                    case TransitionInput.Type.ExternalRecord.name:
                        await cur.execute(
                            "SELECT * FROM transition_input_external_record WHERE transition_input_id = %s",
                            (transition_input["id"],)
                        )
                        transition_input_external_record = await cur.fetchone()
                        tis.append((ExternalRecordTransitionInput(
                            input_commitment=Field.loads(transition_input_external_record["commitment"]),
                        ), transition_input["index"]))

                    case _:
                        raise NotImplementedError
            tis.sort(key=lambda x: x[1])
            tis = [x[0] for x in tis]

            await cur.execute(
                "SELECT * FROM transition_output WHERE transition_id = %s",
                (transition["id"],)
            )
            transition_outputs = await cur.fetchall()
            tos = []
            for transition_output in transition_outputs:
                match transition_output["type"]:
                    case TransitionOutput.Type.Public.name:
                        await cur.execute(
                            "SELECT * FROM transition_output_public WHERE transition_output_id = %s",
                            (transition_output["id"],)
                        )
                        transition_output_public = await cur.fetchone()
                        if transition_output_public["plaintext"] is None:
                            plaintext = None
                        else:
                            plaintext = Plaintext.load(bytearray(transition_output_public["plaintext"]))
                        tos.append((PublicTransitionOutput(
                            plaintext_hash=Field.loads(transition_output_public["plaintext_hash"]),
                            plaintext=Option[Plaintext](plaintext)
                        ), transition_output["index"]))
                    case TransitionOutput.Type.Private.name:
                        await cur.execute(
                            "SELECT * FROM transition_output_private WHERE transition_output_id = %s",
                            (transition_output["id"],)
                        )
                        transition_output_private = await cur.fetchone()
                        if transition_output_private["ciphertext"] is None:
                            ciphertext = None
                        else:
                            ciphertext = Ciphertext.loads(transition_output_private["ciphertext"])
                        tos.append((PrivateTransitionOutput(
                            ciphertext_hash=Field.loads(transition_output_private["ciphertext_hash"]),
                            ciphertext=Option[Ciphertext](ciphertext)
                        ), transition_output["index"]))
                    case TransitionOutput.Type.Record.name:
                        await cur.execute(
                            "SELECT * FROM transition_output_record WHERE transition_output_id = %s",
                            (transition_output["id"],)
                        )
                        transition_output_record = await cur.fetchone()
                        if transition_output_record["record_ciphertext"] is None:
                            record_ciphertext = None
                        else:
                            # noinspection PyArgumentList
                            record_ciphertext = Record[Ciphertext].loads(transition_output_record["record_ciphertext"])
                        tos.append((RecordTransitionOutput(
                            commitment=Field.loads(transition_output_record["commitment"]),
                            checksum=Field.loads(transition_output_record["checksum"]),
                            record_ciphertext=Option[Record[Ciphertext]](record_ciphertext)
                        ), transition_output["index"]))
                    case TransitionOutput.Type.ExternalRecord.name:
                        await cur.execute(
                            "SELECT * FROM transition_output_external_record WHERE transition_output_id = %s",
                            (transition_output["id"],)
                        )
                        transition_output_external_record = await cur.fetchone()
                        tos.append((ExternalRecordTransitionOutput(
                            commitment=Field.loads(transition_output_external_record["commitment"]),
                        ), transition_output["index"]))
                    case _:
                        raise NotImplementedError
            tos.sort(key=lambda x: x[1])
            tos = [x[0] for x in tos]

            await cur.execute(
                "SELECT * FROM transition_finalize WHERE transition_id = %s",
                (transition["id"],)
            )
            transition_finalizes = await cur.fetchall()
            if len(transition_finalizes) == 0:
                finalize = None
            else:
                finalize = []
                for transition_finalize in transition_finalizes:
                    match transition_finalize["type"]:
                        case Value.Type.Plaintext.name:
                            await cur.execute(
                                "SELECT * FROM transition_finalize_plaintext WHERE transition_finalize_id = %s",
                                (transition_finalize["id"],)
                            )
                            transition_finalize_plaintext = await cur.fetchone()
                            finalize.append((PlaintextValue(
                                plaintext=Plaintext.load(bytearray(transition_finalize_plaintext["plaintext"]))
                            ), transition_finalize["index"]))
                        case Value.Type.Record.name:
                            await cur.execute(
                                "SELECT * FROM transition_finalize_record WHERE transition_finalize_id = %s",
                                (transition_finalize["id"],)
                            )
                            transition_finalize_record = await cur.fetchone()
                            # noinspection PyArgumentList
                            finalize.append((RecordValue(
                                record=Record[Plaintext].loads(transition_finalize_record["record"])
                            ), transition_finalize["index"]))
                finalize.sort(key=lambda x: x[1])
                finalize = Vec[Value, u8]([x[0] for x in finalize])

            return Transition(
                id_=TransitionID.loads(transition["transition_id"]),
                program_id=ProgramID.loads(transition["program_id"]),
                function_name=Identifier.loads(transition["function_name"]),
                inputs=Vec[TransitionInput, u8](tis),
                outputs=Vec[TransitionOutput, u8](tos),
                finalize=Option[Vec[Value, u8]](finalize),
                tpk=Group.loads(transition["tpk"]),
                tcm=Field.loads(transition["tcm"]),
            )

    @staticmethod
    async def _get_full_block(block: dict, conn: psycopg.AsyncConnection):
        async with conn.cursor() as cur:
            await cur.execute("SELECT * FROM confirmed_transaction WHERE block_id = %s", (block['id'],))
            confirmed_transactions = await cur.fetchall()
            ctxs = []
            for confirmed_transaction in confirmed_transactions:
                await cur.execute("SELECT * FROM finalize_operation WHERE confirmed_transaction_id = %s", (confirmed_transaction["id"],))
                finalize_operations = await cur.fetchall()
                f = []
                for finalize_operation in finalize_operations:
                    match finalize_operation["type"]:
                        case FinalizeOperation.Type.InitializeMapping.name:
                            await cur.execute(
                                "SELECT * FROM finalize_operation_initialize_mapping WHERE finalize_operation_id = %s",
                                (finalize_operation["id"],)
                            )
                            initialize_mapping = await cur.fetchone()
                            f.append(InitializeMapping(mapping_id=Field.loads(initialize_mapping["mapping_id"])))
                        case FinalizeOperation.Type.InsertKeyValue.name:
                            await cur.execute(
                                "SELECT * FROM finalize_operation_insert_kv WHERE finalize_operation_id = %s",
                                (finalize_operation["id"],)
                            )
                            insert_kv = await cur.fetchone()
                            f.append(InsertKeyValue(
                                mapping_id=Field.loads(insert_kv["mapping_id"]),
                                key_id=Field.loads(insert_kv["key_id"]),
                                value_id=Field.loads(insert_kv["value_id"]),
                            ))
                        case FinalizeOperation.Type.UpdateKeyValue.name:
                            await cur.execute(
                                "SELECT * FROM finalize_operation_update_kv WHERE finalize_operation_id = %s",
                                (finalize_operation["id"],)
                            )
                            update_kv = await cur.fetchone()
                            f.append(UpdateKeyValue(
                                mapping_id=Field.loads(update_kv["mapping_id"]),
                                index=u64(update_kv["index"]),
                                key_id=Field.loads(update_kv["key_id"]),
                                value_id=Field.loads(update_kv["value_id"]),
                            ))
                        case FinalizeOperation.Type.RemoveKeyValue.name:
                            await cur.execute(
                                "SELECT * FROM finalize_operation_remove_kv WHERE finalize_operation_id = %s",
                                (finalize_operation["id"],)
                            )
                            remove_kv = await cur.fetchone()
                            f.append(RemoveKeyValue(
                                mapping_id=Field.loads(remove_kv["mapping_id"]),
                                index=u64(remove_kv["index"]),
                            ))
                        case FinalizeOperation.Type.RemoveMapping.name:
                            await cur.execute(
                                "SELECT * FROM finalize_operation_remove_mapping WHERE finalize_operation_id = %s",
                                (finalize_operation["id"],)
                            )
                            remove_mapping = await cur.fetchone()
                            f.append(RemoveMapping(mapping_id=Field.loads(remove_mapping["mapping_id"])))

                await cur.execute("SELECT * FROM transaction WHERE confimed_transaction_id = %s", (confirmed_transaction["id"],))
                transaction = await cur.fetchone()
                match confirmed_transaction["type"]:
                    case ConfirmedTransaction.Type.AcceptedDeploy.name | ConfirmedTransaction.Type.RejectedDeploy.name:
                        if confirmed_transaction["type"] == ConfirmedTransaction.Type.RejectedDeploy.name:
                            raise NotImplementedError
                        await cur.execute(
                            "SELECT * FROM transaction_deploy WHERE transaction_id = %s",
                            (transaction["id"],)
                        )
                        deploy_transaction = await cur.fetchone()
                        await cur.execute(
                            "SELECT raw_data, owner, signature FROM program WHERE transaction_deploy_id = %s",
                            (deploy_transaction["id"],)
                        )
                        program_data = await cur.fetchone()
                        program = program_data["raw_data"]
                        # noinspection PyArgumentList
                        deployment = Deployment(
                            edition=u16(deploy_transaction["edition"]),
                            program=Program.load(bytearray(program)),
                            verifying_keys=Vec[Tuple[Identifier, VerifyingKey, Certificate], u16].load(bytearray(deploy_transaction["verifying_keys"])),
                        )
                        await cur.execute(
                            "SELECT * FROM fee WHERE transaction_id = %s",
                            (transaction["id"],)
                        )
                        fee = await cur.fetchone()
                        await cur.execute(
                            "SELECT * FROM transition WHERE fee_id = %s",
                            (fee["id"],)
                        )
                        fee_transition = await cur.fetchone()
                        if fee_transition is None:
                            raise ValueError("fee transition not found")
                        proof = None
                        if fee["proof"] is not None:
                            proof = Proof.loads(fee["proof"])
                        fee = Fee(
                            transition=await Database._get_transition(fee_transition, conn),
                            global_state_root=StateRoot.loads(fee["global_state_root"]),
                            proof=Option[Proof](proof),
                        )
                        tx = DeployTransaction(
                            id_=TransactionID.loads(transaction["transaction_id"]),
                            deployment=deployment,
                            fee=fee,
                            owner=ProgramOwner(
                                address=Address.loads(program_data["owner"]),
                                signature=Signature.loads(program_data["signature"])
                            )
                        )
                        ctxs.append(AcceptedDeploy(
                            index=u32(confirmed_transaction["index"]),
                            transaction=tx,
                            finalize=Vec[FinalizeOperation, u16](f),
                        ))
                    case ConfirmedTransaction.Type.AcceptedExecute.name | ConfirmedTransaction.Type.RejectedExecute.name:
                        await cur.execute(
                            "SELECT * FROM transaction_execute WHERE transaction_id = %s",
                            (transaction["id"],)
                        )
                        execute_transaction = await cur.fetchone()
                        await cur.execute(
                            "SELECT * FROM transition WHERE transaction_execute_id = %s",
                            (execute_transaction["id"],)
                        )
                        transitions = await cur.fetchall()
                        tss = []
                        for transition in transitions:
                            tss.append(await Database._get_transition(transition, conn))
                        await cur.execute(
                            "SELECT * FROM fee WHERE transaction_id = %s",
                            (transaction["id"],)
                        )
                        additional_fee = await cur.fetchone()
                        if additional_fee is None:
                            fee = None
                        else:
                            await cur.execute(
                                "SELECT * FROM transition WHERE fee_id = %s",
                                (additional_fee["id"],)
                            )
                            fee_transition = await cur.fetchone()
                            if fee_transition is None:
                                raise ValueError("fee transition not found")
                            proof = None
                            if additional_fee["proof"] is not None:
                                proof = Proof.loads(additional_fee["proof"])
                            fee = Fee(
                                transition=await Database._get_transition(fee_transition, conn),
                                global_state_root=StateRoot.loads(additional_fee["global_state_root"]),
                                proof=Option[Proof](proof),
                            )
                        if execute_transaction["proof"] is None:
                            proof = None
                        else:
                            proof = Proof.loads(execute_transaction["proof"])
                        if confirmed_transaction["type"] == ConfirmedTransaction.Type.AcceptedExecute.name:
                            ctxs.append(AcceptedExecute(
                                index=u32(confirmed_transaction["index"]),
                                transaction=ExecuteTransaction(
                                    id_=TransactionID.loads(transaction["transaction_id"]),
                                    execution=Execution(
                                        transitions=Vec[Transition, u8](tss),
                                        global_state_root=StateRoot.loads(execute_transaction["global_state_root"]),
                                        proof=Option[Proof](proof),
                                    ),
                                    additional_fee=Option[Fee](fee),
                                ),
                                finalize=Vec[FinalizeOperation, u16](f),
                            ))
                        else:
                            ctxs.append(RejectedExecute(
                                index=u32(confirmed_transaction["index"]),
                                transaction=FeeTransaction(
                                    id_=TransactionID.loads(transaction["transaction_id"]),
                                    fee=fee,
                                ),
                                rejected=RejectedExecution(
                                    execution=Execution(
                                        transitions=Vec[Transition, u8](tss),
                                        global_state_root=StateRoot.loads(execute_transaction["global_state_root"]),
                                        proof=Option[Proof](proof),
                                    )
                                )
                            ))
                    case _:
                        raise NotImplementedError

            await cur.execute("SELECT * FROM ratification WHERE block_id = %s ORDER BY index", (block["id"],))
            ratifications = await cur.fetchall()
            rs = []
            for ratification in ratifications:
                match ratification["type"]:
                    case Ratify.Type.ProvingReward.name:
                        rs.append(ProvingReward(
                            address=Address.loads(ratification["address"]),
                            amount=u64(ratification["amount"]),
                        ))
                    case Ratify.Type.StakingReward.name:
                        rs.append(StakingReward(
                            address=Address.loads(ratification["address"]),
                            amount=u64(ratification["amount"]),
                        ))

            await cur.execute("SELECT * FROM coinbase_solution WHERE block_id = %s", (block["id"],))
            coinbase_solution = await cur.fetchone()
            if coinbase_solution is not None:
                await cur.execute(
                    "SELECT * FROM partial_solution WHERE coinbase_solution_id = %s",
                    (coinbase_solution["id"],)
                )
                partial_solutions = await cur.fetchall()
                pss = []
                for partial_solution in partial_solutions:
                    pss.append(PartialSolution.load_json(dict(partial_solution)))
                coinbase_solution = CoinbaseSolution(
                    partial_solutions=Vec[PartialSolution, u32](pss),
                    proof=KZGProof(
                        w=G1Affine(
                            x=Fq(value=int(coinbase_solution["proof_x"])),
                            # This is very wrong
                            flags=False,
                        ),
                        random_v=Option[Field](None),
                    )
                )
            else:
                coinbase_solution = None

            return Block(
                block_hash=BlockHash.loads(block['block_hash']),
                previous_hash=BlockHash.loads(block['previous_hash']),
                header=Database._get_block_header(block),
                transactions=Transactions(
                    transactions=Vec[ConfirmedTransaction, u32](ctxs),
                ),
                ratifications=Vec[Ratify, u32](rs),
                coinbase=Option[CoinbaseSolution](coinbase_solution),
                signature=Signature.loads(block['signature']),
            )

    @staticmethod
    async def _get_full_block_range(start: int, end: int, conn: psycopg.AsyncConnection):
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT * FROM block WHERE height <= %s AND height > %s ORDER BY height DESC",
                (start, end)
            )
            blocks = await cur.fetchall()
            return [await Database._get_full_block(block, conn) for block in blocks]

    @staticmethod
    async def _get_fast_block(block: dict, conn: psycopg.AsyncConnection) -> dict:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT COUNT(*) FROM confirmed_transaction WHERE block_id = %s",
                (block["id"],)
            )
            transaction_count = (await cur.fetchone())["count"]
            await cur.execute(
                "SELECT COUNT(*) FROM partial_solution ps "
                "JOIN coinbase_solution cs on ps.coinbase_solution_id = cs.id "
                "WHERE cs.block_id = %s",
                (block["id"],)
            )
            partial_solution_count = (await cur.fetchone())["count"]
            return {
                **block,
                "transaction_count": transaction_count,
                "partial_solution_count": partial_solution_count,
            }

    @staticmethod
    async def _get_fast_block_range(start: int, end: int, conn: psycopg.AsyncConnection):
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT * FROM block WHERE height <= %s AND height > %s ORDER BY height DESC",
                (start, end)
            )
            blocks = await cur.fetchall()
            return [await Database._get_fast_block(block, conn) for block in blocks]

    async def get_latest_height(self):
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("SELECT height FROM block ORDER BY height DESC LIMIT 1")
                    result = await cur.fetchone()
                    if result is None:
                        return None
                    return result['height']
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_latest_block_timestamp(self):
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("SELECT timestamp FROM block ORDER BY height DESC LIMIT 1")
                    result = await cur.fetchone()
                    if result is None:
                        return None
                    return result['timestamp']
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise


    async def get_latest_block(self):
        conn: psycopg.AsyncConnection
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("SELECT * FROM block ORDER BY height DESC LIMIT 1")
                    block = await cur.fetchone()
                    if block is None:
                        return None
                    return await self._get_full_block(block, conn)
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_block_by_height(self, height: u32):
        conn: psycopg.AsyncConnection
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("SELECT * FROM block WHERE height = %s", (height,))
                    block = await cur.fetchone()
                    if block is None:
                        return None
                    return await self._get_full_block(block, conn)
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_block_hash_by_height(self, height: u32):
        conn: psycopg.AsyncConnection
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("SELECT * FROM block WHERE height = %s", (height,))
                    block = await cur.fetchone()
                    if block is None:
                        return None
                    return BlockHash.loads(block['block_hash'])
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_block_header_by_height(self, height: u32):
        conn: psycopg.AsyncConnection
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("SELECT * FROM block WHERE height = %s", (height,))
                    block = await cur.fetchone()
                    if block is None:
                        return None
                    return self._get_block_header(block)
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_block_by_hash(self, block_hash: BlockHash | str) -> Block | None:
        conn: psycopg.AsyncConnection
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("SELECT * FROM block WHERE block_hash = %s", (str(block_hash),))
                    block = await cur.fetchone()
                    if block is None:
                        return None
                    return await self._get_full_block(block, conn)
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_block_header_by_hash(self, block_hash: BlockHash) -> BlockHeader | None:
        conn: psycopg.AsyncConnection
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("SELECT * FROM block WHERE block_hash = %s", (str(block_hash),))
                    block = await cur.fetchone()
                    if block is None:
                        return None
                    return self._get_block_header(block)
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_recent_blocks_fast(self):
        conn: psycopg.AsyncConnection
        async with self.pool.connection() as conn:
            try:
                latest_height = await self.get_latest_height()
                return await Database._get_fast_block_range(latest_height, latest_height - 30, conn)
            except Exception as e:
                await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                raise

    # noinspection PyUnusedLocal
    async def get_validator_from_block_hash(self, block_hash: BlockHash) -> Address | None:
        raise NotImplementedError
        # noinspection PyUnreachableCode
        conn: psycopg.AsyncConnection
        async with self.pool.connection() as conn:
            try:
                # noinspection PyUnresolvedReferences,SqlResolve
                return await conn.fetchval(
                    "SELECT owner "
                    "FROM explorer.record r "
                    "JOIN explorer.transition ts ON r.output_transition_id = ts.id "
                    "JOIN explorer.transaction tx ON ts.transaction_id = tx.id "
                    "JOIN explorer.block b ON tx.block_id = b.id "
                    "WHERE ts.value_balance < 0 AND r.value > 0 AND b.block_hash = %s",
                    str(block_hash)
                )
            except Exception as e:
                await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                raise

    async def get_block_from_transaction_id(self, transaction_id: TransactionID | str) -> Block | None:
        conn: psycopg.AsyncConnection
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT b.* FROM block b "
                        "JOIN confirmed_transaction ct ON b.id = ct.block_id "
                        "JOIN transaction t ON ct.id = t.confimed_transaction_id WHERE t.transaction_id = %s",
                        (str(transaction_id),)
                    )
                    block = await cur.fetchone()
                    if block is None:
                        return None
                    return await self._get_full_block(block, conn)
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_block_from_transition_id(self, transition_id: TransitionID | str) -> Block | None:
        conn: psycopg.AsyncConnection
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT tx.transaction_id FROM transaction tx "
                        "JOIN transaction_execute te ON tx.id = te.transaction_id "
                        "JOIN transition ts ON te.id = ts.transaction_execute_id "
                        "WHERE ts.transition_id = %s",
                        (str(transition_id),)
                    )
                    transaction_id = await cur.fetchone()
                    if transaction_id is None:
                        await cur.execute(
                            "SELECT tx.transaction_id FROM transaction tx "
                            "JOIN fee ON tx.id = fee.transaction_id "
                            "JOIN transition ts ON fee.id = ts.fee_id "
                            "WHERE ts.transition_id = %s",
                            (str(transition_id),)
                        )
                        transaction_id = await cur.fetchone()
                    if transaction_id is None:
                        return None
                    return await self.get_block_from_transaction_id(transaction_id['transaction_id'])
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def search_block_hash(self, block_hash: str) -> [str]:
        conn: psycopg.AsyncConnection
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("SELECT block_hash FROM block WHERE block_hash LIKE %s", (f"{block_hash}%",))
                    result = await cur.fetchall()
                    return list(map(lambda x: x['block_hash'], result))
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def search_transaction_id(self, transaction_id: str) -> [str]:
        conn: psycopg.AsyncConnection
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("SELECT transaction_id FROM transaction WHERE transaction_id LIKE %s", (f"{transaction_id}%",))
                    result = await cur.fetchall()
                    return list(map(lambda x: x['transaction_id'], result))
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def search_transition_id(self, transition_id: str) -> [str]:
        conn: psycopg.AsyncConnection
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("SELECT transition_id FROM transition WHERE transition_id LIKE %s", (f"{transition_id}%",))
                    result = await cur.fetchall()
                    return list(map(lambda x: x['transition_id'], result))
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_blocks_range(self, start, end):
        conn: psycopg.AsyncConnection
        async with self.pool.connection() as conn:
            try:
                return await Database._get_full_block_range(start, end, conn)
            except Exception as e:
                await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                raise

    async def get_blocks_range_fast(self, start, end):
        conn: psycopg.AsyncConnection
        async with self.pool.connection() as conn:
            try:
                return await Database._get_fast_block_range(start, end, conn)
            except Exception as e:
                await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                raise

    async def get_block_coinbase_reward_by_height(self, height: int) -> int | None:
        conn: psycopg.AsyncConnection
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT coinbase_reward FROM block WHERE height = %s", (height,)
                    )
                    # Copilot: use double quotes for strings
                    return (await cur.fetchone())["coinbase_reward"]
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_block_target_sum_by_height(self, height: int) -> int | None:
        conn: psycopg.AsyncConnection
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT target_sum FROM coinbase_solution "
                        "JOIN block b on coinbase_solution.block_id = b.id "
                        "WHERE height = %s ",
                        (height,)
                    )
                    return (await cur.fetchone())["target_sum"]
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_leaderboard_size(self) -> int:
        conn: psycopg.AsyncConnection
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("SELECT COUNT(*) FROM leaderboard")
                    return (await cur.fetchone())["count"]
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_leaderboard(self, start: int, end: int) -> list:
        conn: psycopg.AsyncConnection
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT * FROM leaderboard "
                        "ORDER BY total_incentive DESC, total_reward DESC "
                        "LIMIT %s OFFSET %s",
                        (end - start, start)
                    )
                    return await cur.fetchall()
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_leaderboard_rewards_by_address(self, address: str) -> (int, int):
        conn: psycopg.AsyncConnection
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT total_reward, total_incentive FROM leaderboard WHERE address = %s", (address,)
                    )
                    row = await cur.fetchone()
                    return row["total_reward"], row["total_incentive"]
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_recent_solutions_by_address(self, address: str) -> list:
        conn: psycopg.AsyncConnection
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT b.height, b.timestamp, ps.nonce, ps.target, reward, cs.target_sum "
                        "FROM partial_solution ps "
                        "JOIN coinbase_solution cs ON cs.id = ps.coinbase_solution_id "
                        "JOIN block b ON b.id = cs.block_id "
                        "WHERE ps.address = %s "
                        "ORDER BY cs.id DESC "
                        "LIMIT 30",
                        (address,)
                    )
                    return await cur.fetchall()
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_solution_count_by_address(self, address: str) -> int:
        conn: psycopg.AsyncConnection
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT COUNT(*) FROM partial_solution WHERE address = %s", (address,)
                    )
                    return (await cur.fetchone())["count"]
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_solution_by_address(self, address: str, start: int, end: int) -> list:
        conn: psycopg.AsyncConnection
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT b.height, b.timestamp, ps.nonce, ps.target, reward, cs.target_sum "
                        "FROM partial_solution ps "
                        "JOIN coinbase_solution cs ON cs.id = ps.coinbase_solution_id "
                        "JOIN block b ON b.id = cs.block_id "
                        "WHERE ps.address = %s "
                        "ORDER BY cs.id DESC "
                        "LIMIT %s OFFSET %s",
                        (address, end - start, start)
                    )
                    return await cur.fetchall()
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_solution_by_height(self, height: int, start: int, end: int) -> list:
        conn: psycopg.AsyncConnection
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT ps.address, ps.nonce, ps.commitment, ps.target, reward "
                        "FROM partial_solution ps "
                        "JOIN coinbase_solution cs on ps.coinbase_solution_id = cs.id "
                        "JOIN block b on cs.block_id = b.id "
                        "WHERE b.height = %s "
                        "ORDER BY target DESC "
                        "LIMIT %s OFFSET %s",
                        (height, end - start, start)
                    )
                    return await cur.fetchall()
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def search_address(self, address: str) -> [str]:
        conn: psycopg.AsyncConnection
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT address FROM leaderboard WHERE address LIKE %s", (f"{address}%",)
                    )
                    return list(map(lambda x: x['address'], await cur.fetchall()))
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_address_speed(self, address: str) -> (float, int): # (speed, interval)
        conn: psycopg.AsyncConnection
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                interval_list = [900, 1800, 3600, 14400, 43200, 86400]
                now = int(time.time())
                try:
                    for interval in interval_list:
                        await cur.execute(
                            "SELECT b.height FROM partial_solution ps "
                            "JOIN coinbase_solution cs ON ps.coinbase_solution_id = cs.id "
                            "JOIN block b ON cs.block_id = b.id "
                            "WHERE address = %s AND timestamp > %s",
                            (address, now - interval)
                        )
                        partial_solutions = await cur.fetchall()
                        if len(partial_solutions) < 10:
                            continue
                        heights = list(map(lambda x: x['height'], partial_solutions))
                        ref_heights = list(map(lambda x: x - 1, set(heights)))
                        await cur.execute(
                            "SELECT height, proof_target FROM block WHERE height = ANY(%s::bigint[])", (ref_heights,)
                        )
                        ref_proof_targets = await cur.fetchall()
                        ref_proof_target_dict = dict(map(lambda x: (x['height'], x['proof_target']), ref_proof_targets))
                        total_solutions = 0
                        for height in heights:
                            total_solutions += ref_proof_target_dict[height - 1]
                        return total_solutions / interval, interval
                    return 0, 0
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_network_speed(self) -> float:
        conn: psycopg.AsyncConnection
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                now = int(time.time())
                interval = 900
                try:
                    await cur.execute(
                        "SELECT b.height FROM partial_solution ps "
                        "JOIN coinbase_solution cs ON ps.coinbase_solution_id = cs.id "
                        "JOIN block b ON cs.block_id = b.id "
                        "WHERE timestamp > %s",
                        (now - interval,)
                    )
                    partial_solutions = await cur.fetchall()
                    heights = list(map(lambda x: x['height'], partial_solutions))
                    ref_heights = list(map(lambda x: x - 1, set(heights)))
                    await cur.execute(
                        "SELECT height, proof_target FROM block WHERE height = ANY(%s::bigint[])", (ref_heights,)
                    )
                    ref_proof_targets = await cur.fetchall()
                    ref_proof_target_dict = dict(map(lambda x: (x['height'], x['proof_target']), ref_proof_targets))
                    total_solutions = 0
                    for height in heights:
                        total_solutions += ref_proof_target_dict[height - 1]
                    return total_solutions / interval
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_leaderboard_total(self) -> int:
        conn: psycopg.AsyncConnection
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("SELECT total_credit FROM leaderboard_total")
                    total_credit = await cur.fetchone()
                    if total_credit is None:
                        await cur.execute("INSERT INTO leaderboard_total (total_credit) VALUES (0)")
                        total_credit = 0
                    return int(total_credit["total_credit"])
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_puzzle_commitment(self, commitment: str) -> dict | None:
        conn: psycopg.AsyncConnection
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT reward, height FROM partial_solution "
                        "JOIN coinbase_solution cs on cs.id = partial_solution.coinbase_solution_id "
                        "JOIN block b on b.id = cs.block_id "
                        "WHERE commitment = %s",
                        (commitment,)
                    )
                    row = await cur.fetchone()
                    if row is None:
                        return None
                    return {
                        'reward': row['reward'],
                        'height': row['height']
                    }
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_function_definition(self, program_id: str, function_name: str) -> dict | None:
        conn: psycopg.AsyncConnection
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT * FROM program_function "
                        "JOIN program ON program.id = program_function.program_id "
                        "WHERE program.program_id = %s AND name = %s",
                        (program_id, function_name)
                    )
                    return await cur.fetchone()
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_program_count(self, no_helloworld: bool = False) -> int:
        conn: psycopg.AsyncConnection
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    if no_helloworld:
                        await cur.execute(
                            "SELECT COUNT(*) FROM program "
                            "WHERE feature_hash NOT IN (SELECT hash FROM program_filter_hash)"
                        )
                    else:
                        await cur.execute("SELECT COUNT(*) FROM program")
                    return (await cur.fetchone())['count']
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_programs(self, start, end, no_helloworld: bool = False) -> list:
        conn: psycopg.AsyncConnection
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    where = "WHERE feature_hash NOT IN (SELECT hash FROM program_filter_hash) " if no_helloworld else ""
                    await cur.execute(
                        "SELECT p.program_id, b.height, t.transaction_id, SUM(pf.called) as called "
                        "FROM program p "
                        "JOIN transaction_deploy td on p.transaction_deploy_id = td.id "
                        "JOIN transaction t on td.transaction_id = t.id "
                        "JOIN confirmed_transaction ct on t.confimed_transaction_id = ct.id "
                        "JOIN block b on ct.block_id = b.id "
                        "JOIN program_function pf on p.id = pf.program_id "
                        f"{where}"
                        "GROUP BY p.program_id, b.height, t.transaction_id "
                        "ORDER BY b.height DESC "
                        "LIMIT %s OFFSET %s",
                        (end - start, start)
                    )
                    return await cur.fetchall()
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_builtin_programs(self):
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT p.program_id, SUM(pf.called) as called "
                        "FROM program p "
                        "JOIN program_function pf on p.id = pf.program_id "
                        "WHERE p.transaction_deploy_id IS NULL "
                        "GROUP BY p.program_id "
                    )
                    return await cur.fetchall()
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_programs_with_feature_hash(self, feature_hash: bytes, start, end) -> list:
        conn: psycopg.AsyncConnection
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT p.program_id, b.height, t.transaction_id, SUM(pf.called) as called "
                        "FROM program p "
                        "JOIN transaction_deploy td on p.transaction_deploy_id = td.id "
                        "JOIN transaction t on td.transaction_id = t.id "
                        "JOIN confirmed_transaction ct on t.confimed_transaction_id = ct.id "
                        "JOIN block b on ct.block_id = b.id "
                        "JOIN program_function pf on p.id = pf.program_id "
                        "WHERE feature_hash = %s "
                        "GROUP BY p.program_id, b.height, t.transaction_id "
                        "ORDER BY b.height "
                        "LIMIT %s OFFSET %s",
                        (feature_hash, end - start, start)
                    )
                    return await cur.fetchall()
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise


    async def get_block_by_program_id(self, program_id: str) -> Block | None:
        conn: psycopg.AsyncConnection
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT height FROM transaction tx "
                        "JOIN transaction_deploy td on tx.id = td.transaction_id "
                        "JOIN program p on td.id = p.transaction_deploy_id "
                        "JOIN confirmed_transaction ct on ct.id = tx.confimed_transaction_id "
                        "JOIN block b on ct.block_id = b.id "
                        "WHERE p.program_id = %s",
                        (program_id,)
                    )
                    height = await cur.fetchone()
                    if height is None:
                        return None
                    return await self.get_block_by_height(height["height"])
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise


    async def get_program_called_times(self, program_id: str) -> int:
        conn: psycopg.AsyncConnection
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT sum(called) FROM program_function "
                        "JOIN program ON program.id = program_function.program_id "
                        "WHERE program.program_id = %s",
                        (program_id,)
                    )
                    return (await cur.fetchone())['sum']
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise


    async def get_program_calls(self, program_id: str, start: int, end: int) -> list:
        conn: psycopg.AsyncConnection
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT b.height, b.timestamp, ts.transition_id, function_name, ct.type "
                        "FROM transition ts "
                        "JOIN transaction_execute te on te.id = ts.transaction_execute_id "
                        "JOIN transaction t on te.transaction_id = t.id "
                        "JOIN confirmed_transaction ct on t.confimed_transaction_id = ct.id "
                        "JOIN block b on ct.block_id = b.id "
                        "WHERE ts.program_id = %s "
                        "ORDER BY b.height DESC "
                        "LIMIT %s OFFSET %s",
                        (program_id, end - start, start)
                    )
                    return await cur.fetchall()
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_program_similar_count(self, program_id: str) -> int:
        conn: psycopg.AsyncConnection
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT COUNT(*) FROM program "
                        "WHERE feature_hash = (SELECT feature_hash FROM program WHERE program_id = %s)",
                        (program_id,)
                    )
                    return (await cur.fetchone())['count'] - 1
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_program_feature_hash(self, program_id: str) -> bytes:
        conn: psycopg.AsyncConnection
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT feature_hash FROM program WHERE program_id = %s",
                        (program_id,)
                    )
                    return (await cur.fetchone())['feature_hash']
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def search_program(self, program_id: str) -> [str]:
        conn: psycopg.AsyncConnection
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    # TODO just use LIKE after we disallow uppercase program names
                    await cur.execute(
                        "SELECT program_id FROM program WHERE program_id ILIKE %s", (f"{program_id}%",)
                    )
                    return list(map(lambda x: x['program_id'], await cur.fetchall()))
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_recent_programs_by_address(self, address: str) -> list:
        conn: psycopg.AsyncConnection
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT program_id FROM program WHERE owner = %s ORDER BY id DESC LIMIT 30", (address,)
                    )
                    return list(map(lambda x: x['program_id'], await cur.fetchall()))
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_program_count_by_address(self, address: str) -> int:
        conn: psycopg.AsyncConnection
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("SELECT COUNT(*) FROM program WHERE owner = %s", (address,))
                    return (await cur.fetchone())['count']
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_program_bytes(self, program_id: str) -> bytes:
        conn: psycopg.AsyncConnection
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("SELECT raw_data FROM program WHERE program_id = %s", (program_id,))
                    return (await cur.fetchone())['raw_data']
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise


    async def get_program(self, program_id: str) -> bytes | None:
        conn: psycopg.AsyncConnection
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("SELECT raw_data FROM program WHERE program_id = %s", (program_id,))
                    res = await cur.fetchone()
                    if res is None:
                        return None
                    return res['raw_data']
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_mapping_cache(self, mapping_id: str) -> list:
        conn: psycopg.AsyncConnection
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT index, key_id, value_id, key, value FROM mapping_value mv "
                        "JOIN mapping m on mv.mapping_id = m.id "
                        "WHERE m.mapping_id = %s "
                        "ORDER BY index",
                        (mapping_id,)
                    )
                    return await cur.fetchall()
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_mapping_value(self, program_id: str, mapping: str, key_id: str) -> bytes | None:
        conn: psycopg.AsyncConnection
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT value FROM mapping_value mv "
                        "JOIN mapping m on mv.mapping_id = m.id "
                        "WHERE m.program_id = %s AND m.mapping = %s AND mv.key_id = %s",
                        (program_id, mapping, key_id)
                    )
                    res = await cur.fetchone()
                    if res is None:
                        return None
                    return res['value']
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def initialize_mapping(self, cur, mapping_id: str, program_id: str, mapping: str):
        try:
            await cur.execute(
                "INSERT INTO mapping (mapping_id, program_id, mapping) VALUES (%s, %s, %s)",
                (mapping_id, program_id, mapping)
            )
        except Exception as e:
            await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
            raise

    async def initialize_builtin_mapping(self, mapping_id: str, program_id: str, mapping: str):
        conn: psycopg.AsyncConnection
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "INSERT INTO mapping (mapping_id, program_id, mapping) VALUES (%s, %s, %s)",
                        (mapping_id, program_id, mapping)
                    )
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def update_mapping_key_value(self, cur, mapping_id: str, index: int, key_id: str, value_id: str,
                                        key: bytes, value: bytes):
        try:
            await cur.execute("SELECT id FROM mapping WHERE mapping_id = %s", (mapping_id,))
            mapping = await cur.fetchone()
            if mapping is None:
                raise ValueError(f"Mapping {mapping_id} not found")
            mapping_id = mapping['id']
            await cur.execute(
                "SELECT key_id FROM mapping_value WHERE mapping_id = %s AND index = %s",
                (mapping_id, index)
            )
            if (res := await cur.fetchone()) is None:
                await cur.execute(
                    "INSERT INTO mapping_value (mapping_id, index, key_id, value_id, key, value) "
                    "VALUES (%s, %s, %s, %s, %s, %s)",
                    (mapping_id, index, key_id, value_id, key, value)
                )
            else:
                if res["key_id"] != key_id:
                    raise ValueError(f"Key id mismatch: {res['key_id']} != {key_id}")
                await cur.execute(
                    "UPDATE mapping_value SET value_id = %s, value = %s "
                    "WHERE mapping_id = %s AND index = %s",
                    (value_id, value, mapping_id, index)
                )
        except Exception as e:
            await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
            raise
                
                
    async def get_program_leo_source_code(self, program_id: str) -> str | None:
        conn: psycopg.AsyncConnection
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("SELECT leo_source FROM program WHERE program_id = %s", (program_id,))
                    return (await cur.fetchone())['leo_source']
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def store_program_leo_source_code(self, program_id: str, source_code: str):
        conn: psycopg.AsyncConnection
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "UPDATE program SET leo_source = %s WHERE program_id = %s", (source_code, program_id)
                    )
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def save_feedback(self, contact: str, content: str):
        conn: psycopg.AsyncConnection
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("INSERT INTO feedback (contact, content) VALUES (%s, %s)", (contact, content))
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise


    # migration methods
    async def migrate(self):
        migrations = [

        ]
        conn: psycopg.AsyncConnection
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    for migrated_id, method in migrations:
                        await cur.execute("SELECT COUNT(*) FROM _migration WHERE migrated_id = %s", (migrated_id,))
                        if (await cur.fetchone())['count'] == 0:
                            print(f"DB migrating {migrated_id}")
                            async with conn.transaction():
                                await method()
                                await cur.execute("INSERT INTO _migration (migrated_id) VALUES (%s)", (migrated_id,))
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    # debug method
    async def clear_database(self):
        conn: psycopg.AsyncConnection
        async with self.pool.connection() as conn:
            try:
                await conn.execute("TRUNCATE TABLE block RESTART IDENTITY CASCADE")
                await conn.execute("TRUNCATE TABLE mapping RESTART IDENTITY CASCADE")
            except Exception as e:
                await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                raise