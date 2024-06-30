from __future__ import annotations

from collections import defaultdict

import psycopg
import psycopg.sql

from aleo_types import *
from explorer.types import Message as ExplorerMessage
from .base import DatabaseBase, profile


class DatabaseBlock(DatabaseBase):

    @staticmethod
    def _get_block_header(block: dict[str, Any]):
        return BlockHeader(
            previous_state_root=StateRoot.loads(block["previous_state_root"]),
            transactions_root=Field.loads(block["transactions_root"]),
            finalize_root=Field.loads(block["finalize_root"]),
            ratifications_root=Field.loads(block["ratifications_root"]),
            solutions_root=Field.loads(block["solutions_root"]),
            subdag_root=Field.loads(block["subdag_root"]),
            metadata=BlockHeaderMetadata(
                network=u16(3),
                round_=u64(block["round"]),
                height=u32(block["height"]),
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
    @profile
    async def _load_future(conn: psycopg.AsyncConnection[dict[str, Any]], transition_output_db_id: Optional[int],
                           future_argument_db_id: Optional[int]) -> Optional[Future]:
        async with conn.cursor() as cur:
            if transition_output_db_id:
                await cur.execute(
                    "SELECT id, program_id, function_name FROM future WHERE type = 'Output' AND "
                    "transition_output_future_id = %s",
                    (transition_output_db_id,)
                )
            elif future_argument_db_id:
                await cur.execute(
                    "SELECT id, program_id, function_name FROM future WHERE type = 'Argument' AND "
                    "future_argument_id = %s",
                    (future_argument_db_id,)
                )
            else:
                raise ValueError("transition_output_db_id or future_argument_db_id must be set")
            if (res := await cur.fetchone()) is None:
                if transition_output_db_id:
                    return None
                raise RuntimeError("failed to insert row into database")
            future_db_id = res["id"]
            program_id = res["program_id"]
            function_name = res["function_name"]
            await cur.execute(
                "SELECT id, type, plaintext FROM future_argument WHERE future_id = %s",
                (future_db_id,)
            )
            arguments: list[Argument] = []
            for res in await cur.fetchall():
                if res["type"] == "Plaintext":
                    arguments.append(PlaintextArgument(
                        plaintext=Plaintext.load(BytesIO(res["plaintext"]))
                    ))
                elif res["type"] == "Future":
                    arguments.append(FutureArgument(
                        future=await DatabaseBlock._load_future(conn, None, res["id"]) # type: ignore
                    ))
                else:
                    raise NotImplementedError
            return Future(
                program_id=ProgramID.loads(program_id),
                function_name=Identifier.loads(function_name),
                arguments=Vec[Argument, u8](arguments)
            )

    @staticmethod
    @profile
    async def _get_transition_from_dict(transition: dict[str, Any], conn: psycopg.AsyncConnection[dict[str, Any]]):
        async with conn.cursor() as cur:
            await cur.execute("SELECT * FROM get_transition_inputs(%s)", (transition["id"],))
            transition_inputs = await cur.fetchall()
            tis: list[tuple[TransitionInput, int]] = []
            for transition_input in transition_inputs:
                if transition_input["type"] == TransitionInput.Type.Public.name:
                    if transition_input["plaintext"] is None:
                        plaintext = None
                    else:
                        plaintext = Plaintext.load(BytesIO(transition_input["plaintext"]))
                    tis.append((PublicTransitionInput(
                        plaintext_hash=Field.loads(transition_input["plaintext_hash"]),
                        plaintext=Option[Plaintext](plaintext)
                    ), transition_input["index"]))
                elif transition_input["type"] == TransitionInput.Type.Private.name:
                    if transition_input["ciphertext"] is None:
                        ciphertext = None
                    else:
                        ciphertext = Ciphertext.loads(transition_input["ciphertext"])
                    tis.append((PrivateTransitionInput(
                        ciphertext_hash=Field.loads(transition_input["ciphertext_hash"]),
                        ciphertext=Option[Ciphertext](ciphertext)
                    ), transition_input["index"]))
                elif transition_input["type"] == TransitionInput.Type.Record.name:
                    tis.append((RecordTransitionInput(
                        serial_number=Field.loads(transition_input["serial_number"]),
                        tag=Field.loads(transition_input["tag"])
                    ), transition_input["index"]))
                elif transition_input["type"] == TransitionInput.Type.ExternalRecord.name:
                    tis.append((ExternalRecordTransitionInput(
                        input_commitment=Field.loads(transition_input["commitment"]),
                    ), transition_input["index"]))
                else:
                    raise NotImplementedError
            tis.sort(key=lambda x: x[1])
            transition_inputs = [x[0] for x in tis]

            await cur.execute("SELECT * FROM get_transition_outputs(%s)", (transition["id"],))
            transition_outputs = await cur.fetchall()
            tos: list[tuple[TransitionOutput, int]] = []
            for transition_output in transition_outputs:
                if transition_output["type"] == TransitionOutput.Type.Public.name:
                    if transition_output["plaintext"] is None:
                        plaintext = None
                    else:
                        plaintext = Plaintext.load(BytesIO(transition_output["plaintext"]))
                    tos.append((PublicTransitionOutput(
                        plaintext_hash=Field.loads(transition_output["plaintext_hash"]),
                        plaintext=Option[Plaintext](plaintext)
                    ), transition_output["index"]))
                elif transition_output["type"] == TransitionOutput.Type.Private.name:
                    if transition_output["ciphertext"] is None:
                        ciphertext = None
                    else:
                        ciphertext = Ciphertext.loads(transition_output["ciphertext"])
                    tos.append((PrivateTransitionOutput(
                        ciphertext_hash=Field.loads(transition_output["ciphertext_hash"]),
                        ciphertext=Option[Ciphertext](ciphertext)
                    ), transition_output["index"]))
                elif transition_output["type"] == TransitionOutput.Type.Record.name:
                    if transition_output["record_ciphertext"] is None:
                        record_ciphertext = None
                    else:
                        record_ciphertext = Record[Ciphertext].loads(transition_output["record_ciphertext"])
                    tos.append((RecordTransitionOutput(
                        commitment=Field.loads(transition_output["record_commitment"]),
                        checksum=Field.loads(transition_output["checksum"]),
                        record_ciphertext=Option[Record[Ciphertext]](record_ciphertext)
                    ), transition_output["index"]))
                elif transition_output["type"] == TransitionOutput.Type.ExternalRecord.name:
                    tos.append((ExternalRecordTransitionOutput(
                        commitment=Field.loads(transition_output["external_record_commitment"]),
                    ), transition_output["index"]))
                elif transition_output["type"] == TransitionOutput.Type.Future.name:
                    future = await DatabaseBlock._load_future(conn, transition_output["future_id"], None)
                    tos.append((FutureTransitionOutput(
                        future_hash=Field.loads(transition_output["future_hash"]),
                        future=Option[Future](future)
                    ), transition_output["index"]))
                else:
                    raise NotImplementedError
            tos.sort(key=lambda x: x[1])
            transition_outputs = [x[0] for x in tos]

            return Transition(
                id_=TransitionID.loads(transition["transition_id"]),
                program_id=ProgramID.loads(transition["program_id"]),
                function_name=Identifier.loads(transition["function_name"]),
                inputs=Vec[TransitionInput, u8](transition_inputs),
                outputs=Vec[TransitionOutput, u8](transition_outputs),
                tpk=Group.loads(transition["tpk"]),
                tcm=Field.loads(transition["tcm"]),
                scm=Field.loads(transition["scm"]),
            )

    async def get_transaction_reject_reason(self, transaction_id: TransactionID | str) -> Optional[str]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT reject_reason FROM confirmed_transaction ct "
                        "JOIN transaction t on ct.id = t.confirmed_transaction_id "
                        "WHERE t.transaction_id = %s",
                        (str(transaction_id),)
                    )
                    if (res := await cur.fetchone()) is None:
                        return None
                    return res["reject_reason"]
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_block_from_transaction_id(self, transaction_id: TransactionID | str) -> Block | None:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT b.* FROM block b "
                        "JOIN confirmed_transaction ct ON b.id = ct.block_id "
                        "JOIN transaction t ON ct.id = t.confirmed_transaction_id WHERE t.transaction_id = %s",
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

    async def get_block_confirm_time(self, height: int) -> Optional[int]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    if height == 0:
                        await cur.execute("SELECT timestamp FROM block WHERE height = 0")
                        res = await cur.fetchone()
                        if res is None:
                            return None
                        return res["timestamp"]
                    await cur.execute(
                        "SELECT max(dv.timestamp) FROM dag_vertex dv "
                        "JOIN authority a on dv.authority_id = a.id "
                        "JOIN block b on a.block_id = b.id "
                        "WHERE b.height = %s",
                        (height,)
                    )
                    res = await cur.fetchone()
                    if not res:
                        return None
                    return res["max"]
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_transition(self, transition_id: str) -> Optional[Transition]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("SELECT * FROM transition WHERE transition_id = %s", (transition_id,))
                    transition = await cur.fetchone()
                    if transition is None:
                        return None
                    return await DatabaseBlock._get_transition_from_dict(transition, conn)
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_updated_transaction_id(self, transaction_id: str) -> str:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT transaction_id FROM transaction WHERE original_transaction_id = %s",
                        (transaction_id,)
                    )
                    res = await cur.fetchone()
                    if res is None:
                        return transaction_id
                    return res["transaction_id"]
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def is_transaction_confirmed(self, transaction_id: str) -> Optional[bool]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT confirmed_transaction_id FROM transaction WHERE transaction_id = %s",
                        (transaction_id,)
                    )
                    res = await cur.fetchone()
                    if res is None:
                        return None
                    return res["confirmed_transaction_id"] is not None
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_unconfirmed_transaction(self, transaction_id: str) -> Optional[Transaction]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT id, transaction_id, type, confirmed_transaction_id FROM transaction "
                        "WHERE transaction_id = %s",
                        (transaction_id,)
                    )
                    transaction = await cur.fetchone()
                    if transaction is None:
                        return None
                    if transaction["confirmed_transaction_id"] is not None:
                        raise ValueError("transaction is confirmed")
                    if transaction["type"] == Transaction.Type.Deploy.name:
                        await cur.execute(
                            "SELECT id, edition, verifying_keys, program_id, owner FROM transaction_deploy WHERE transaction_id = %s",
                            (transaction["id"],)
                        )
                        deploy = await cur.fetchone()
                        if deploy is None:
                            raise RuntimeError("database inconsistent")
                        await cur.execute(
                            "SELECT id, global_state_root, proof FROM fee WHERE transaction_id = %s",
                            (transaction["id"],)
                        )
                        fee = await cur.fetchone()
                        if fee is None:
                            raise RuntimeError("database inconsistent")
                        await cur.execute("SELECT * FROM transition WHERE fee_id = %s", (fee["id"],))
                        fee_transition = await cur.fetchone()
                        if fee_transition is None:
                            raise ValueError("fee transition not found")
                        tx = DeployTransaction(
                            id_=TransactionID.loads(transaction["transaction_id"]),
                            deployment=Deployment(
                                edition=u16(),
                                program=Program(
                                    id_=ProgramID.loads("placeholder.aleo"),
                                    imports=Vec[Import, u8]([]),
                                    mappings={},
                                    structs={},
                                    records={},
                                    closures={},
                                    functions={},
                                    identifiers=[],
                                ),
                                verifying_keys=Vec[Tuple[Identifier, VerifyingKey, Certificate], u16].load(BytesIO(deploy["verifying_keys"])),
                            ),
                            fee=Fee(
                                transition=await self._get_transition_from_dict(fee_transition, conn),
                                global_state_root=StateRoot.loads(fee["global_state_root"]),
                                proof=Option[Proof](Proof.loads(fee["proof"])),
                            ),
                            owner=ProgramOwner(
                                address=Address.loads("aleo1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq3ljyzc"),
                                signature=Signature(
                                    challenge=Scalar(0),
                                    response=Scalar(0),
                                    compute_key=ComputeKey(
                                        pk_sig=Group(0),
                                        pr_sig=Group(0),
                                    )
                                )
                            )
                        )
                    elif transaction["type"] == Transaction.Type.Execute.name:
                        await cur.execute(
                            "SELECT id, global_state_root, proof FROM transaction_execute WHERE transaction_id = %s",
                            (transaction["id"],)
                        )
                        execute = await cur.fetchone()
                        if execute is None:
                            raise RuntimeError("database inconsistent")
                        await cur.execute(
                            "SELECT * FROM transition WHERE transaction_execute_id = %s",
                            (execute["id"],)
                        )
                        transitions = await cur.fetchall()
                        tss: list[Transition] = []
                        for transition in transitions:
                            tss.append(await self._get_transition_from_dict(transition, conn))
                        await cur.execute(
                            "SELECT id, global_state_root, proof FROM fee WHERE transaction_id = %s",
                            (transaction["id"],)
                        )
                        fee = await cur.fetchone()
                        if fee is not None:
                            await cur.execute("SELECT * FROM transition WHERE fee_id = %s", (fee["id"],))
                            fee_transition = await cur.fetchone()
                            fee = Fee(
                                transition=await self._get_transition_from_dict(fee_transition, conn),
                                global_state_root=StateRoot.loads(fee["global_state_root"]),
                                proof=Option[Proof](Proof.loads(fee["proof"])),
                            )
                        tx = ExecuteTransaction(
                            id_=TransactionID.loads(transaction["transaction_id"]),
                            execution=Execution(
                                transitions=Vec[Transition, u8](tss),
                                global_state_root=StateRoot.loads(execute["global_state_root"]),
                                proof=Option[Proof](Proof.loads(execute["proof"])),
                            ),
                            fee=Option[Fee](fee),
                        )
                    elif transaction["type"] == Transaction.Type.Fee.name:
                        raise ValueError("transaction is confirmed")
                    else:
                        raise NotImplementedError
                    return tx
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_rejected_transaction_original_id(self, transaction_id: str) -> Optional[str]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT original_transaction_id FROM transaction WHERE transaction_id = %s",
                        (transaction_id,)
                    )
                    res = await cur.fetchone()
                    if res is None:
                        return None
                    return res["original_transaction_id"]
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_deploy_transaction_program_info(self, transaction_id: str) -> Optional[dict[str, Optional[str]]]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT program_id, owner FROM transaction_deploy td "
                        "JOIN transaction t ON td.transaction_id = t.id "
                        "WHERE t.transaction_id = %s",
                        (transaction_id,)
                    )
                    deploy = await cur.fetchone()
                    if deploy is None:
                        return None
                    return deploy
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_transaction_first_seen(self, transaction_id: str) -> Optional[int]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT first_seen FROM transaction WHERE transaction_id = %s",
                        (transaction_id,)
                    )
                    res = await cur.fetchone()
                    if res is None:
                        return None
                    return res["first_seen"]
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_unconfirmed_transaction_count(self) -> int:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("SELECT COUNT(*) FROM transaction WHERE confirmed_transaction_id IS NULL")
                    res = await cur.fetchone()
                    if res is None:
                        raise RuntimeError("database inconsistent")
                    return res["count"]
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_unconfirmed_transactions_range(self, start: int, end: int) -> list[Transaction]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT transaction_id FROM transaction WHERE confirmed_transaction_id IS NULL "
                        "ORDER BY first_seen DESC LIMIT %s OFFSET %s",
                        (end - start, start)
                    )
                    transaction_ids = await cur.fetchall()
                    if not transaction_ids:
                        return []
                    txs: list[Transaction] = []
                    for transaction_id in transaction_ids:
                        tx = await self.get_unconfirmed_transaction(transaction_id["transaction_id"])
                        if tx is None:
                            raise RuntimeError("database inconsistent")
                        txs.append(tx)
                    return txs
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    @staticmethod
    async def get_confirmed_transaction_from_dict(conn: psycopg.AsyncConnection[dict[str, Any]], confirmed_transaction: dict[str, Any]) -> ConfirmedTransaction:
        async with conn.cursor() as cur:
            await cur.execute("SELECT * FROM get_finalize_operations(%s)", (confirmed_transaction["confirmed_transaction_id"],))
            finalize_operations = await cur.fetchall()
            f: list[FinalizeOperation] = []
            for finalize_operation in finalize_operations:
                if finalize_operation["type"] == FinalizeOperation.Type.InitializeMapping.name:
                    f.append(InitializeMapping(mapping_id=Field.loads(finalize_operation["mapping_id"])))
                elif finalize_operation["type"] == FinalizeOperation.Type.InsertKeyValue.name:
                    f.append(InsertKeyValue(
                        mapping_id=Field.loads(finalize_operation["mapping_id"]),
                        key_id=Field.loads(finalize_operation["key_id"]),
                        value_id=Field.loads(finalize_operation["value_id"]),
                    ))
                elif finalize_operation["type"] == FinalizeOperation.Type.UpdateKeyValue.name:
                    f.append(UpdateKeyValue(
                        mapping_id=Field.loads(finalize_operation["mapping_id"]),
                        key_id=Field.loads(finalize_operation["key_id"]),
                        value_id=Field.loads(finalize_operation["value_id"]),
                    ))
                elif finalize_operation["type"] == FinalizeOperation.Type.RemoveKeyValue.name:
                    f.append(RemoveKeyValue(
                        mapping_id=Field.loads(finalize_operation["mapping_id"]),
                        key_id=Field.loads(finalize_operation["key_id"]),
                    ))
                elif finalize_operation["type"] == FinalizeOperation.Type.ReplaceMapping.name:
                    f.append(ReplaceMapping(mapping_id=Field.loads(finalize_operation["mapping_id"])))
                elif finalize_operation["type"] == FinalizeOperation.Type.RemoveMapping.name:
                    f.append(RemoveMapping(mapping_id=Field.loads(finalize_operation["mapping_id"])))
                else:
                    raise NotImplementedError

            transaction = confirmed_transaction
            # TODO: store full program on rejected deploy so we dont need dummy data - should we?
            match confirmed_transaction["confirmed_transaction_type"]:
                case ConfirmedTransaction.Type.AcceptedDeploy.name | ConfirmedTransaction.Type.RejectedDeploy.name:
                    deploy_transaction = transaction
                    if confirmed_transaction["confirmed_transaction_type"] == ConfirmedTransaction.Type.AcceptedDeploy.name:
                        await cur.execute(
                            "SELECT raw_data, owner, signature FROM program WHERE transaction_deploy_id = %s",
                            (deploy_transaction["transaction_deploy_id"],)
                        )
                        program_data = await cur.fetchone()
                        if program_data is None:
                            raise RuntimeError("database inconsistent")
                        program = program_data["raw_data"]
                        deployment = Deployment(
                            edition=u16(deploy_transaction["edition"]),
                            program=Program.load(BytesIO(program)),
                            verifying_keys=Vec[Tuple[Identifier, VerifyingKey, Certificate], u16].load(BytesIO(deploy_transaction["verifying_keys"])),
                        )
                    else:
                        deployment = Deployment(
                            edition=u16(deploy_transaction["edition"]),
                            program=Program(
                                id_=ProgramID.loads("placeholder.aleo"),
                                imports=Vec[Import, u8]([]),
                                mappings={},
                                structs={},
                                records={},
                                closures={},
                                functions={},
                                identifiers=[],
                            ),
                            verifying_keys=Vec[Tuple[Identifier, VerifyingKey, Certificate], u16]([])
                        )
                        program_data = None
                    fee_dict = transaction
                    if not fee_dict:
                        raise RuntimeError("database inconsistent")
                    await cur.execute(
                        "SELECT * FROM transition WHERE fee_id = %s",
                        (fee_dict["fee_id"],)
                    )
                    fee_transition = await cur.fetchone()
                    if fee_transition is None:
                        raise ValueError("fee transition not found")
                    proof = None
                    if fee_dict["fee_proof"] is not None:
                        proof = Proof.loads(fee_dict["fee_proof"])
                    fee = Fee(
                        transition=await DatabaseBlock._get_transition_from_dict(fee_transition, conn),
                        global_state_root=StateRoot.loads(fee_dict["fee_global_state_root"]),
                        proof=Option[Proof](proof),
                    )
                    if confirmed_transaction["confirmed_transaction_type"] == ConfirmedTransaction.Type.AcceptedDeploy.name:
                        program_data = cast(dict[str, Any], program_data)
                        tx = DeployTransaction(
                            id_=TransactionID.loads(transaction["transaction_id"]),
                            deployment=deployment,
                            fee=fee,
                            owner=ProgramOwner(
                                address=Address.loads(program_data["owner"]),
                                signature=Signature.loads(program_data["signature"])
                            )
                        )
                    else:
                        tx = DeployTransaction(
                            id_=TransactionID.loads(transaction["transaction_id"]),
                            deployment=deployment,
                            fee=fee,
                            owner=ProgramOwner(
                                address=Address.loads(deploy_transaction["owner"]),
                                signature=Signature(
                                    challenge=Scalar(0),
                                    response=Scalar(0),
                                    compute_key=ComputeKey(
                                        pk_sig=Group(0),
                                        pr_sig=Group(0),
                                    )
                                )
                            )
                        )
                    ctx = AcceptedDeploy(
                        index=u32(confirmed_transaction["index"]),
                        transaction=tx,
                        finalize=Vec[FinalizeOperation, u16](f),
                    )
                case ConfirmedTransaction.Type.AcceptedExecute.name | ConfirmedTransaction.Type.RejectedExecute.name:
                    execute_transaction = transaction
                    await cur.execute(
                        "SELECT * FROM transition WHERE transaction_execute_id = %s",
                        (execute_transaction["transaction_execute_id"],)
                    )
                    transitions = await cur.fetchall()
                    tss: list[Transition] = []
                    for transition in transitions:
                        tss.append(await DatabaseBlock._get_transition_from_dict(transition, conn))
                    fee = transaction
                    if fee["fee_id"] is None:
                        fee = None
                    else:
                        await cur.execute(
                            "SELECT * FROM transition WHERE fee_id = %s",
                            (fee["fee_id"],)
                        )
                        fee_transition = await cur.fetchone()
                        if fee_transition is None:
                            print(transaction)
                            raise ValueError("fee transition not found")
                        proof = None
                        if fee["fee_proof"] is not None:
                            proof = Proof.loads(fee["fee_proof"])
                        fee = Fee(
                            transition=await DatabaseBlock._get_transition_from_dict(fee_transition, conn),
                            global_state_root=StateRoot.loads(fee["fee_global_state_root"]),
                            proof=Option[Proof](proof),
                        )
                    if execute_transaction["proof"] is None:
                        proof = None
                    else:
                        proof = Proof.loads(execute_transaction["proof"])
                    if confirmed_transaction["confirmed_transaction_type"] == ConfirmedTransaction.Type.AcceptedExecute.name:
                        ctx = AcceptedExecute(
                            index=u32(confirmed_transaction["index"]),
                            transaction=ExecuteTransaction(
                                id_=TransactionID.loads(transaction["transaction_id"]),
                                execution=Execution(
                                    transitions=Vec[Transition, u8](tss),
                                    global_state_root=StateRoot.loads(execute_transaction["global_state_root"]),
                                    proof=Option[Proof](proof),
                                ),
                                fee=Option[Fee](fee),
                            ),
                            finalize=Vec[FinalizeOperation, u16](f),
                        )
                    else:
                        if fee is None:
                            raise ValueError("fee is None")
                        ctx = RejectedExecute(
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
                            ),
                            finalize=Vec[FinalizeOperation, u16](f),
                        )
                case _:
                    raise NotImplementedError
            return ctx

    async def get_confirmed_transaction(self, transaction_id: str) -> Optional[ConfirmedTransaction]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT t.id as transaction_db_id,  t.transaction_id, t.type as transaction_type, "
                        "ct.id as confirmed_transaction_id, ct.type as confirmed_transaction_type, ct.index, ct.reject_reason "
                        "FROM transaction t "
                        "JOIN confirmed_transaction ct ON t.confirmed_transaction_id = ct.id "
                        "WHERE transaction_id = %s", (transaction_id,))
                    confirmed_transaction = await cur.fetchone()
                    if confirmed_transaction is None:
                        return None
                    confirmed_transaction_type = confirmed_transaction["confirmed_transaction_type"]
                    if confirmed_transaction_type == ConfirmedTransaction.Type.AcceptedDeploy.name or \
                        confirmed_transaction_type == ConfirmedTransaction.Type.RejectedDeploy.name:
                        if confirmed_transaction_type == ConfirmedTransaction.Type.RejectedDeploy.name:
                            await cur.execute(
                                "SELECT id as transaction_deploy_id, edition, verifying_keys, program_id, owner "
                                "FROM transaction_deploy WHERE transaction_id = %s",
                                (confirmed_transaction["transaction_db_id"],)
                            )
                        else:
                            await cur.execute(
                                "SELECT id as transaction_deploy_id, edition, verifying_keys FROM transaction_deploy WHERE transaction_id = %s",
                                (confirmed_transaction["transaction_db_id"],)
                            )
                        deploy = await cur.fetchone()
                        if deploy is None:
                            raise RuntimeError("database inconsistent")
                        confirmed_transaction.update(deploy)
                    elif confirmed_transaction_type == ConfirmedTransaction.Type.AcceptedExecute.name or \
                        confirmed_transaction_type == ConfirmedTransaction.Type.RejectedExecute.name:
                        await cur.execute(
                            "SELECT id as transaction_execute_id, global_state_root, proof FROM transaction_execute WHERE transaction_id = %s",
                            (confirmed_transaction["transaction_db_id"],)
                        )
                        execute = await cur.fetchone()
                        if execute is None:
                            raise RuntimeError("database inconsistent")
                        confirmed_transaction.update(execute)
                    else:
                        raise NotImplementedError
                    await cur.execute(
                        "SELECT id as fee_id, global_state_root as fee_global_state_root, proof as fee_proof "
                        "FROM fee WHERE transaction_id = %s",
                        (confirmed_transaction["transaction_db_id"],)
                    )
                    fee = await cur.fetchone()
                    if fee is None:
                        confirmed_transaction.update({"fee_id": None, "fee_global_state_root": None, "fee_proof": None})
                    else:
                        confirmed_transaction.update(fee)
                    return await DatabaseBlock.get_confirmed_transaction_from_dict(conn, confirmed_transaction)
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    @staticmethod
    @profile
    async def _get_full_block(block: dict[str, Any], conn: psycopg.AsyncConnection[dict[str, Any]]):
        async with conn.cursor() as cur:
            await cur.execute("SELECT * FROM get_confirmed_transactions(%s)", (block["id"],))
            confirmed_transactions = await cur.fetchall()
            ctxs: list[ConfirmedTransaction] = []
            for confirmed_transaction in confirmed_transactions:
                ctxs.append(await DatabaseBlock.get_confirmed_transaction_from_dict(conn, confirmed_transaction))
            await cur.execute("SELECT * FROM ratification WHERE block_id = %s ORDER BY index", (block["id"],))
            ratifications = await cur.fetchall()
            rs: list[Ratify] = []
            for ratification in ratifications:
                match ratification["type"]:
                    case Ratify.Type.Genesis.name:
                        await cur.execute("SELECT * FROM committee_history WHERE height = %s", (0,))
                        committee_history = await cur.fetchone()
                        if committee_history is None:
                            raise RuntimeError("database inconsistent")
                        await cur.execute("SELECT * FROM committee_history_member WHERE committee_id = %s", (committee_history["id"],))
                        committee_history_members = await cur.fetchall()
                        members: list[Tuple[Address, u64, bool_, u8]] = []
                        for committee_history_member in committee_history_members:
                            members.append(Tuple[Address, u64, bool_, u8]((
                                Address.loads(committee_history_member["address"]),
                                u64(committee_history_member["stake"]),
                                bool_(committee_history_member["is_open"]),
                                u8(committee_history_member["commission"]),
                            )))
                        committee = Committee(
                            id_=Field.loads(committee_history["committee_id"]),
                            starting_round=u64(committee_history["starting_round"]),
                            members=Vec[Tuple[Address, u64, bool_, u8], u16](members),
                            total_stake=u64(committee_history["total_stake"]),
                        )
                        await cur.execute("SELECT * FROM ratification_genesis_balance")
                        public_balances = await cur.fetchall()
                        balances: list[Tuple[Address, u64]] = []
                        for public_balance in public_balances:
                            balances.append(Tuple[Address, u64]((Address.loads(public_balance["address"]), u64(public_balance["amount"]))))
                        await cur.execute("SELECT * FROM ratification_genesis_bonded")
                        bonded_balances = await cur.fetchall()
                        bonded: list[Tuple[Address, Address, Address, u64]] = []
                        for bonded_balance in bonded_balances:
                            bonded.append(
                                Tuple[Address, Address, Address, u64]((
                                    Address.loads(bonded_balance["staker"]),
                                    Address.loads(bonded_balance["validator"]),
                                    Address.loads(bonded_balance["withdrawal"]),
                                    u64(bonded_balance["amount"])
                                ))
                            )
                        rs.append(GenesisRatify(
                            committee=committee,
                            public_balances=Vec[Tuple[Address, u64], u16](balances),
                            bonded_balances=Vec[Tuple[Address, Address, Address, u64], u16](bonded),
                        ))
                    case Ratify.Type.BlockReward.name:
                        rs.append(BlockRewardRatify(
                            amount=u64(ratification["amount"]),
                        ))
                    case Ratify.Type.PuzzleReward.name:
                        rs.append(PuzzleRewardRatify(
                            amount=u64(ratification["amount"]),
                        ))
                    case _:
                        raise NotImplementedError

            await cur.execute("SELECT * FROM puzzle_solution WHERE block_id = %s", (block["id"],))
            puzzle_solution = await cur.fetchone()
            if puzzle_solution is not None:
                await cur.execute(
                    "SELECT * FROM solution WHERE puzzle_solution_id = %s",
                    (puzzle_solution["id"],)
                )
                solutions = await cur.fetchall()
                ss: list[Solution] = []
                for solution in solutions:
                    ss.append(Solution(
                        partial_solution=PartialSolution(
                            solution_id=solution["puzzle_solution_id"],
                            epoch_hash=solution["epoch_hash"],
                            address=Address.loads(solution["address"]),
                            counter=u64(solution["counter"]),
                        ),
                        target=u64(solution["target"]),
                    ))
                puzzle_solution = PuzzleSolutions(solutions=Vec[Solution, u8](ss))
            else:
                puzzle_solution = None

            await cur.execute("SELECT * FROM authority WHERE block_id = %s", (block["id"],))
            authority = await cur.fetchone()
            if authority is None:
                raise RuntimeError("database inconsistent")
            if authority["type"] == Authority.Type.Beacon.name:
                auth = BeaconAuthority(
                    signature=Signature.loads(authority["signature"]),
                )
            elif authority["type"] == Authority.Type.Quorum.name:
                await cur.execute(
                    "SELECT * FROM dag_vertex WHERE authority_id = %s ORDER BY index",
                    (authority["id"],)
                )
                dag_vertices = await cur.fetchall()
                certificates: list[BatchCertificate] = []
                for dag_vertex in dag_vertices:
                    # await cur.execute(
                    #     "SELECT * FROM dag_vertex_signature WHERE vertex_id = %s ORDER BY index",
                    #     (dag_vertex["id"],)
                    # )
                    # dag_vertex_signatures = await cur.fetchall()

                    signatures: list[Signature] = []
                    # for signature in dag_vertex_signatures:
                    #     signatures.append(Signature.loads(signature["signature"]))

                    # await cur.execute(
                    #     "SELECT previous_vertex_id FROM dag_vertex_adjacency WHERE vertex_id = %s ORDER BY index",
                    #     (dag_vertex["id"],)
                    # )
                    # previous_ids = [x["previous_vertex_id"] for x in await cur.fetchall()]

                    # TODO: use batch id after next reset - do we still want to keep this? would be way too expensive
                    # await cur.execute(
                    #     "SELECT batch_certificate_id FROM dag_vertex v "
                    #     "JOIN UNNEST(%s) WITH ORDINALITY q(id, ord) ON q.id = v.id "
                    #     "ORDER BY ord",
                    #     (previous_ids,)
                    # )
                    # previous_cert_ids = [x["batch_certificate_id"] for x in await cur.fetchall()]
                    previous_cert_ids: list[str] = []

                    await cur.execute(
                        "SELECT * FROM dag_vertex_transmission_id WHERE vertex_id = %s ORDER BY index",
                        (dag_vertex["id"],)
                    )
                    tids: list[TransmissionID] = []
                    for tid in await cur.fetchall():
                        if tid["type"] == TransmissionID.Type.Ratification:
                            tids.append(RatificationTransmissionID())
                        elif tid["type"] == TransmissionID.Type.Solution:
                            tids.append(SolutionTransmissionID(id_=SolutionID.loads(tid["commitment"])))
                        elif tid["type"] == TransmissionID.Type.Transaction:
                            tids.append(TransactionTransmissionID(id_=TransactionID.loads(tid["transaction_id"])))

                    certificates.append(
                        BatchCertificate(
                            batch_header=BatchHeader(
                                batch_id=Field.loads(dag_vertex["batch_id"]),
                                author=Address.loads(dag_vertex["author"]),
                                round_=u64(dag_vertex["round"]),
                                timestamp=i64(dag_vertex["timestamp"]),
                                committee_id=Field.loads(dag_vertex["committee_id"]),
                                transmission_ids=Vec[TransmissionID, u32](tids),
                                previous_certificate_ids=Vec[Field, u16]([Field.loads(x) for x in previous_cert_ids]),
                                signature=Signature.loads(dag_vertex["author_signature"]),
                            ),
                            signatures=Vec[Signature, u16](signatures),
                        )
                    )
                subdags: dict[u64, Vec[BatchCertificate, u16]] = defaultdict(lambda: Vec[BatchCertificate, u16]([]))
                for certificate in certificates:
                    subdags[certificate.batch_header.round].append(certificate)
                subdag = Subdag(
                    subdag=subdags
                )
                auth = QuorumAuthority(subdag=subdag)
            else:
                raise NotImplementedError

            await cur.execute("SELECT * FROM block_aborted_solution_id WHERE block_id = %s", (block["id"],))
            aborted_solution_ids = await cur.fetchall()
            aborted_solution_ids = [SolutionID.loads(x["solution_id"]) for x in aborted_solution_ids]

            await cur.execute("SELECT * FROM block_aborted_transaction_id WHERE block_id = %s", (block["id"],))
            aborted_transaction_ids = await cur.fetchall()
            aborted_transaction_ids = [TransactionID.loads(x["transaction_id"]) for x in aborted_transaction_ids]

            return Block(
                block_hash=BlockHash.loads(block['block_hash']),
                previous_hash=BlockHash.loads(block['previous_hash']),
                header=DatabaseBlock._get_block_header(block),
                authority=auth,
                transactions=Transactions(
                    transactions=Vec[ConfirmedTransaction, u32](ctxs),
                ),
                ratifications=Ratifications(ratifications=Vec[Ratify, u32](rs)),
                solutions=Solutions(solutions=Option[PuzzleSolutions](puzzle_solution)),
                aborted_solution_ids=Vec[SolutionID, u32](aborted_solution_ids),
                aborted_transactions_ids=Vec[TransactionID, u32](aborted_transaction_ids),
            )

    @staticmethod
    async def get_full_block_range(start: int, end: int, conn: psycopg.AsyncConnection[dict[str, Any]]):
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT * FROM block WHERE height <= %s AND height > %s ORDER BY height DESC",
                (start, end)
            )
            blocks = await cur.fetchall()
            return [await DatabaseBlock._get_full_block(block, conn) for block in blocks]

    @staticmethod
    async def _get_fast_block(block: dict[str, Any], conn: psycopg.AsyncConnection[dict[str, Any]]) -> dict[str, Any]:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT COUNT(*) FROM confirmed_transaction WHERE block_id = %s",
                (block["id"],)
            )
            if (res := await cur.fetchone()) is None:
                transaction_count = 0
            else:
                transaction_count = res["count"]
            await cur.execute(
                "SELECT COUNT(*) FROM solution s "
                "JOIN puzzle_solution ps on s.puzzle_solution_id = ps.id "
                "WHERE ps.block_id = %s",
                (block["id"],)
            )
            if (res := await cur.fetchone()) is None:
                partial_solution_count = 0
            else:
                partial_solution_count = res["count"]
            return {
                **block,
                "transaction_count": transaction_count,
                "partial_solution_count": partial_solution_count,
            }

    @staticmethod
    async def _get_fast_block_range(start: int, end: int, conn: psycopg.AsyncConnection[dict[str, Any]]):
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT * FROM block WHERE height <= %s AND height > %s ORDER BY height DESC",
                (start, end)
            )
            blocks = await cur.fetchall()
            return [await DatabaseBlock._get_fast_block(block, conn) for block in blocks]

    async def get_latest_height(self) -> Optional[int]:
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

    async def get_latest_block_timestamp(self) -> int:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("SELECT timestamp FROM block ORDER BY height DESC LIMIT 1")
                    result = await cur.fetchone()
                    if result is None:
                        raise RuntimeError("no blocks in database")
                    return result['timestamp']
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise


    async def get_latest_block(self):
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("SELECT * FROM block ORDER BY height DESC LIMIT 1")
                    block = await cur.fetchone()
                    if block is None:
                        raise RuntimeError("no blocks in database")
                    return await self._get_full_block(block, conn)
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_latest_coinbase_target(self) -> int:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("SELECT coinbase_target FROM block ORDER BY height DESC LIMIT 1")
                    result = await cur.fetchone()
                    if result is None:
                        raise RuntimeError("no blocks in database")
                    return result['coinbase_target']
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_latest_cumulative_proof_target(self) -> int:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("SELECT cumulative_proof_target FROM block ORDER BY height DESC LIMIT 1")
                    result = await cur.fetchone()
                    if result is None:
                        raise RuntimeError("no blocks in database")
                    return result['cumulative_proof_target']
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_block_by_height(self, height: int):
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

    async def get_block_hash_by_height(self, height: int):
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

    async def get_block_header_by_height(self, height: int):
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

    async def get_recent_blocks_fast(self, limit: int = 30):
        async with self.pool.connection() as conn:
            try:
                latest_height = await self.get_latest_height()
                if latest_height is None:
                    raise RuntimeError("no blocks in database")
                return await DatabaseBlock._get_fast_block_range(latest_height, latest_height - limit, conn)
            except Exception as e:
                await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                raise

    async def get_blocks_range(self, start: int, end: int):
        async with self.pool.connection() as conn:
            try:
                return await DatabaseBlock.get_full_block_range(start, end, conn)
            except Exception as e:
                await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                raise

    async def get_blocks_range_fast(self, start: int, end: int):
        async with self.pool.connection() as conn:
            try:
                return await DatabaseBlock._get_fast_block_range(start, end, conn)
            except Exception as e:
                await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                raise

    async def get_block_coinbase_reward_by_height(self, height: int) -> Optional[int]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT coinbase_reward FROM block WHERE height = %s", (height,)
                    )
                    if (res := await cur.fetchone()) is None:
                        return None
                    return res["coinbase_reward"]
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_block_target_sum_by_height(self, height: int) -> int:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT target_sum FROM puzzle_solution ps "
                        "JOIN block b on ps.block_id = b.id "
                        "WHERE height = %s ",
                        (height,)
                    )
                    if (res := await cur.fetchone()) is None:
                        return 0
                    return res["target_sum"]
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise