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
    async def _get_transition(transition: dict[str, Any], conn: psycopg.AsyncConnection[dict[str, Any]]):
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
            )

    async def get_transaction_reject_reason(self, transaction_id: TransactionID | str) -> Optional[str]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT reject_reason FROM confirmed_transaction ct "
                        "JOIN transaction t on ct.id = t.confimed_transaction_id "
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

    async def get_transition(self, transition_id: str) -> Optional[Transition]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("SELECT * FROM transition WHERE transition_id = %s", (transition_id,))
                    transition = await cur.fetchone()
                    if transition is None:
                        return None
                    return await DatabaseBlock._get_transition(transition, conn)
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
                            index=u64(),
                            key_id=Field.loads(finalize_operation["key_id"]),
                            value_id=Field.loads(finalize_operation["value_id"]),
                        ))
                    elif finalize_operation["type"] == FinalizeOperation.Type.RemoveKeyValue.name:
                        f.append(RemoveKeyValue(
                            mapping_id=Field.loads(finalize_operation["mapping_id"]),
                            index=u64(),
                        ))
                    elif finalize_operation["type"] == FinalizeOperation.Type.RemoveMapping.name:
                        f.append(RemoveMapping(mapping_id=Field.loads(finalize_operation["mapping_id"])))
                    else:
                        raise NotImplementedError

                transaction = confirmed_transaction
                match confirmed_transaction["confirmed_transaction_type"]:
                    case ConfirmedTransaction.Type.AcceptedDeploy.name | ConfirmedTransaction.Type.RejectedDeploy.name:
                        if confirmed_transaction["confirmed_transaction_type"] == ConfirmedTransaction.Type.RejectedDeploy.name:
                            raise NotImplementedError
                        deploy_transaction = transaction
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
                        fee_dict = transaction
                        if fee_dict is None:
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
                            transition=await DatabaseBlock._get_transition(fee_transition, conn),
                            global_state_root=StateRoot.loads(fee_dict["fee_global_state_root"]),
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
                        execute_transaction = transaction
                        await cur.execute(
                            "SELECT * FROM transition WHERE transaction_execute_id = %s",
                            (execute_transaction["transaction_execute_id"],)
                        )
                        transitions = await cur.fetchall()
                        tss: list[Transition] = []
                        for transition in transitions:
                            tss.append(await DatabaseBlock._get_transition(transition, conn))
                        additional_fee = transaction
                        if additional_fee["fee_id"] is None:
                            fee = None
                        else:
                            await cur.execute(
                                "SELECT * FROM transition WHERE fee_id = %s",
                                (additional_fee["fee_id"],)
                            )
                            fee_transition = await cur.fetchone()
                            if fee_transition is None:
                                raise ValueError("fee transition not found")
                            proof = None
                            if additional_fee["fee_proof"] is not None:
                                proof = Proof.loads(additional_fee["fee_proof"])
                            fee = Fee(
                                transition=await DatabaseBlock._get_transition(fee_transition, conn),
                                global_state_root=StateRoot.loads(additional_fee["fee_global_state_root"]),
                                proof=Option[Proof](proof),
                            )
                        if execute_transaction["proof"] is None:
                            proof = None
                        else:
                            proof = Proof.loads(execute_transaction["proof"])
                        if confirmed_transaction["confirmed_transaction_type"] == ConfirmedTransaction.Type.AcceptedExecute.name:
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
                            if fee is None:
                                raise ValueError("fee is None")
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
                                ),
                                finalize=Vec[FinalizeOperation, u16](f),
                            ))
                    case _:
                        raise NotImplementedError

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
                        members: list[Tuple[Address, u64, bool_]] = []
                        for committee_history_member in committee_history_members:
                            members.append(Tuple[Address, u64, bool_]((
                                Address.loads(committee_history_member["address"]),
                                u64(committee_history_member["stake"]),
                                bool_(committee_history_member["is_open"]))
                            ))
                        committee = Committee(
                            starting_round=u64(committee_history["starting_round"]),
                            members=Vec[Tuple[Address, u64, bool_], u16](members),
                            total_stake=u64(committee_history["total_stake"]),
                        )
                        await cur.execute("SELECT * FROM ratification_genesis_balance")
                        public_balances = await cur.fetchall()
                        balances: list[Tuple[Address, u64]] = []
                        for public_balance in public_balances:
                            balances.append(Tuple[Address, u64]((Address.loads(public_balance["address"]), u64(public_balance["amount"]))))
                        rs.append(GenesisRatify(
                            committee=committee,
                            public_balances=Vec[Tuple[Address, u64], u16](balances),
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

            await cur.execute("SELECT * FROM coinbase_solution WHERE block_id = %s", (block["id"],))
            coinbase_solution = await cur.fetchone()
            if coinbase_solution is not None:
                await cur.execute(
                    "SELECT * FROM prover_solution WHERE coinbase_solution_id = %s",
                    (coinbase_solution["id"],)
                )
                prover_solutions = await cur.fetchall()
                pss: list[ProverSolution] = []
                for prover_solution in prover_solutions:
                    pss.append(ProverSolution(
                        partial_solution=PartialSolution(
                            address=Address.loads(prover_solution["address"]),
                            nonce=u64(prover_solution["nonce"]),
                            commitment=PuzzleCommitment.loads(prover_solution["commitment"]),
                        ),
                        proof=KZGProof(
                            w=G1Affine(
                                x=Fq(value=int(prover_solution["proof_x"])),
                                y_is_positive=prover_solution["proof_y_is_positive"],
                            ),
                            random_v=Option[Field](None),
                        )
                    ))
                coinbase_solution = CoinbaseSolution(solutions=Vec[ProverSolution, u16](pss))
            else:
                coinbase_solution = None

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

                    if dag_vertex["batch_certificate_id"] is not None:
                        signatures: list[Tuple[Signature, i64]] = []
                        # for signature in dag_vertex_signatures:
                        #     signatures.append(
                        #         Tuple[Signature, i64]((
                        #             Signature.loads(signature["signature"]),
                        #             i64(signature["timestamp"]),
                        #         ))
                        #     )
                    else:
                        signatures: list[Signature] = []
                        # for signature in dag_vertex_signatures:
                        #     signatures.append(Signature.loads(signature["signature"]))

                    # await cur.execute(
                    #     "SELECT previous_vertex_id FROM dag_vertex_adjacency WHERE vertex_id = %s ORDER BY index",
                    #     (dag_vertex["id"],)
                    # )
                    # previous_ids = [x["previous_vertex_id"] for x in await cur.fetchall()]

                    # TODO: use batch id after next reset
                    # await cur.execute(
                    #     "SELECT batch_certificate_id FROM dag_vertex v "
                    #     "JOIN UNNEST(%s) WITH ORDINALITY q(id, ord) ON q.id = v.id "
                    #     "ORDER BY ord",
                    #     (previous_ids,)
                    # )
                    # previous_cert_ids = [x["batch_certificate_id"] for x in await cur.fetchall()]
                    previous_cert_ids = []

                    await cur.execute(
                        "SELECT * FROM dag_vertex_transmission_id WHERE vertex_id = %s ORDER BY index",
                        (dag_vertex["id"],)
                    )
                    tids: list[TransmissionID] = []
                    for tid in await cur.fetchall():
                        if tid["type"] == TransmissionID.Type.Ratification:
                            tids.append(RatificationTransmissionID())
                        elif tid["type"] == TransmissionID.Type.Solution:
                            tids.append(SolutionTransmissionID(id_=PuzzleCommitment.loads(tid["commitment"])))
                        elif tid["type"] == TransmissionID.Type.Transaction:
                            tids.append(TransactionTransmissionID(id_=TransactionID.loads(tid["transaction_id"])))

                    if dag_vertex["batch_certificate_id"] is not None:
                        certificates.append(
                            BatchCertificate1(
                                certificate_id=Field.loads(dag_vertex["batch_certificate_id"]),
                                batch_header=BatchHeader1(
                                    batch_id=Field.loads(dag_vertex["batch_id"]),
                                    author=Address.loads(dag_vertex["author"]),
                                    round_=u64(dag_vertex["round"]),
                                    timestamp=i64(dag_vertex["timestamp"]),
                                    transmission_ids=Vec[TransmissionID, u32](tids),
                                    previous_certificate_ids=Vec[Field, u32]([Field.loads(x) for x in previous_cert_ids]),
                                    signature=Signature.loads(dag_vertex["author_signature"]),
                                ),
                                signatures=Vec[Tuple[Signature, i64], u32](signatures),
                            )
                        )
                    else:
                        certificates.append(
                            BatchCertificate2(
                                batch_header=BatchHeader1(
                                    batch_id=Field.loads(dag_vertex["batch_id"]),
                                    author=Address.loads(dag_vertex["author"]),
                                    round_=u64(dag_vertex["round"]),
                                    timestamp=i64(dag_vertex["timestamp"]),
                                    transmission_ids=Vec[TransmissionID, u32](tids),
                                    previous_certificate_ids=Vec[Field, u32]([Field.loads(x) for x in previous_cert_ids]),
                                    signature=Signature.loads(dag_vertex["author_signature"]),
                                ),
                                signatures=Vec[Signature, u16](signatures),
                            )
                        )
                subdags: dict[u64, Vec[BatchCertificate, u32]] = defaultdict(lambda: Vec[BatchCertificate, u32]([]))
                for certificate in certificates:
                    subdags[certificate.batch_header.round].append(certificate)
                subdag = Subdag1(
                    subdag=subdags
                )
                auth = QuorumAuthority(subdag=subdag)
            else:
                raise NotImplementedError

            return Block(
                block_hash=BlockHash.loads(block['block_hash']),
                previous_hash=BlockHash.loads(block['previous_hash']),
                header=DatabaseBlock._get_block_header(block),
                authority=auth,
                transactions=Transactions(
                    transactions=Vec[ConfirmedTransaction, u32](ctxs),
                ),
                ratifications=Ratifications(ratifications=Vec[Ratify, u32](rs)),
                solutions=Option[CoinbaseSolution](coinbase_solution),
                # TODO: save and fill in
                aborted_transactions_ids=Vec[TransactionID, u32]([]),
            )

    @staticmethod
    async def _get_full_block_range(start: int, end: int, conn: psycopg.AsyncConnection[dict[str, Any]]):
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
                "SELECT COUNT(*) FROM prover_solution ps "
                "JOIN coinbase_solution cs on ps.coinbase_solution_id = cs.id "
                "WHERE cs.block_id = %s",
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

    async def get_recent_blocks_fast(self):
        async with self.pool.connection() as conn:
            try:
                latest_height = await self.get_latest_height()
                if latest_height is None:
                    raise RuntimeError("no blocks in database")
                return await DatabaseBlock._get_fast_block_range(latest_height, latest_height - 30, conn)
            except Exception as e:
                await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                raise

    async def get_blocks_range(self, start: int, end: int):
        async with self.pool.connection() as conn:
            try:
                return await DatabaseBlock._get_full_block_range(start, end, conn)
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
                        "SELECT target_sum FROM coinbase_solution "
                        "JOIN block b on coinbase_solution.block_id = b.id "
                        "WHERE height = %s ",
                        (height,)
                    )
                    if (res := await cur.fetchone()) is None:
                        return 0
                    return res["target_sum"]
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise