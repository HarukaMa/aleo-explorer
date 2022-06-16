import aleo
import asyncpg

from explorer.type import Message
from node.type import Block, Transaction, Transition, SerialNumber, RecordCiphertext, Event, Vec, u16, TransitionID, \
    AleoAmount, OuterProof, InnerCircuitID, LedgerRoot, BlockHash, BlockHeader, TransactionsRoot, BlockHeaderMetadata, \
    u32, i64, u64, u128, PoSWNonce, PoSWProof, Transactions, DeprecatedPoSWProof, RecordViewKeyEvent, Record, Operation, \
    CustomEvent, RecordViewKey, u8, NoopOperation, CoinbaseOperation, Address, TransferOperation, EvaluateOperation, \
    FunctionID, FunctionType, FunctionInputs, Payload, OperationEvent


class Database:

    def __init__(self, *, server: str, user: str, password: str, database: str, schema: str,
                 explorer_message: callable):
        self.server = server
        self.user = user
        self.password = password
        self.database = database
        self.schema = schema
        self.explorer_message = explorer_message
        self.pool = None

    async def connect(self):
        try:
            self.pool = await asyncpg.create_pool(host=self.server, user=self.user, password=self.password,
                                                  database=self.database, server_settings={'search_path': self.schema},
                                                  min_size=1, max_size=4)
        except Exception as e:
            await self.explorer_message(Message(Message.Type.DatabaseConnectError, e))
            return
        await self.explorer_message(Message(Message.Type.DatabaseConnected, None))

    async def get_latest_height(self):
        async with self.pool.acquire() as conn:
            try:
                result = await conn.fetchrow(
                    "SELECT height FROM block WHERE is_canonical = true ORDER BY height DESC LIMIT 1")
                if result is None:
                    return None
                return result['height']
            except Exception as e:
                await self.explorer_message(Message(Message.Type.DatabaseError, e))
                raise

    async def get_latest_weight(self):
        conn: asyncpg.Connection
        async with self.pool.acquire() as conn:
            try:
                result = await conn.fetchrow(
                    "SELECT cumulative_weight FROM block WHERE is_canonical = true ORDER BY height DESC LIMIT 1")
                if result is None:
                    return None
                return result['cumulative_weight']
            except Exception as e:
                await self.explorer_message(Message(Message.Type.DatabaseError, e))
                raise

    async def _save_block(self, block: Block, is_canonical: bool):
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                try:
                    block_db_id = await conn.fetchval(
                        "INSERT INTO block (height, block_hash, previous_block_hash, previous_ledger_root, "
                        "transactions_root, timestamp, difficulty_target, cumulative_weight, nonce, proof, is_canonical) "
                        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) RETURNING id",
                        block.header.metadata.height, str(block.block_hash), str(block.previous_block_hash),
                        str(block.header.previous_ledger_root),
                        str(block.header.transactions_root), block.header.metadata.timestamp,
                        block.header.metadata.difficulty_target, block.header.metadata.cumulative_weight,
                        str(block.header.nonce), str(block.header.proof), is_canonical)
                    transaction: Transaction
                    for transaction in block.transactions:
                        transaction_db_id = await conn.fetchval(
                            "INSERT INTO transaction (block_id, transaction_id, inner_circuit_id, ledger_root) VALUES ($1, $2, $3, $4) RETURNING id",
                            block_db_id, str(transaction.transaction_id), str(transaction.inner_circuit_id),
                            str(transaction.ledger_root))
                        transition: Transition
                        for transition in transaction.transitions:
                            transition_db_id = await conn.fetchval(
                                "INSERT INTO transition (transaction_id, transition_id, value_balance, proof) VALUES ($1, $2, $3, $4) RETURNING id",
                                transaction_db_id, str(transition.transition_id), transition.value_balance,
                                str(transition.proof))
                            for i, sn in enumerate(transition.serial_numbers):
                                await conn.execute(
                                    "INSERT INTO serial_number (transition_id, index, serial_number) VALUES ($1, $2, $3)",
                                    transition_db_id, i, str(sn))
                            for i, event in enumerate(transition.events):
                                event_db_id = await conn.fetchval(
                                    "INSERT INTO event (transition_id, index, event_type) VALUES ($1, $2, $3) RETURNING id",
                                    transition_db_id, i, event.type.name)
                                match event.type:
                                    case Event.Type.Custom:
                                        await conn.execute("INSERT INTO custom_event (event_id, bytes) VALUES ($1, $2)",
                                                           event_db_id, event.event.bytes.dump())
                                    case Event.Type.RecordViewKey:
                                        await conn.execute(
                                            "INSERT INTO record_view_key_event (event_id, index, record_view_key) VALUES ($1, $2, $3)",
                                            event_db_id, event.event.index, str(event.event.record_view_key))
                                    case Event.Type.Operation:
                                        operation_event_db_id = await conn.fetchval(
                                            "INSERT INTO operation_event (event_id, operation_type) VALUES ($1, $2) RETURNING id",
                                            event_db_id, event.event.operation.type.name)
                                        match event.event.operation.type:
                                            case Operation.Type.Coinbase:
                                                await conn.execute(
                                                    "INSERT INTO coinbase_operation (operation_event_id, recipient, amount) VALUES ($1, $2, $3)",
                                                    operation_event_db_id,
                                                    str(event.event.operation.operation.recipient),
                                                    str(event.event.operation.operation.amount))
                                            case Operation.Type.Transfer:
                                                await conn.execute(
                                                    "INSERT INTO transfer_operation (operation_event_id, caller, recipient, amount) VALUES ($1, $2, $3, $4)",
                                                    operation_event_db_id, str(event.event.operation.operation.caller),
                                                    str(event.event.operation.operation.recipient),
                                                    str(event.event.operation.operation.amount))
                                            case Operation.Type.Evaluate:
                                                await conn.execute(
                                                    "INSERT INTO evaluate_operation (operation_event_id, function_id, function_type, caller, recipient, amount, record_payload) VALUES ($1, $2, $3, $4, $5, $6, $7)",
                                                    operation_event_db_id,
                                                    str(event.event.operation.operation.function_id),
                                                    event.event.operation.operation.function_type.name,
                                                    str(event.event.operation.operation.function_inputs.caller),
                                                    str(event.event.operation.operation.function_inputs.recipient),
                                                    event.event.operation.operation.function_inputs.amount,
                                                    event.event.operation.operation.function_inputs.record_payload.dump())

                            for i, ct in enumerate(transition.ciphertexts):
                                ciphertext_db_id = await conn.fetchval(
                                    "INSERT INTO ciphertext (transition_id, index, ciphertext) VALUES ($1, $2, $3) RETURNING id",
                                    transition_db_id, i, str(ct))
                                event: Event
                                for event in transition.events:
                                    if event.type == Event.Type.RecordViewKey:
                                        rvk_event: RecordViewKeyEvent = event.event
                                        if rvk_event.index == i:
                                            record = Record.load(bytearray(
                                                aleo.get_record(event.event.record_view_key.dump(), ct.dump())))
                                            event_db_id = await conn.fetchval(
                                                "SELECT event.id FROM event JOIN record_view_key_event e ON e.event_id = event.id WHERE transition_id = $1 AND event_type = 'RecordViewKey' AND e.index = $2",
                                                transition_db_id, i)
                                            if event_db_id is None:
                                                raise ValueError("inconsistent database state")
                                            await conn.execute(
                                                "INSERT INTO record (output_transition_id, record_view_key_event_id, ciphertext_id, owner, value, payload, program_id, randomizer, commitment) "
                                                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
                                                transition_db_id, event_db_id, ciphertext_db_id, str(record.owner),
                                                record.value, record.payload.dump(), str(record.program_id),
                                                str(record.randomizer), str(record.commitment))
                    await self.explorer_message(Message(Message.Type.DatabaseBlockAdded, block.header.metadata.height))
                except Exception as e:
                    await self.explorer_message(Message(Message.Type.DatabaseError, e))
                    raise

    async def save_canonical_block(self, block: Block):
        await self._save_block(block, True)

    async def save_non_canonical_block(self, block: Block):
        await self._save_block(block, False)

    @staticmethod
    def _get_block_header(block: dict):
        if block["height"] < 100000:
            proof = DeprecatedPoSWProof.loads(block['proof'])
        else:
            proof = PoSWProof.loads(block['proof'])
        return BlockHeader(
            previous_ledger_root=LedgerRoot.loads(block['previous_ledger_root']),
            transactions_root=TransactionsRoot.loads(block['transactions_root']),
            metadata=BlockHeaderMetadata(
                height=u32(block['height']),
                timestamp=i64(block['timestamp']),
                difficulty_target=u64(block['difficulty_target']),
                cumulative_weight=u128(block['cumulative_weight'])
            ),
            nonce=PoSWNonce.loads(block['nonce']),
            proof=proof
        )

    @staticmethod
    async def _get_full_block(block: dict, conn: asyncpg.Connection):
        transactions = await conn.fetch("SELECT * FROM transaction WHERE block_id = $1", block['id'])
        txs = []
        for transaction in transactions:
            transitions = await conn.fetch("SELECT * FROM transition WHERE transaction_id = $1",
                                           transaction['id'])
            tss = []
            for transition in transitions:
                serial_numbers = await conn.fetch("SELECT * FROM serial_number WHERE transition_id = $1 ORDER BY index",
                                                  transition['id'])
                sns = []
                for serial_number in serial_numbers:
                    sns.append(SerialNumber.loads(serial_number['serial_number']))
                ciphertexts = await conn.fetch("SELECT * FROM ciphertext WHERE transition_id = $1 ORDER BY index",
                                               transition['id'])
                cts = []
                for ciphertext in ciphertexts:
                    cts.append(RecordCiphertext.loads(ciphertext['ciphertext']))
                events = await conn.fetch("SELECT * FROM event WHERE transition_id = $1 ORDER BY index",
                                          transition['id'])
                es = []
                for event in events:
                    # noinspection PyUnresolvedReferences
                    match Event.Type[event['event_type']]:
                        case Event.Type.Custom:
                            custom = await conn.fetchrow("SELECT * FROM custom_event WHERE event_id = $1", event['id'])
                            es.append(Event(type_=Event.Type.Custom, event=CustomEvent(bytes_=custom['bytes'])))
                        case Event.Type.RecordViewKey:
                            rvk = await conn.fetchrow("SELECT * FROM record_view_key_event WHERE event_id = $1",
                                                      event['id'])
                            es.append(Event(type_=Event.Type.RecordViewKey, event=RecordViewKeyEvent(
                                record_view_key=RecordViewKey.loads(rvk['record_view_key']), index=u8(rvk['index']))))
                        case Event.Type.Operation:
                            op = await conn.fetchrow("SELECT * FROM operation_event WHERE event_id = $1", event['id'])
                            # noinspection PyUnresolvedReferences
                            match Operation.Type[op['operation_type']]:
                                case Operation.Type.Noop:
                                    operation = Operation(type_=Operation.Type.Noop, operation=NoopOperation())
                                case Operation.Type.Coinbase:
                                    coinbase = await conn.fetchrow(
                                        "SELECT * FROM coinbase_operation WHERE operation_event_id = $1", op['id'])
                                    operation = Operation(type_=Operation.Type.Coinbase, operation=CoinbaseOperation(
                                        recipient=Address.loads(coinbase['recipient']),
                                        amount=AleoAmount(coinbase['amount'])))
                                case Operation.Type.Transfer:
                                    transfer = await conn.fetchrow(
                                        "SELECT * FROM transfer_operation WHERE operation_event_id = $1", op['id'])
                                    operation = Operation(type_=Operation.Type.Transfer, operation=TransferOperation(
                                        caller=Address.loads(transfer["caller"]),
                                        recipient=Address.loads(transfer['recipient']),
                                        amount=AleoAmount(transfer['amount'])))
                                case Operation.Type.Evaluate:
                                    evaluate = await conn.fetchrow(
                                        "SELECT * FROM evaluate_operation WHERE operation_event_id = $1", op['id'])
                                    # noinspection PyUnresolvedReferences
                                    operation = Operation(
                                        type_=Operation.Type.Evaluate,
                                        operation=EvaluateOperation(
                                            function_id=FunctionID.loads(evaluate['function_id']),
                                            function_type=FunctionType[evaluate['function_type']],
                                            function_inputs=FunctionInputs(
                                                caller=Address.loads(evaluate['caller']),
                                                recipient=Address.loads(evaluate['recipient']),
                                                amount=AleoAmount(evaluate['amount']),
                                                record_payload=Payload.load(bytearray(evaluate['record_payload'])),
                                            )
                                        )
                                    )
                                case _:
                                    raise ValueError("invalid operation type")
                            es.append(Event(type_=Event.Type.Operation, event=OperationEvent(operation=operation)))
                        case _:
                            raise ValueError("invalid event type")
                tss.append(Transition(
                    transition_id=TransitionID.loads(transition['transition_id']),
                    serial_numbers=Vec[SerialNumber, 2](sns),
                    ciphertexts=Vec[RecordCiphertext, 2](cts),
                    value_balance=AleoAmount(transition['value_balance']),
                    events=Vec[Event, u16](es),
                    proof=OuterProof.loads(transition['proof'])
                ))
            txs.append(Transaction(
                inner_circuit_id=InnerCircuitID.loads(transaction['inner_circuit_id']),
                ledger_root=LedgerRoot.loads(transaction['ledger_root']),
                transitions=Vec[Transition, u16](tss)
            ))
        return Block(
            block_hash=BlockHash.loads(block['block_hash']),
            previous_block_hash=BlockHash.loads(block['previous_block_hash']),
            header=Database._get_block_header(block),
            transactions=Transactions(
                transactions=Vec[Transaction, u16](txs)
            )
        )

    async def get_latest_block(self):
        conn: asyncpg.Connection
        async with self.pool.acquire() as conn:
            try:
                block = await conn.fetchrow(
                    "SELECT * FROM block WHERE is_canonical = true ORDER BY height DESC LIMIT 1")
                if block is None:
                    return None
                return await self._get_full_block(block, conn)
            except Exception as e:
                await self.explorer_message(Message(Message.Type.DatabaseError, e))
                raise

    async def get_block_by_height(self, height: u32):
        conn: asyncpg.Connection
        async with self.pool.acquire() as conn:
            try:
                block = await conn.fetchrow(
                    "SELECT * FROM block WHERE is_canonical = true AND height = $1", height)
                if block is None:
                    return None
                return await self._get_full_block(block, conn)
            except Exception as e:
                await self.explorer_message(Message(Message.Type.DatabaseError, e))
                raise

    async def get_block_hash_by_height(self, height: u32):
        conn: asyncpg.Connection
        async with self.pool.acquire() as conn:
            try:
                block = await conn.fetchrow(
                    "SELECT * FROM block WHERE is_canonical = true AND height = $1", height)
                if block is None:
                    return None
                return BlockHash.loads(block['block_hash'])
            except Exception as e:
                await self.explorer_message(Message(Message.Type.DatabaseError, e))
                raise

    async def get_block_header_by_height(self, height: u32):
        conn: asyncpg.Connection
        async with self.pool.acquire() as conn:
            try:
                block = await conn.fetchrow(
                    "SELECT * FROM block WHERE is_canonical = true AND height = $1", height)
                if block is None:
                    return None
                return self._get_block_header(block)
            except Exception as e:
                await self.explorer_message(Message(Message.Type.DatabaseError, e))
                raise
