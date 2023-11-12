from __future__ import annotations

import os
import time
from collections import defaultdict
from typing import Awaitable, ParamSpec, cast

import psycopg
import psycopg.sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool
from redis.asyncio import Redis

from aleo_types import *
from disasm.utils import value_type_to_mode_type_str, plaintext_type_to_str
from explorer.types import Message as ExplorerMessage
from util.global_cache import global_mapping_cache

try:
    from line_profiler import profile
except ImportError:
    P = ParamSpec('P')
    R = TypeVar('R')
    def profile(func: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            return await func(*args, **kwargs)
        return wrapper


class Database:

    def __init__(self, *, server: str, user: str, password: str, database: str, schema: str,
                 redis_server: str, redis_port: int, redis_db: int,
                 message_callback: Callable[[ExplorerMessage], Awaitable[None]]):
        self.server = server
        self.user = user
        self.password = password
        self.database = database
        self.schema = schema
        self.message_callback = message_callback
        self.redis_server = redis_server
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.pool: AsyncConnectionPool
        self.redis: Redis[str]

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
            # noinspection PyArgumentList
            self.redis = Redis(host=self.redis_server, port=self.redis_port, db=self.redis_db, decode_responses=True) # type: ignore
        except Exception as e:
            await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseConnectError, e))
            return
        await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseConnected, None))

    @staticmethod
    def _get_addresses_from_struct(plaintext: StructPlaintext):
        addresses: set[str] = set()
        for _, p in plaintext.members:
            if isinstance(p, LiteralPlaintext) and p.literal.type == Literal.Type.Address:
                addresses.add(str(p.literal.primitive))
            elif isinstance(p, StructPlaintext):
                addresses.update(Database._get_addresses_from_struct(p))
        return addresses

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
                        addresses = Database._get_addresses_from_struct(plaintext)
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
                    await Database._insert_future(conn, argument.future, argument_db_id=argument_db_id)
                else:
                    raise NotImplementedError

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
                        future=await Database._load_future(conn, None, res["id"]) # type: ignore
                    ))
                else:
                    raise NotImplementedError
            return Future(
                program_id=ProgramID.loads(program_id),
                function_name=Identifier.loads(function_name),
                arguments=Vec[Argument, u8](arguments)
            )

    @staticmethod
    def _get_primitive_from_argument_unchecked(argument: Argument):
        plaintext = cast(PlaintextArgument, cast(PlaintextArgument, argument).plaintext)
        literal = cast(LiteralPlaintext, plaintext).literal
        return literal.primitive

    @staticmethod
    async def _insert_transition(conn: psycopg.AsyncConnection[dict[str, Any]], redis_conn: Redis[str],
                                 exe_tx_db_id: Optional[int], fee_db_id: Optional[int],
                                 transition: Transition, ts_index: int):
        async with conn.cursor() as cur:
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
                            addresses = Database._get_addresses_from_struct(plaintext)
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
                        await Database._insert_future(conn, transition_output.future.value, transition_output_future_db_id)
                else:
                    raise NotImplementedError

            await cur.execute(
                "SELECT id FROM program WHERE program_id = %s", (str(transition.program_id),)
            )
            if (res := await cur.fetchone()) is None:
                raise RuntimeError("failed to insert row into database")
            program_db_id = res["id"]
            await cur.execute(
                "UPDATE program_function SET called = called + 1 WHERE program_id = %s AND name = %s",
                (program_db_id, str(transition.function_name))
            )

            if transition.program_id == "credits.aleo":
                transfer_from = None
                transfer_to = None
                fee_from = None
                if transition.function_name == "transfer_public":
                    output = cast(FutureTransitionOutput, transition.outputs[0])
                    future = cast(Future, output.future.value)
                    transfer_from = str(Database._get_primitive_from_argument_unchecked(future.arguments[0]))
                    transfer_to = str(Database._get_primitive_from_argument_unchecked(future.arguments[1]))
                    amount = int(cast(int, Database._get_primitive_from_argument_unchecked(future.arguments[2])))
                elif transition.function_name == "transfer_private_to_public":
                    output = cast(FutureTransitionOutput, transition.outputs[1])
                    future = cast(Future, output.future.value)
                    transfer_to = str(Database._get_primitive_from_argument_unchecked(future.arguments[0]))
                    amount = int(cast(int, Database._get_primitive_from_argument_unchecked(future.arguments[1])))
                elif transition.function_name == "transfer_public_to_private":
                    output = cast(FutureTransitionOutput, transition.outputs[1])
                    future = cast(Future, output.future.value)
                    transfer_from = str(Database._get_primitive_from_argument_unchecked(future.arguments[0]))
                    amount = int(cast(int, Database._get_primitive_from_argument_unchecked(future.arguments[1])))
                elif transition.function_name == "fee_public":
                    output = cast(FutureTransitionOutput, transition.outputs[0])
                    future = cast(Future, output.future.value)
                    fee_from = str(Database._get_primitive_from_argument_unchecked(future.arguments[0]))
                    amount = int(cast(int, Database._get_primitive_from_argument_unchecked(future.arguments[1])))

                if transfer_from != transfer_to:
                    if transfer_from is not None:
                        await redis_conn.hincrby("address_transfer_out", transfer_from, amount) # type: ignore
                    if transfer_to is not None:
                        await redis_conn.hincrby("address_transfer_in", transfer_to, amount) # type: ignore

                if fee_from is not None:
                    await redis_conn.hincrby("address_fee", fee_from, amount) # type: ignore



    async def save_builtin_program(self, program: Program):
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                await self._save_program(cur, program, None, None)

    # noinspection PyMethodMayBeStatic
    async def _save_program(self, cur: psycopg.AsyncCursor[dict[str, Any]], program: Program,
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
            value_id = Field.loads(aleo_explorer_rust.get_value_id(str(key_id), value.dump()))
            committee_mapping[str(key_id)] = {
                "key": key.dump().hex(),
                "value_id": str(value_id),
                "value": value.dump().hex(),
            }
            global_mapping_cache[committee_mapping_id][key_id] = {
                "key": key,
                "value_id": value_id,
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
            value_id = Field.loads(aleo_explorer_rust.get_value_id(str(key_id), value.dump()))
            bonded_mapping[str(key_id)] = {
                "key": key.dump().hex(),
                "value_id": str(value_id),
                "value": value.dump().hex(),
            }
            global_mapping_cache[bonded_mapping_id][key_id] = {
                "key": key,
                "value_id": value_id,
                "value": value,
            }
        await redis_conn.execute_command("MULTI")
        await redis_conn.delete("credits.aleo:bonded")
        await redis_conn.hset("credits.aleo:bonded", mapping={k: json.dumps(v) for k, v in bonded_mapping.items()})
        await redis_conn.execute_command("EXEC")
        await cur.execute(
            "INSERT INTO mapping_bonded_history (height, content) VALUES (%s, %s)",
            (height, Jsonb(bonded_mapping))
        )

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
        await Database._save_committee_history(cur, 0, committee)

        stakers: dict[Address, tuple[Address, u64]] = {}
        for validator, amount, _ in committee.members:
            stakers[validator] = validator, amount
        committee_members = {address: (amount, is_open) for address, amount, is_open in committee.members}
        await Database._update_committee_bonded_map(cur, self.redis, committee_members, stakers, 0)

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
                "value_id": value_id,
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

    @staticmethod
    async def _get_bonded_mapping(redis_conn: Redis[str]) -> dict[Address, tuple[Address, u64]]:
        data = await redis_conn.hgetall("credits.aleo:bonded")

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
        for ratification in ratifications:
            if isinstance(ratification, BlockRewardRatify):
                committee_members = await Database._get_committee_mapping(redis_conn)
                stakers = await Database._get_bonded_mapping(redis_conn)

                Database._check_committee_staker_match(committee_members, stakers)

                stakers, stake_rewards = Database._stake_rewards(committee_members, stakers, ratification.amount)
                committee_members = Database._next_committee_members(committee_members, stakers)

                for address, amount in stake_rewards.items():
                    await self.redis.hincrby("address_stake_reward", str(address), amount)

                await Database._update_committee_bonded_map(cur, self.redis, committee_members, stakers, height)
                await Database._save_committee_history(cur, height, Committee(
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
                await cur.execute(
                    "SELECT id FROM mapping m WHERE m.mapping_id = %s",
                    (str(account_mapping_id),)
                )
                data = await cur.fetchone()
                if data is None:
                    raise RuntimeError("missing current account data")
                mapping_db_id = data["id"]
                await cur.execute(
                    "SELECT key_id, value FROM mapping_value WHERE mapping_id = %s",
                    (mapping_db_id,)
                )
                data = await cur.fetchall()

                current_balances: dict[str, dict[str, Any]] = {}
                for d in data:
                    current_balances[str(d["key_id"])] = {
                        "value": d["value"],
                    }

                from interpreter.interpreter import global_mapping_cache

                operations: list[dict[str, Any]] = []
                for address, amount in address_puzzle_rewards.items():
                    key = LiteralPlaintext(literal=Literal(type_=Literal.Type.Address, primitive=Address.loads(address)))
                    key_id = Field.loads(cached_get_key_id("credits.aleo", "account", key.dump()))
                    if str(key_id) not in current_balances:
                        current_balance = u64()
                    else:
                        current_balance_data = current_balances[str(key_id)]
                        value = Value.load(BytesIO(current_balance_data["value"]))
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
                        "value_id": value_id,
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

                        dag_transmission_ids: tuple[dict[str, int], dict[str, int]] = {}, {}

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
                            for round_, certificates in subdag.subdag.items():
                                for index, certificate in enumerate(certificates):
                                    if round_ != certificate.batch_header.round:
                                        raise ValueError("invalid subdag round")
                                    if isinstance(certificate, BatchCertificate1):
                                        await cur.execute(
                                            "INSERT INTO dag_vertex (authority_id, round, batch_certificate_id, batch_id, "
                                            "author, timestamp, author_signature, index) "
                                            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id",
                                            (authority_db_id, round_, str(certificate.certificate_id), str(certificate.batch_header.batch_id),
                                             str(certificate.batch_header.author), certificate.batch_header.timestamp,
                                             str(certificate.batch_header.signature), index)
                                        )
                                    elif isinstance(certificate, BatchCertificate2):
                                        await cur.execute(
                                            "INSERT INTO dag_vertex (authority_id, round, batch_id, "
                                            "author, timestamp, author_signature, index) "
                                            "VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id",
                                            (authority_db_id, round_, str(certificate.batch_header.batch_id),
                                             str(certificate.batch_header.author), certificate.batch_header.timestamp,
                                             str(certificate.batch_header.signature), index)
                                        )
                                    if (res := await cur.fetchone()) is None:
                                        raise RuntimeError("failed to insert row into database")
                                    vertex_db_id = res["id"]

                                    if isinstance(certificate, BatchCertificate1):
                                        for sig_index, (signature, timestamp) in enumerate(certificate.signatures):
                                            await cur.execute(
                                                "INSERT INTO dag_vertex_signature (vertex_id, signature, timestamp, index) "
                                                "VALUES (%s, %s, %s, %s)",
                                                (vertex_db_id, str(signature), timestamp, sig_index)
                                            )
                                    elif isinstance(certificate, BatchCertificate2):
                                        for sig_index, signature in enumerate(certificate.signatures):
                                            await cur.execute(
                                                "INSERT INTO dag_vertex_signature (vertex_id, signature, index) "
                                                "VALUES (%s, %s, %s)",
                                                (vertex_db_id, str(signature), sig_index)
                                            )

                                    prev_cert_ids = certificate.batch_header.previous_certificate_ids
                                    await cur.execute(
                                        "SELECT v.id, batch_certificate_id FROM dag_vertex v "
                                        "JOIN UNNEST(%s::text[]) WITH ORDINALITY c(id, ord) ON v.batch_certificate_id = c.id "
                                        "ORDER BY ord",
                                        (list(map(str, prev_cert_ids)),)
                                    )
                                    res = await cur.fetchall()
                                    # temp allow
                                    # if len(res) != len(prev_cert_ids):
                                    #     raise RuntimeError("dag referenced unknown previous certificate")
                                    prev_vertex_db_ids = {x["batch_certificate_id"]: x["id"] for x in res}
                                    adj_copy_data: list[tuple[int, int, int]] = []
                                    for prev_index, prev_cert_id in enumerate(prev_cert_ids):
                                        if str(prev_cert_id) in prev_vertex_db_ids:
                                            adj_copy_data.append((vertex_db_id, prev_vertex_db_ids[str(prev_cert_id)], prev_index))
                                    async with cur.copy("COPY dag_vertex_adjacency (vertex_id, previous_vertex_id, index) FROM STDIN") as copy:
                                        for row in adj_copy_data:
                                            await copy.write_row(row)

                                    tid_copy_data: list[tuple[int, str, int, Optional[str], Optional[str]]] = []
                                    for tid_index, transmission_id in enumerate(certificate.batch_header.transmission_ids):
                                        if isinstance(transmission_id, SolutionTransmissionID):
                                            tid_copy_data.append((vertex_db_id, transmission_id.type.name, tid_index, str(transmission_id.id), None))
                                            dag_transmission_ids[0][str(transmission_id.id)] = vertex_db_id
                                        elif isinstance(transmission_id, TransactionTransmissionID):
                                            tid_copy_data.append((vertex_db_id, transmission_id.type.name, tid_index, None, str(transmission_id.id)))
                                            dag_transmission_ids[1][str(transmission_id.id)] = vertex_db_id
                                        elif isinstance(transmission_id, RatificationTransmissionID):
                                            tid_copy_data.append((vertex_db_id, transmission_id.type.name, tid_index, None, None))
                                        else:
                                            raise NotImplementedError
                                    async with cur.copy("COPY dag_vertex_transmission_id (vertex_id, type, index, commitment, transaction_id) FROM STDIN") as copy:
                                        for row in tid_copy_data:
                                            await copy.write_row(row)


                        for ct_index, confirmed_transaction in enumerate(block.transactions):
                            await cur.execute(
                                "INSERT INTO confirmed_transaction (block_id, index, type) VALUES (%s, %s, %s) RETURNING id",
                                (block_db_id, confirmed_transaction.index, confirmed_transaction.type.name)
                            )
                            if (res := await cur.fetchone()) is None:
                                raise RuntimeError("failed to insert row into database")
                            confirmed_transaction_db_id = res["id"]
                            transaction = confirmed_transaction.transaction
                            transaction_id = transaction.id
                            if block.height != 0 and isinstance(confirmed_transaction, (AcceptedDeploy, AcceptedExecute)):
                                dag_vertex_db_id = dag_transmission_ids[1][str(transaction_id)]
                            else:
                                dag_vertex_db_id = None
                            await cur.execute(
                                "INSERT INTO transaction (dag_vertex_id, confimed_transaction_id, transaction_id, type) "
                                "VALUES (%s, %s, %s, %s) RETURNING id",
                                (dag_vertex_db_id, confirmed_transaction_db_id, str(transaction_id), transaction.type.name)
                            )
                            if (res := await cur.fetchone()) is None:
                                raise RuntimeError("failed to insert row into database")
                            transaction_db_id = res["id"]
                            if isinstance(confirmed_transaction, AcceptedDeploy):
                                if reject_reasons[ct_index] is not None:
                                    raise RuntimeError("expected no rejected reason for accepted deploy transaction")
                                if not isinstance(transaction, DeployTransaction):
                                    raise ValueError("expected deploy transaction")
                                await cur.execute(
                                    "INSERT INTO transaction_deploy (transaction_id, edition, verifying_keys) "
                                    "VALUES (%s, %s, %s) RETURNING id",
                                    (transaction_db_id, transaction.deployment.edition, transaction.deployment.verifying_keys.dump())
                                )
                                if (res := await cur.fetchone()) is None:
                                    raise RuntimeError("failed to insert row into database")
                                deploy_transaction_db_id = res["id"]


                                # TODO: remove bug workaround
                                from node.testnet3 import Testnet3
                                if str(confirmed_transaction.transaction.id) not in Testnet3.ignore_deploy_txids:
                                    await self._save_program(cur, transaction.deployment.program, deploy_transaction_db_id, transaction)

                                await cur.execute(
                                    "INSERT INTO fee (transaction_id, global_state_root, proof) "
                                    "VALUES (%s, %s, %s) RETURNING id",
                                    (transaction_db_id, str(transaction.fee.global_state_root), transaction.fee.proof.dumps())
                                )
                                if (res := await cur.fetchone()) is None:
                                    raise RuntimeError("failed to insert row into database")
                                fee_db_id = res["id"]
                                await self._insert_transition(conn, self.redis, None, fee_db_id, transaction.fee.transition, 0)

                            elif isinstance(confirmed_transaction, RejectedDeploy):
                                if reject_reasons[ct_index] is None:
                                    raise RuntimeError("expected a rejected reason for rejected deploy transaction")
                                await cur.execute("UPDATE confirmed_transaction SET reject_reason = %s WHERE id = %s",
                                            (reject_reasons[ct_index], confirmed_transaction_db_id))
                                if not isinstance(transaction, FeeTransaction):
                                    raise ValueError("expected fee transaction")
                                fee = transaction.fee
                                await cur.execute(
                                    "INSERT INTO fee (transaction_id, global_state_root, proof) "
                                    "VALUES (%s, %s, %s) RETURNING id",
                                    (transaction_db_id, str(fee.global_state_root), fee.proof.dumps())
                                )
                                if (res := await cur.fetchone()) is None:
                                    raise RuntimeError("failed to insert row into database")
                                fee_db_id = res["id"]
                                await self._insert_transition(conn, self.redis, None, fee_db_id, fee.transition, 0)
                                rejected = confirmed_transaction.rejected
                                if not isinstance(rejected, RejectedDeployment):
                                    raise ValueError("expected rejected deployment")
                                await cur.execute(
                                    "INSERT INTO transaction_deploy (transaction_id, edition, verifying_keys) "
                                    "VALUES (%s, %s, %s)",
                                    (transaction_db_id, rejected.deploy.edition, rejected.deploy.verifying_keys.dump())
                                )
                                # TODO: consider saving rejected programs as well

                            elif isinstance(confirmed_transaction, AcceptedExecute):
                                if reject_reasons[ct_index] is not None:
                                    raise RuntimeError("expected no rejected reason for accepted execute transaction")
                                if not isinstance(transaction, ExecuteTransaction):
                                    raise ValueError("expected execute transaction")
                                await cur.execute(
                                    "INSERT INTO transaction_execute (transaction_id, global_state_root, proof) "
                                    "VALUES (%s, %s, %s) RETURNING id",
                                    (transaction_db_id, str(transaction.execution.global_state_root),
                                     transaction.execution.proof.dumps())
                                )
                                if (res := await cur.fetchone()) is None:
                                    raise RuntimeError("failed to insert row into database")
                                execute_transaction_db_id = res["id"]

                                for ts_index, transition in enumerate(transaction.execution.transitions):
                                    await self._insert_transition(conn, self.redis, execute_transaction_db_id, None, transition, ts_index)

                                if transaction.additional_fee.value is not None:
                                    fee = transaction.additional_fee.value
                                    await cur.execute(
                                        "INSERT INTO fee (transaction_id, global_state_root, proof) "
                                        "VALUES (%s, %s, %s) RETURNING id",
                                        (transaction_db_id, str(fee.global_state_root), fee.proof.dumps())
                                    )
                                    if (res := await cur.fetchone()) is None:
                                        raise RuntimeError("failed to insert row into database")
                                    fee_db_id = res["id"]
                                    await self._insert_transition(conn, self.redis, None, fee_db_id, fee.transition, 0)

                            elif isinstance(confirmed_transaction, RejectedExecute):
                                if reject_reasons[ct_index] is None:
                                    raise RuntimeError("expected a rejected reason for rejected execute transaction")
                                await cur.execute("UPDATE confirmed_transaction SET reject_reason = %s WHERE id = %s",
                                            (reject_reasons[ct_index], confirmed_transaction_db_id))
                                if not isinstance(transaction, FeeTransaction):
                                    raise ValueError("expected fee transaction")
                                fee = transaction.fee
                                await cur.execute(
                                    "INSERT INTO fee (transaction_id, global_state_root, proof) "
                                    "VALUES (%s, %s, %s) RETURNING id",
                                    (transaction_db_id, str(fee.global_state_root), fee.proof.dumps())
                                )
                                if (res := await cur.fetchone()) is None:
                                    raise RuntimeError("failed to insert row into database")
                                fee_db_id = res["id"]
                                await self._insert_transition(conn, self.redis, None, fee_db_id, fee.transition, 0)

                                rejected = confirmed_transaction.rejected
                                if not isinstance(rejected, RejectedExecution):
                                    raise ValueError("expected rejected execution")
                                await cur.execute(
                                    "INSERT INTO transaction_execute (transaction_id, global_state_root, proof) "
                                    "VALUES (%s, %s, %s) RETURNING id",
                                    (transaction_db_id, str(rejected.execution.global_state_root),
                                     rejected.execution.proof.dumps())
                                )
                                if (res := await cur.fetchone()) is None:
                                    raise RuntimeError("failed to insert row into database")
                                execute_transaction_db_id = res["id"]
                                for ts_index, transition in enumerate(rejected.execution.transitions):
                                    await self._insert_transition(conn, self.redis, execute_transaction_db_id, None, transition, ts_index)

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
                            copy_data: list[tuple[int, int, str, u64, str, int, int, str, bool]] = []
                            for prover_solution, target, reward in solutions:
                                partial_solution = prover_solution.partial_solution
                                dag_vertex_db_id = dag_transmission_ids[0][str(partial_solution.commitment)]
                                copy_data.append(
                                    (dag_vertex_db_id, coinbase_solution_db_id, str(partial_solution.address), partial_solution.nonce,
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

                        await self._redis_cleanup(self.redis, redis_keys, block.height, False)

                        await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseBlockAdded, block.header.metadata.height))
                    except Exception as e:
                        await self._redis_cleanup(self.redis, redis_keys, block.height, True)
                        await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                        raise

    async def save_block(self, block: Block):
        await self._save_block(block)

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
                    future = await Database._load_future(conn, transition_output["future_id"], None)
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
                            transition=await Database._get_transition(fee_transition, conn),
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
                            tss.append(await Database._get_transition(transition, conn))
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
                                transition=await Database._get_transition(fee_transition, conn),
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
                    await cur.execute(
                        "SELECT * FROM dag_vertex_signature WHERE vertex_id = %s ORDER BY index",
                        (dag_vertex["id"],)
                    )
                    dag_vertex_signatures = await cur.fetchall()

                    if dag_vertex["batch_certificate_id"] is not None:
                        signatures: list[Tuple[Signature, i64]] = []
                        for signature in dag_vertex_signatures:
                            signatures.append(
                                Tuple[Signature, i64]((
                                    Signature.loads(signature["signature"]),
                                    i64(signature["timestamp"]),
                                ))
                            )
                    else:
                        signatures: list[Signature] = []
                        for signature in dag_vertex_signatures:
                            signatures.append(Signature.loads(signature["signature"]))

                    await cur.execute(
                        "SELECT previous_vertex_id FROM dag_vertex_adjacency WHERE vertex_id = %s ORDER BY index",
                        (dag_vertex["id"],)
                    )
                    previous_ids = [x["previous_vertex_id"] for x in await cur.fetchall()]

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
                                batch_header=BatchHeader(
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
                                batch_header=BatchHeader(
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
                subdag = Subdag(
                    subdag=subdags
                )
                auth = QuorumAuthority(subdag=subdag)
            else:
                raise NotImplementedError

            return Block(
                block_hash=BlockHash.loads(block['block_hash']),
                previous_hash=BlockHash.loads(block['previous_hash']),
                header=Database._get_block_header(block),
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
            return [await Database._get_full_block(block, conn) for block in blocks]

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
            return [await Database._get_fast_block(block, conn) for block in blocks]

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
                return await Database._get_fast_block_range(latest_height, latest_height - 30, conn)
            except Exception as e:
                await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                raise

    # noinspection PyUnusedLocal
    async def get_validator_from_block_hash(self, block_hash: BlockHash) -> Address | None:
        raise NotImplementedError
        # noinspection PyUnreachableCode
        async with self.pool.connection() as conn:
            try:
                # noinspection PyUnresolvedReferences,SqlResolve
                return await conn.fetchval(
                    "SELECT owner "
                    "FROM record r "
                    "JOIN transition ts ON r.output_transition_id = ts.id "
                    "JOIN transaction tx ON ts.transaction_id = tx.id "
                    "JOIN block b ON tx.block_id = b.id "
                    "WHERE ts.value_balance < 0 AND r.value > 0 AND b.block_hash = %s",
                    str(block_hash)
                )
            except Exception as e:
                await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                raise

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
                    return await Database._get_transition(transition, conn)
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def search_block_hash(self, block_hash: str) -> list[str]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("SELECT block_hash FROM block WHERE block_hash LIKE %s", (f"{block_hash}%",))
                    result = await cur.fetchall()
                    return list(map(lambda x: x['block_hash'], result))
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def search_transaction_id(self, transaction_id: str) -> list[str]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("SELECT transaction_id FROM transaction WHERE transaction_id LIKE %s", (f"{transaction_id}%",))
                    result = await cur.fetchall()
                    return list(map(lambda x: x['transaction_id'], result))
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def search_transition_id(self, transition_id: str) -> list[str]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("SELECT transition_id FROM transition WHERE transition_id LIKE %s", (f"{transition_id}%",))
                    result = await cur.fetchall()
                    return list(map(lambda x: x['transition_id'], result))
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_blocks_range(self, start: int, end: int):
        async with self.pool.connection() as conn:
            try:
                return await Database._get_full_block_range(start, end, conn)
            except Exception as e:
                await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                raise

    async def get_blocks_range_fast(self, start: int, end: int):
        async with self.pool.connection() as conn:
            try:
                return await Database._get_fast_block_range(start, end, conn)
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

    async def get_leaderboard_size(self) -> int:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("SELECT COUNT(*) FROM leaderboard")
                    if (res := await cur.fetchone()) is None:
                        return 0
                    return res["count"]
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_leaderboard(self, start: int, end: int) -> list[dict[str, Any]]:
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

    async def get_leaderboard_rewards_by_address(self, address: str) -> tuple[int, int]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT total_reward, total_incentive FROM leaderboard WHERE address = %s", (address,)
                    )
                    row = await cur.fetchone()
                    if row is None:
                        return 0, 0
                    return row["total_reward"], row["total_incentive"]
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_recent_solutions_by_address(self, address: str) -> list[dict[str, Any]]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT b.height, b.timestamp, ps.nonce, ps.target, reward, cs.target_sum "
                        "FROM prover_solution ps "
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
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT COUNT(*) FROM prover_solution WHERE address = %s", (address,)
                    )
                    if (res := await cur.fetchone()) is None:
                        return 0
                    return res["count"]
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_solution_by_address(self, address: str, start: int, end: int) -> list[dict[str, Any]]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT b.height, b.timestamp, ps.nonce, ps.target, reward, cs.target_sum "
                        "FROM prover_solution ps "
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

    async def get_solution_by_height(self, height: int, start: int, end: int) -> list[dict[str, Any]]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT ps.address, ps.nonce, ps.commitment, ps.target, reward "
                        "FROM prover_solution ps "
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

    async def search_address(self, address: str) -> list[str]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT DISTINCT address FROM leaderboard WHERE address LIKE %s", (f"{address}%",)
                    )
                    res = set(map(lambda x: x['address'], await cur.fetchall()))
                    await cur.execute(
                        "SELECT DISTINCT owner FROM program WHERE owner LIKE %s", (f"{address}%",)
                    )
                    res.update(set(map(lambda x: x['owner'], await cur.fetchall())))
                    await cur.execute(
                        "SELECT DISTINCT address FROM address_transition WHERE address LIKE %s", (f"{address}%",)
                    )
                    res.update(set(map(lambda x: x['address'], await cur.fetchall())))
                    return list(res)
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_address_recent_transitions(self, address: str) -> list[dict[str, Any]]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT DISTINCT t.transition_id, b.height, b.timestamp FROM address_transition at "
                        "JOIN transition t on at.transition_id = t.id "
                        "JOIN transaction_execute te on te.id = t.transaction_execute_id "
                        "JOIN transaction t2 on t2.id = te.transaction_id "
                        "JOIN confirmed_transaction ct on ct.id = t2.confimed_transaction_id "
                        "JOIN block b on b.id = ct.block_id "
                        "WHERE at.address = %s ORDER BY b.height DESC LIMIT 30",
                        (address,)
                    )
                    def transform(x: dict[str, Any]):
                        return {
                            "transition_id": x["transition_id"],
                            "height": x["height"],
                            "timestamp": x["timestamp"]
                        }
                    return list(map(lambda x: transform(x), await cur.fetchall()))
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_address_stake_reward(self, address: str) -> Optional[int]:
        data = await self.redis.hget("address_stake_reward", address)
        if data is None:
            return None
        return int(data)

    async def get_address_transfer_in(self, address: str) -> Optional[int]:
        data = await self.redis.hget("address_transfer_in", address)
        if data is None:
            return None
        return int(data)

    async def get_address_transfer_out(self, address: str) -> Optional[int]:
        data = await self.redis.hget("address_transfer_out", address)
        if data is None:
            return None
        return int(data)

    async def get_address_total_fee(self, address: str) -> Optional[int]:
        data = await self.redis.hget("address_fee", address)
        if data is None:
            return None
        return int(data)

    async def get_address_speed(self, address: str) -> tuple[float, int]: # (speed, interval)
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                interval_list = [900, 1800, 3600, 14400, 43200, 86400]
                now = int(time.time())
                try:
                    for interval in interval_list:
                        await cur.execute(
                            "SELECT b.height FROM prover_solution ps "
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
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                now = int(time.time())
                interval = 900
                try:
                    await cur.execute(
                        "SELECT b.height FROM prover_solution ps "
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
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("SELECT total_credit FROM leaderboard_total")
                    total_credit = await cur.fetchone()
                    if total_credit is None:
                        await cur.execute("INSERT INTO leaderboard_total (total_credit) VALUES (0)")
                        return 0
                    return int(total_credit["total_credit"])
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_puzzle_commitment(self, commitment: str) -> Optional[dict[str, Any]]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT reward, height FROM prover_solution "
                        "JOIN coinbase_solution cs on cs.id = prover_solution.coinbase_solution_id "
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

    async def get_function_definition(self, program_id: str, function_name: str) -> Optional[dict[str, Any]]:
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
                    if (res := await cur.fetchone()) is None:
                        return 0
                    return res['count']
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_programs(self, start: int, end: int, no_helloworld: bool = False) -> list[dict[str, Any]]:
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

    async def get_programs_with_feature_hash(self, feature_hash: bytes, start: int, end: int) -> list[dict[str, Any]]:
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

    async def get_deploy_info_by_program_id(self, program_id: str) -> dict[str, Any] | None:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT b.height, b.timestamp, t.transaction_id FROM block b "
                        "JOIN confirmed_transaction ct on b.id = ct.block_id "
                        "JOIN transaction t on ct.id = t.confimed_transaction_id "
                        "JOIN transaction_deploy td on t.id = td.transaction_id "
                        "JOIN program p on td.id = p.transaction_deploy_id "
                        "WHERE p.program_id = %s",
                        (program_id,)
                    )
                    data = await cur.fetchone()
                    if data is None:
                        return None
                    return data
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise


    async def get_program_called_times(self, program_id: str) -> int:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT sum(called) FROM program_function "
                        "JOIN program ON program.id = program_function.program_id "
                        "WHERE program.program_id = %s",
                        (program_id,)
                    )
                    if (res := await cur.fetchone()) is None:
                        return 0
                    return res['sum']
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise


    async def get_program_calls(self, program_id: str, start: int, end: int) -> list[dict[str, Any]]:
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
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT COUNT(*) FROM program "
                        "WHERE feature_hash = (SELECT feature_hash FROM program WHERE program_id = %s)",
                        (program_id,)
                    )
                    if (res := await cur.fetchone()) is None:
                        raise ValueError(f"Program {program_id} not found")
                    return res['count'] - 1
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_program_feature_hash(self, program_id: str) -> Optional[bytes]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT feature_hash FROM program WHERE program_id = %s",
                        (program_id,)
                    )
                    if (res := await cur.fetchone()) is None:
                        return None
                    return res['feature_hash']
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def search_program(self, program_id: str) -> list[str]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT program_id FROM program WHERE program_id LIKE %s", (f"{program_id}%",)
                    )
                    return list(map(lambda x: x['program_id'], await cur.fetchall()))
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_recent_programs_by_address(self, address: str) -> list[str]:
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
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("SELECT COUNT(*) FROM program WHERE owner = %s", (address,))
                    if (res := await cur.fetchone()) is None:
                        return 0
                    return res['count']
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_program(self, program_id: str) -> Optional[bytes]:
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

    async def get_mapping_cache_with_cur(self, cur: psycopg.AsyncCursor[dict[str, Any]], program_name: str,
                                         mapping_name: str) -> dict[Field, Any]:
        if program_name == "credits.aleo" and mapping_name in ["committee", "bonded"]:
            def transform(d: dict[str, Any]):
                return {
                    "value_id": Field.loads(d["value_id"]),
                    "key": Plaintext.load(BytesIO(bytes.fromhex(d["key"]))),
                    "value": Value.load(BytesIO(bytes.fromhex(d["value"]))),
                }
            data = await self.redis.hgetall(f"{program_name}:{mapping_name}")
            return {Field.loads(k): transform(json.loads(v)) for k, v in data.items()}
        else:
            mapping_id = Field.loads(cached_get_mapping_id(program_name, mapping_name))
            try:
                await cur.execute(
                    "SELECT key_id, value_id, key, value FROM mapping_value mv "
                    "JOIN mapping m on mv.mapping_id = m.id "
                    "WHERE m.mapping_id = %s ",
                    (str(mapping_id),)
                )
                data = await cur.fetchall()
                def transform(d: dict[str, Any]):
                    return {
                        "value_id": Field.loads(d["value_id"]),
                        "key": Plaintext.load(BytesIO(d["key"])),
                        "value": Value.load(BytesIO(d["value"])),
                    }
                return {Field.loads(x["key_id"]): transform(x) for x in data}
            except Exception as e:
                await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                raise

    async def get_mapping_cache(self, program_name: str, mapping_name: str) -> dict[Field, Any]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                return await self.get_mapping_cache_with_cur(cur, program_name, mapping_name)

    async def get_mapping_value(self, program_id: str, mapping: str, key_id: str) -> Optional[bytes]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    if program_id == "credits.aleo" and mapping in ["committee", "bonded"]:
                        conn = self.redis
                        data = await conn.hget(f"{program_id}:{mapping}", key_id)
                        if data is None:
                            return None
                        return bytes.fromhex(json.loads(data)["value"])
                    else:
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

    async def get_mapping_size(self, program_id: str, mapping: str) -> int:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT COUNT(*) FROM mapping_value mv "
                        "JOIN mapping m on mv.mapping_id = m.id "
                        "WHERE m.program_id = %s AND m.mapping = %s",
                        (program_id, mapping)
                    )
                    if (res := await cur.fetchone()) is None:
                        return 0
                    return res['count']
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def initialize_mapping(self, cur: psycopg.AsyncCursor[dict[str, Any]], mapping_id: str, program_id: str, mapping: str):
        try:
            await cur.execute(
                "INSERT INTO mapping (mapping_id, program_id, mapping) VALUES (%s, %s, %s)",
                (mapping_id, program_id, mapping)
            )
        except Exception as e:
            await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
            raise

    async def initialize_builtin_mapping(self, mapping_id: str, program_id: str, mapping: str):
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "INSERT INTO mapping (mapping_id, program_id, mapping) VALUES (%s, %s, %s) "
                        "ON CONFLICT DO NOTHING",
                        (mapping_id, program_id, mapping)
                    )
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def update_mapping_key_value(self, cur: psycopg.AsyncCursor[dict[str, Any]], program_name: str,
                                       mapping_name: str, mapping_id: str, key_id: str, value_id: str,
                                       key: bytes, value: bytes, height: int, from_transaction: bool):
        try:
            if program_name == "credits.aleo" and mapping_name in ["committee", "bonded"]:
                conn = self.redis
                data = {
                    "key": key.hex(),
                    "value_id": value_id,
                    "value": value.hex(),
                }
                await conn.hset(f"{program_name}:{mapping_name}", key_id, json.dumps(data))
            else:
                await cur.execute("SELECT id FROM mapping WHERE mapping_id = %s", (mapping_id,))
                mapping = await cur.fetchone()
                if mapping is None:
                    raise ValueError(f"mapping {mapping_id} not found")
                mapping_id = mapping['id']
                await cur.execute(
                    "INSERT INTO mapping_value (mapping_id, key_id, value_id, key, value) "
                    "VALUES (%s, %s, %s, %s, %s) "
                    "ON CONFLICT (mapping_id, key_id) DO UPDATE SET value_id = %s, value = %s",
                    (mapping_id, key_id, value_id, key, value, value_id, value)
                )

                await cur.execute(
                    "INSERT INTO mapping_history (mapping_id, height, key_id, key, value, from_transaction) "
                    "VALUES (%s, %s, %s, %s, %s, %s)",
                    (mapping_id, height, key_id, key, value, from_transaction)
                )

        except Exception as e:
            await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
            raise

    async def remove_mapping_key_value(self, cur: psycopg.AsyncCursor[dict[str, Any]], program_name: str,
                                       mapping_name: str, mapping_id: str, key_id: str, key: bytes, height: int,
                                       from_transaction: bool):
        try:
            if program_name == "credits.aleo" and mapping_name in ["committee", "bonded"]:
                conn = self.redis
                await conn.hdel(f"{program_name}:{mapping_name}", key_id)
            else:
                await cur.execute("SELECT id FROM mapping WHERE mapping_id = %s", (mapping_id,))
                mapping = await cur.fetchone()
                if mapping is None:
                    raise ValueError(f"mapping {mapping_id} not found")
                mapping_id = mapping['id']
                await cur.execute(
                    "DELETE FROM mapping_value WHERE mapping_id = %s AND key_id = %s",
                    (mapping_id, key_id)
                )

                await cur.execute(
                    "INSERT INTO mapping_history (mapping_id, height, key_id, key, value, from_transaction) "
                    "VALUES (%s, %s, %s, %s, NULL, %s)",
                    (mapping_id, height, key_id, key, from_transaction)
                )

        except Exception as e:
            await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
            raise

    async def get_finalize_operations_by_height(self, height: int) -> list[FinalizeOperation]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT f.id, f.type FROM finalize_operation f "
                        "JOIN confirmed_transaction ct on ct.id = f.confirmed_transaction_id "
                        "JOIN block b on b.id = ct.block_id "
                        "WHERE b.height = %s "
                        "ORDER BY f.id",
                        (height,)
                    )
                    data = await cur.fetchall()
                    result: list[FinalizeOperation] = []
                    for d in data:
                        if d["type"] == "UpdateKeyValue":
                            await cur.execute(
                                "SELECT mapping_id, key_id, value_id FROM finalize_operation_update_kv fu "
                                "JOIN explorer.finalize_operation fo on fo.id = fu.finalize_operation_id "
                                "WHERE fo.id = %s",
                                (d["id"],)
                            )
                            u = await cur.fetchone()
                            result.append(UpdateKeyValue(
                                mapping_id=Field.loads(u["mapping_id"]),
                                key_id=Field.loads(u["key_id"]),
                                value_id=Field.loads(u["value_id"]),
                                index=u64(),
                            ))
                        elif d["type"] == "RemoveKeyValue":
                            await cur.execute(
                                "SELECT mapping_id FROM finalize_operation_remove_kv fu "
                                "JOIN explorer.finalize_operation fo on fo.id = fu.finalize_operation_id "
                                "WHERE fo.id = %s",
                                (d["id"],)
                            )
                            u = await cur.fetchone()
                            result.append(RemoveKeyValue(
                                mapping_id=Field.loads(u["mapping_id"]),
                                index=u64()
                            ))
                    return result
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_transaction_mapping_history_by_height(self, height: int) -> list[dict[str, Any]]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT mh.id, m.program_id, m.mapping, m.mapping_id, mh.key_id, mh.key, mh.value FROM mapping_history mh "
                        "JOIN mapping m on mh.mapping_id = m.id "
                        "WHERE mh.height = %s AND mh.from_transaction = TRUE "
                        "ORDER BY mh.id",
                        (height,)
                    )
                    return await cur.fetchall()
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_mapping_history_previous_value(self, history_id: int, key_id: str) -> Optional[bytes]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT key, value FROM mapping_history WHERE id < %s AND key_id = %s ORDER BY id DESC LIMIT 1",
                        (history_id, key_id)
                    )
                    res = await cur.fetchone()
                    if res is None:
                        return None
                    return res['value']
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

                
    async def get_program_leo_source_code(self, program_id: str) -> Optional[str]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("SELECT leo_source FROM program WHERE program_id = %s", (program_id,))
                    if (res := await cur.fetchone()) is None:
                        return None
                    return res['leo_source']
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def store_program_leo_source_code(self, program_id: str, source_code: str):
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
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("INSERT INTO feedback (contact, content) VALUES (%s, %s)", (contact, content))
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise


    # migration methods
    async def migrate(self):
        migrations: list[tuple[int, Callable[[psycopg.AsyncConnection[dict[str, Any]]], Awaitable[None]]]] = [
            (1, self.migrate_1_add_dag_vertex_adjacency_index),
            (2, self.migrate_2_add_helper_functions),
            (3, self.migrate_3_set_mapping_history_key_not_null),
            (4, self.migrate_4_support_batch_certificate_v2),
        ]
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    for migrated_id, method in migrations:
                        await cur.execute("SELECT COUNT(*) FROM _migration WHERE migrated_id = %s", (migrated_id,))
                        res = await cur.fetchone()
                        if res is None or res['count'] == 0:
                            print(f"DB migrating {migrated_id}")
                            async with conn.transaction():
                                await method(conn)
                                await cur.execute("INSERT INTO _migration (migrated_id) VALUES (%s)", (migrated_id,))
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    @staticmethod
    async def migrate_1_add_dag_vertex_adjacency_index(conn: psycopg.AsyncConnection[dict[str, Any]]):
        await conn.execute("create index dag_vertex_adjacency_vertex_id_index on explorer.dag_vertex_adjacency (vertex_id)")

    @staticmethod
    async def migrate_2_add_helper_functions(conn: psycopg.AsyncConnection[dict[str, Any]]):
        await conn.execute(open("migration_2.sql").read())

    @staticmethod
    async def migrate_3_set_mapping_history_key_not_null(conn: psycopg.AsyncConnection[dict[str, Any]]):
        await conn.execute("alter table explorer.mapping_history alter column key set not null")

    @staticmethod
    async def migrate_4_support_batch_certificate_v2(conn: psycopg.AsyncConnection[dict[str, Any]]):
        await conn.execute("alter table explorer.dag_vertex alter column batch_certificate_id drop not null")
        await conn.execute("alter table explorer.dag_vertex_signature alter column timestamp drop not null")

    # debug method
    async def clear_database(self):
        async with self.pool.connection() as conn:
            try:
                await conn.execute("TRUNCATE TABLE block RESTART IDENTITY CASCADE")
                await conn.execute("TRUNCATE TABLE mapping RESTART IDENTITY CASCADE")
                await self.redis.flushall()
            except Exception as e:
                await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                raise