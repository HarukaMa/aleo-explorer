import copy
import functools
from collections import OrderedDict
from io import BytesIO
from typing import Any, cast, Optional, ParamSpec, TypeVar, Callable, Awaitable

import aleo_explorer_rust
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import RedirectResponse

import util.arc0137
from aleo_types import u32, Transition, ExecuteTransaction, PrivateTransitionInput, \
    RecordTransitionInput, TransitionOutput, RecordTransitionOutput, DeployTransaction, PublicTransitionInput, \
    PublicTransitionOutput, PrivateTransitionOutput, ExternalRecordTransitionInput, \
    ExternalRecordTransitionOutput, AcceptedDeploy, AcceptedExecute, RejectedExecute, \
    FeeTransaction, RejectedDeploy, RejectedExecution, Identifier, Entry, FutureTransitionOutput, Future, \
    PlaintextArgument, FutureArgument, StructPlaintext, Finalize, \
    PlaintextFinalizeType, StructPlaintextType, UpdateKeyValue, Value, Plaintext, RemoveKeyValue, FinalizeOperation, \
    NodeType, cached_get_mapping_id, cached_get_key_id, FeeComponent, Fee
from db import Database
from node.light_node import LightNodeState
from util.global_cache import get_program
from .classes import UIAddress
from .template import htmx_template
from .utils import function_signature, out_of_sync_check, function_definition, get_relative_time

try:
    from line_profiler import profile
except ImportError:
    P = ParamSpec('P')
    R = TypeVar('R')
    def profile(func: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            return await func(*args, **kwargs)
        return wrapper

DictList = list[dict[str, Any]]

@profile
@htmx_template("block.jinja2")
async def block_route(request: Request):
    db: Database = request.app.state.db
    height = request.query_params.get("h")
    block_hash = request.query_params.get("bh")
    if height is None and block_hash is None:
        raise HTTPException(status_code=400, detail="Missing height or block hash")
    if height is not None:
        block = await db.get_block_by_height(u32(int(height)))
        if block is None:
            raise HTTPException(status_code=404, detail="Block not found")
        block_hash = block.block_hash
    elif block_hash is not None:
        block = await db.get_block_by_hash(block_hash)
        if block is None:
            raise HTTPException(status_code=404, detail="Block not found")
        height = block.header.metadata.height
    else:
        raise RuntimeError("unreachable")
    height = int(height)

    coinbase_reward = await db.get_block_coinbase_reward_by_height(height)
    css: DictList = []
    target_sum = 0
    if coinbase_reward is not None:
        solutions = await db.get_solution_by_height(height, 0, 100)
        for solution in solutions:
            css.append({
                "address": solution["address"],
                "address_trunc": solution["address"][:15] + "..." + solution["address"][-10:],
                "counter": solution["counter"],
                "target": solution["target"],
                "reward": solution["reward"],
            })
            target_sum += solution["target"]

    txs: DictList = []
    total_base_fee = 0
    total_priority_fee = 0
    total_burnt_fee = 0
    for ct in block.transactions.transactions:
        # TODO: use proper fee calculation
        # fee_breakdown = await ct.get_fee_breakdown(db)
        fee = ct.transaction.fee
        if isinstance(fee, Fee):
            base_fee, priority_fee = fee.amount
        elif fee.value is not None:
            base_fee, priority_fee = fee.value.amount
        else:
            base_fee, priority_fee = 0, 0
        fee_breakdown = FeeComponent(base_fee, 0, [0], priority_fee, 0)
        print(fee_breakdown)
        base_fee = fee_breakdown.storage_cost + fee_breakdown.namespace_cost + sum(fee_breakdown.finalize_costs)
        priority_fee = fee_breakdown.priority_fee
        burnt_fee = fee_breakdown.burnt
        total_base_fee += base_fee
        total_priority_fee += priority_fee
        total_burnt_fee += burnt_fee
        if isinstance(ct, AcceptedDeploy):
            tx = ct.transaction
            if not isinstance(tx, DeployTransaction):
                raise HTTPException(status_code=550, detail="Invalid transaction type")
            t = {
                "tx_id": tx.id,
                "index": ct.index,
                "type": "Deploy",
                "state": "Accepted",
                "transitions_count": 1,
                "base_fee": base_fee - burnt_fee,
                "priority_fee": priority_fee,
                "burnt_fee": burnt_fee,
                "program_id": tx.deployment.program.id,
            }
            txs.append(t)
        elif isinstance(ct, AcceptedExecute):
            tx = ct.transaction
            if not isinstance(tx, ExecuteTransaction):
                raise HTTPException(status_code=550, detail="Invalid transaction type")
            fee = tx.fee.value
            if fee is not None:
                base_fee, priority_fee = fee.amount
            else:
                base_fee, priority_fee = 0, 0
            root_transition = tx.execution.transitions[-1]
            t = {
                "tx_id": tx.id,
                "index": ct.index,
                "type": "Execute",
                "state": "Accepted",
                "transitions_count": len(tx.execution.transitions) + bool(tx.fee.value is not None),
                "base_fee": base_fee - burnt_fee,
                "priority_fee": priority_fee,
                "burnt_fee": burnt_fee,
                "root_transition": f"{root_transition.program_id}/{root_transition.function_name}",
            }
            txs.append(t)
        elif isinstance(ct, RejectedExecute):
            tx = ct.transaction
            if not isinstance(tx, FeeTransaction):
                raise HTTPException(status_code=550, detail="Invalid transaction type")
            base_fee, priority_fee = tx.fee.amount
            rejected = ct.rejected
            if not isinstance(rejected, RejectedExecution):
                raise HTTPException(status_code=550, detail="Invalid rejected transaction type")
            root_transition = rejected.execution.transitions[-1]
            t = {
                "tx_id": tx.id,
                "index": ct.index,
                "type": "Execute",
                "state": "Rejected",
                "transitions_count": 1,
                "base_fee": base_fee - burnt_fee,
                "priority_fee": priority_fee,
                "burnt_fee": burnt_fee,
                "root_transition": f"{root_transition.program_id}/{root_transition.function_name}",
            }
            txs.append(t)
        else:
            raise HTTPException(status_code=550, detail="Unsupported transaction type")

    validators, all_validators_raw = await db.get_validator_by_height(height)
    all_validators = []
    for v in all_validators_raw:
        all_validators.append(await UIAddress(v["address"]).resolve(db))

    sync_info = await out_of_sync_check(request.app.state.session, db)
    ctx = {
        "block": block,
        "block_hash_trunc": str(block_hash)[:12] + "..." + str(block_hash)[-6:],
        "validator": "Not implemented", # await db.get_miner_from_block_hash(block.block_hash),
        "coinbase_reward": coinbase_reward,
        "transactions": txs,
        "coinbase_solutions": css,
        "target_sum": target_sum,
        "total_base_fee": total_base_fee,
        "total_priority_fee": total_priority_fee,
        "total_burnt_fee": total_burnt_fee,
        "validators": validators,
        "all_validators": all_validators,
        "sync_info": sync_info,
    }
    return ctx, {'Cache-Control': 'public, max-age=3600'}


@htmx_template("transaction.jinja2")
async def transaction_route(request: Request):
    db: Database = request.app.state.db
    tx_id = request.query_params.get("id")
    if tx_id is None:
        raise HTTPException(status_code=400, detail="Missing transaction id")
    tx_id = await db.get_updated_transaction_id(tx_id)
    is_confirmed = await db.is_transaction_confirmed(tx_id)
    if is_confirmed is None:
        raise HTTPException(status_code=404, detail="Transaction not found")
    if is_confirmed:
        confirmed_transaction = await db.get_confirmed_transaction(tx_id)
        if confirmed_transaction is None:
            raise HTTPException(status_code=550, detail="Database inconsistent")
        transaction = confirmed_transaction.transaction
    else:
        confirmed_transaction = None
        transaction = await db.get_unconfirmed_transaction(tx_id)
        if transaction is None:
            raise HTTPException(status_code=404, detail="Transaction not found")

    first_seen = await db.get_transaction_first_seen(tx_id)
    index = -1
    original_txid: Optional[str] = None
    program_info: Optional[dict[str, Any]] = None
    if isinstance(transaction, DeployTransaction):
        transaction_type = "Deploy"
        if is_confirmed:
            transaction_state = "Accepted"
            index = confirmed_transaction.index
        else:
            transaction_state = "Unconfirmed"
            program_info = await db.get_deploy_transaction_program_info(tx_id)
            if program_info is None:
                raise HTTPException(status_code=550, detail="Database inconsistent")
    elif isinstance(transaction, ExecuteTransaction):
        transaction_type = "Execute"
        if is_confirmed:
            transaction_state = "Accepted"
            index = confirmed_transaction.index
        else:
            transaction_state = "Unconfirmed"
    elif isinstance(transaction, FeeTransaction):
        if confirmed_transaction is None:
            raise HTTPException(status_code=550, detail="Database inconsistent")
        index = confirmed_transaction.index
        if isinstance(confirmed_transaction, RejectedDeploy):
            transaction_type = "Deploy"
            transaction_state = "Rejected"
            program_info = await db.get_deploy_transaction_program_info(tx_id)
        elif isinstance(confirmed_transaction, RejectedExecute):
            transaction_type = "Execute"
            transaction_state = "Rejected"
        else:
            raise HTTPException(status_code=550, detail="Database inconsistent")
        original_txid = await db.get_rejected_transaction_original_id(tx_id)
    else:
        raise HTTPException(status_code=550, detail="Unsupported transaction type")

    # TODO: use proper fee calculation
    if confirmed_transaction is None:
        # storage_cost, namespace_cost, finalize_costs, priority_fee, burnt = await transaction.get_fee_breakdown(db)
        block = None
        block_confirm_time = None
    else:
        # storage_cost, namespace_cost, finalize_costs, priority_fee, burnt = await confirmed_transaction.get_fee_breakdown(db)
        block = await db.get_block_from_transaction_id(tx_id)
        block_confirm_time = await db.get_block_confirm_time(block.height)

    fee = transaction.fee
    if isinstance(fee, Fee):
        storage_cost, priority_fee = fee.amount
    elif fee.value is not None:
        storage_cost, priority_fee = fee.value.amount
    else:
        storage_cost, priority_fee = 0, 0
    namespace_cost = 0
    finalize_costs = []
    burnt = 0

    sync_info = await out_of_sync_check(request.app.state.session, db)
    ctx: dict[str, Any] = {
        "tx_id": tx_id,
        "tx_id_trunc": str(tx_id)[:12] + "..." + str(tx_id)[-6:],
        "block": block,
        "block_confirm_time": block_confirm_time,
        "index": index,
        "transaction": transaction,
        "type": transaction_type,
        "state": transaction_state,
        "total_fee": storage_cost + namespace_cost + sum(finalize_costs) + priority_fee + burnt,
        "storage_cost": storage_cost,
        "namespace_cost": namespace_cost,
        "finalize_costs": finalize_costs,
        "priority_fee": priority_fee,
        "burnt_fee": burnt,
        "first_seen": first_seen,
        "original_txid": original_txid,
        "program_info": program_info,
        "reject_reason": await db.get_transaction_reject_reason(tx_id) if transaction_state == "Rejected" else None,
        "sync_info": sync_info,
    }

    if isinstance(transaction, DeployTransaction):
        deployment = transaction.deployment
        program = deployment.program
        fee_transition = transaction.fee.transition
        ctx.update({
            "edition": int(deployment.edition),
            "program_id": str(program.id),
            "transitions": [{
                "transition_id": transaction.fee.transition.id,
                "action": f"{fee_transition.program_id}/{fee_transition.function_name}",
            }],
        })
    elif isinstance(transaction, ExecuteTransaction):
        global_state_root = transaction.execution.global_state_root
        proof = transaction.execution.proof.value
        transitions: DictList = []

        for transition in transaction.execution.transitions:
            transitions.append({
                "transition_id": transition.id,
                "action":f"{transition.program_id}/{transition.function_name}",
            })
        if transaction.fee.value is not None:
            fee = transaction.fee.value
            transition = fee.transition
            fee_transition = {
                "transition_id": transition.id,
                "action":f"{transition.program_id}/{transition.function_name}",
            }
        else:
            fee_transition = None
        ctx.update({
            "global_state_root": global_state_root,
            "proof": proof,
            "proof_trunc": str(proof)[:30] + "..." + str(proof)[-30:] if proof else None,
            "transitions": transitions,
            "fee_transition": fee_transition,
        })
    elif isinstance(transaction, FeeTransaction):
        global_state_root = transaction.fee.global_state_root
        proof = transaction.fee.proof.value
        transitions = []
        rejected_transitions: DictList = []
        transition = transaction.fee.transition
        transitions.append({
            "transition_id": transition.id,
            "action":f"{transition.program_id}/{transition.function_name}",
        })
        if isinstance(confirmed_transaction, RejectedExecute):
            rejected = confirmed_transaction.rejected
            if not isinstance(rejected, RejectedExecution):
                raise HTTPException(status_code=550, detail="invalid rejected transaction")
            for transition in rejected.execution.transitions:
                transition: Transition
                rejected_transitions.append({
                    "transition_id": transition.id,
                    "action": await function_signature(db, str(transition.program_id), str(transition.function_name)),
                })
        else:
            raise HTTPException(status_code=550, detail="Unsupported transaction type")
        ctx.update({
            "global_state_root": global_state_root,
            "proof": proof,
            "proof_trunc": str(proof)[:30] + "..." + str(proof)[-30:] if proof else None,
            "transitions": transitions,
            "rejected_transitions": rejected_transitions,
        })

    else:
        raise HTTPException(status_code=550, detail="Unsupported transaction type")

    mapping_operations: Optional[list[dict[str, Any]]] = None
    if confirmed_transaction is not None:
        limited_tracking = {
            cached_get_mapping_id("credits.aleo", "committee"): ("credits.aleo", "committee"),
            cached_get_mapping_id("credits.aleo", "bonded"): ("credits.aleo", "bonded"),
        }
        fos: list[FinalizeOperation] = []
        untracked_fos: list[FinalizeOperation] = []
        for ct in block.transactions:
            for fo in ct.finalize:
                if isinstance(fo, (UpdateKeyValue, RemoveKeyValue)):
                    if str(fo.mapping_id) in limited_tracking:
                        untracked_fos.append(fo)
                    else:
                        fos.append(fo)
        mhs = await db.get_transaction_mapping_history_by_height(block.height)
        # TODO: remove compatibility after mainnet
        after_tracking = False
        if len(fos) + len(untracked_fos) == len(mhs):
            after_tracking = True
            fos = []
            for ct in block.transactions:
                for fo in ct.finalize:
                    if isinstance(fo, (UpdateKeyValue, RemoveKeyValue)):
                        fos.append(fo)
        if len(fos) == len(mhs):
            indices: list[int] = []
            untracked_indices: list[int] = []
            last_index = -1
            for fo in confirmed_transaction.finalize:
                if isinstance(fo, (UpdateKeyValue, RemoveKeyValue)):
                    if not after_tracking and fo in untracked_fos:
                        untracked_indices.append(untracked_fos.index(fo))
                    else:
                        last_index = fos.index(fo, last_index + 1)
                        indices.append(last_index)
            mapping_operations: Optional[list[dict[str, Any]]] = []
            for i in untracked_indices:
                fo = untracked_fos[i]
                program_id, mapping_name = limited_tracking[str(fo.mapping_id)]
                if isinstance(fo, UpdateKeyValue):
                    mapping_operations.append({
                        "type": "Update",
                        "program_id": program_id,
                        "mapping_name": mapping_name,
                        "key": None,
                        "value": None,
                        "previous_value": None,
                    })
                elif isinstance(fo, RemoveKeyValue):
                    mapping_operations.append({
                        "type": "Remove",
                        "program_id": program_id,
                        "mapping_name": mapping_name,
                        "key": None,
                        "value": None,
                        "previous_value": None,
                    })
            for i in indices:
                fo = fos[i]
                mh = mhs[i]
                if str(fo.mapping_id) != str(mh["mapping_id"]):
                    mapping_operations = None
                    break
                limited_tracked = str(fo.mapping_id) in limited_tracking
                if isinstance(fo, UpdateKeyValue):
                    if mh["value"] is None:
                        mapping_operations = None
                        break
                    key_id = cached_get_key_id(mh["program_id"], mh["mapping"], mh["key"])
                    value_id = aleo_explorer_rust.get_value_id(str(key_id), mh["value"])
                    if value_id != str(fo.value_id):
                        mapping_operations = None
                        break
                    if limited_tracked:
                        previous_value = None
                    else:
                        previous_value = await db.get_mapping_history_previous_value(mh["id"], mh["key_id"])
                    if previous_value is not None:
                        previous_value = str(Value.load(BytesIO(previous_value)))
                    mapping_operations.append({
                        "type": "Update",
                        "program_id": mh["program_id"],
                        "mapping_name": mh["mapping"],
                        "key": str(Plaintext.load(BytesIO(mh["key"]))),
                        "value": str(Value.load(BytesIO(mh["value"]))),
                        "previous_value": previous_value,
                        "limited_tracked": limited_tracked,
                    })
                elif isinstance(fo, RemoveKeyValue):
                    if mh["value"] is not None:
                        mapping_operations = None
                        break
                    if limited_tracked:
                        previous_value = None
                    else:
                        previous_value = await db.get_mapping_history_previous_value(mh["id"], mh["key_id"])
                    if previous_value is not None:
                        previous_value = str(Value.load(BytesIO(previous_value)))
                    elif not limited_tracked:
                        mapping_operations = None
                        break
                    mapping_operations.append({
                        "type": "Remove",
                        "program_id": mh["program_id"],
                        "mapping_name": mh["mapping"],
                        "key": str(Plaintext.load(BytesIO(mh["key"]))),
                        "previous_value": previous_value,
                        "limited_tracked": limited_tracked,
                    })

    ctx["mapping_operations"] = mapping_operations

    return ctx, {'Cache-Control': 'public, max-age=15'}


@htmx_template("transition.jinja2")
async def transition_route(request: Request):
    db: Database = request.app.state.db
    ts_id = request.query_params.get("id")
    if ts_id is None:
        raise HTTPException(status_code=400, detail="Missing transition id")
    block = await db.get_block_from_transition_id(ts_id)
    if block is None:
        raise HTTPException(status_code=404, detail="Transition not found")

    transaction_id = None
    transition = None
    state = ""
    for ct in block.transactions:
        if transition is not None:
            break
        match ct:
            case AcceptedDeploy():
                tx = ct.transaction
                if not isinstance(tx, DeployTransaction):
                    raise HTTPException(status_code=550, detail="Database inconsistent")
                state = "Accepted"
                if str(tx.fee.transition.id) == ts_id:
                    transition = tx.fee.transition
                    transaction_id = tx.id
                    break
            case AcceptedExecute():
                tx = ct.transaction
                if not isinstance(tx, ExecuteTransaction):
                    raise HTTPException(status_code=550, detail="Database inconsistent")
                state = "Accepted"
                for ts in tx.execution.transitions:
                    if str(ts.id) == ts_id:
                        transition = ts
                        transaction_id = tx.id
                        break
                if transaction_id is None and tx.fee.value is not None:
                    ts = tx.fee.value.transition
                    if str(ts.id) == ts_id:
                        transition = ts
                        transaction_id = tx.id
                        break
            case RejectedExecute():
                tx = ct.transaction
                if not isinstance(tx, FeeTransaction):
                    raise HTTPException(status_code=550, detail="Database inconsistent")
                if str(tx.fee.transition.id) == ts_id:
                    transition = tx.fee.transition
                    transaction_id = tx.id
                    state = "Accepted"
                else:
                    rejected = ct.rejected
                    if not isinstance(rejected, RejectedExecution):
                        raise HTTPException(status_code=550, detail="Database inconsistent")
                    for ts in rejected.execution.transitions:
                        if str(ts.id) == ts_id:
                            transition = ts
                            transaction_id = tx.id
                            state = "Rejected"
                            break
            case _:
                raise HTTPException(status_code=550, detail="Not implemented")
    if transaction_id is None:
        raise HTTPException(status_code=550, detail="Transition not found in block")
    transition = cast(Transition, transition)

    program_id = transition.program_id
    function_name = transition.function_name
    tpk = transition.tpk
    tcm = transition.tcm

    inputs: DictList = []
    for input_ in transition.inputs:
        if isinstance(input_, PublicTransitionInput):
            inputs.append({
                "type": "Public",
                "plaintext_hash": input_.plaintext_hash,
                "plaintext": input_.plaintext.value,
            })
        elif isinstance(input_, PrivateTransitionInput):
            inputs.append({
                "type": "Private",
                "ciphertext_hash": input_.ciphertext_hash,
                "ciphertext": input_.ciphertext.value,
            })
        elif isinstance(input_, RecordTransitionInput):
            inputs.append({
                "type": "Record",
                "serial_number": input_.serial_number,
                "tag": input_.tag,
            })
        elif isinstance(input_, ExternalRecordTransitionInput):
            inputs.append({
                "type": "External record",
                "commitment": input_.input_commitment,
            })
        else:
            raise HTTPException(status_code=550, detail="Not implemented")

    outputs: DictList = []
    self_future: Optional[Future] = None
    for output in transition.outputs:
        output: TransitionOutput
        if isinstance(output, PublicTransitionOutput):
            outputs.append({
                "type": "Public",
                "plaintext_hash": output.plaintext_hash,
                "plaintext": output.plaintext.value,
            })
        elif isinstance(output, PrivateTransitionOutput):
            outputs.append({
                "type": "Private",
                "ciphertext_hash": output.ciphertext_hash,
                "ciphertext": output.ciphertext.value,
            })
        elif isinstance(output, RecordTransitionOutput):
            output_data: dict[str, Any] = {
                "type": "Record",
                "commitment": output.commitment,
                "checksum": output.checksum,
                "record": output.record_ciphertext.value,
            }
            record = output.record_ciphertext.value
            if record is not None:
                record_data: dict[str, Any] = {
                    "owner": record.owner,
                }
                data: list[tuple[Identifier, Entry[Any]]] = []
                for identifier, entry in record.data:
                    data.append((identifier, entry))
                record_data["data"] = data
                output_data["record_data"] = record_data
            outputs.append(output_data)
        elif isinstance(output, ExternalRecordTransitionOutput):
            outputs.append({
                "type": "External record",
                "commitment": output.commitment,
            })
        elif isinstance(output, FutureTransitionOutput):
            outputs.append({
                "type": "Future",
                "future_hash": output.future_hash,
                "future": output.future.value,
            })
            if output.future.value is not None:
                future = output.future.value
                if future.program_id == program_id and future.function_name == function_name:
                    self_future = future
        else:
            raise HTTPException(status_code=550, detail="Not implemented")

    finalizes: list[dict[str, str]] = []
    if self_future is not None:
        for i, argument in enumerate(self_future.arguments):
            if isinstance(argument, PlaintextArgument):
                struct_type = ""
                if isinstance(argument.plaintext, StructPlaintext):
                    program = await get_program(db, str(transition.program_id))
                    finalize = cast(Finalize, program.functions[transition.function_name].finalize.value)
                    finalize_type = cast(PlaintextFinalizeType, finalize.inputs[i].finalize_type)
                    struct_type = str(cast(StructPlaintextType, finalize_type.plaintext_type).struct)
                finalizes.append({
                    "type": "Plaintext",
                    "struct_type": struct_type,
                    "value": str(argument.plaintext)
                })
            elif isinstance(argument, FutureArgument):
                future = argument.future
                finalizes.append({
                    "type": "Future",
                    "value": f"{future.program_id}/{future.function_name}(...)",
                })

    sync_info = await out_of_sync_check(request.app.state.session, db)
    ctx = {
        "ts_id": ts_id,
        "ts_id_trunc": str(ts_id)[:12] + "..." + str(ts_id)[-6:],
        "transaction_id": transaction_id,
        "transition": transition,
        "state": state,
        "program_id": program_id,
        "function_name": function_name,
        "tpk": tpk,
        "tcm": tcm,
        "function_signature": await function_signature(db, str(transition.program_id), str(transition.function_name)),
        "function_definition": await function_definition(db, str(transition.program_id), str(transition.function_name)),
        "inputs": inputs,
        "outputs": outputs,
        "finalizes": finalizes,
        "sync_info": sync_info,
    }
    return ctx, {'Cache-Control': 'public, max-age=15'}


@htmx_template("search_result.jinja2")
async def search_route(request: Request):
    db: Database = request.app.state.db
    query = request.query_params.get("q")
    if query is None:
        raise HTTPException(status_code=400, detail="Missing query")
    query = query.lower().strip()
    remaining_query = dict(request.query_params)
    del remaining_query["q"]
    if remaining_query:
        remaining_query = "&" + "&".join([f"{k}={v}" for k, v in remaining_query.items()])
    else:
        remaining_query = ""
    try:
        height = int(query)
        return RedirectResponse(f"/block?h={height}{remaining_query}", status_code=302)
    except ValueError:
        pass
    if query.startswith("aprivatekey1zkp"):
        raise HTTPException(status_code=400, detail=">>> YOU HAVE LEAKED YOUR PRIVATE KEY <<< Please throw it away and generate a new one.")
    elif query.startswith("ab1"):
        # block hash
        blocks = await db.search_block_hash(query)
        if not blocks:
            raise HTTPException(status_code=404, detail="Block not found")
        if len(blocks) == 1:
            return RedirectResponse(f"/block?bh={blocks[0]}{remaining_query}", status_code=302)
        too_many = False
        if len(blocks) > 50:
            blocks = blocks[:50]
            too_many = True
        ctx = {
            "query": query,
            "type": "block",
            "blocks": blocks,
            "too_many": too_many,
        }
        return ctx, {'Cache-Control': 'public, max-age=15'}
    elif query.startswith("at1"):
        # transaction id
        transactions = await db.search_transaction_id(query)
        if not transactions:
            raise HTTPException(status_code=404, detail="Transaction not found")
        if len(transactions) == 1:
            return RedirectResponse(f"/transaction?id={transactions[0]}{remaining_query}", status_code=302)
        too_many = False
        if len(transactions) > 50:
            transactions = transactions[:50]
            too_many = True
        ctx = {
            "query": query,
            "type": "transaction",
            "transactions": transactions,
            "too_many": too_many,
        }
        return ctx, {'Cache-Control': 'public, max-age=15'}
    elif query.startswith("au1"):
        # transition id
        transitions = await db.search_transition_id(query)
        if not transitions:
            raise HTTPException(status_code=404, detail="Transition not found")
        if len(transitions) == 1:
            return RedirectResponse(f"/transition?id={transitions[0]}{remaining_query}", status_code=302)
        too_many = False
        if len(transitions) > 50:
            transitions = transitions[:50]
            too_many = True
        ctx = {
            "query": query,
            "type": "transition",
            "transitions": transitions,
            "too_many": too_many,
        }
        return ctx, {'Cache-Control': 'public, max-age=15'}
    elif query.startswith("aleo1"):
        # address
        addresses = await db.search_address(query)
        if not addresses:
            raise HTTPException(status_code=404, detail="Address not found. See FAQ for more info.")
        if len(addresses) == 1:
            return RedirectResponse(f"/address?a={addresses[0]}{remaining_query}", status_code=302)
        too_many = False
        if len(addresses) > 50:
            addresses = addresses[:50]
            too_many = True
        ctx = {
            "query": query,
            "type": "address",
            "addresses": addresses,
            "too_many": too_many,
        }
        return ctx, {'Cache-Control': 'public, max-age=15'}
    elif query.endswith(".ans"):
        address = await util.arc0137.get_address_from_domain(db, query)
        if address is None:
            raise HTTPException(status_code=404, detail="ANS domain not found")
        if address == "":
            raise HTTPException(status_code=404, detail="ANS domain is private")
        return RedirectResponse(f"/address?a={address}{remaining_query}", status_code=302)
    else:
        # have to do this to support program name prefix search
        programs = await db.search_program(query)
        if programs:
            if len(programs) == 1:
                return RedirectResponse(f"/program?id={programs[0]}{remaining_query}", status_code=302)
            too_many = False
            if len(programs) > 50:
                programs = programs[:50]
                too_many = True
            ctx = {
                "query": query,
                "type": "program",
                "programs": programs,
                "too_many": too_many,
            }
            return ctx, {'Cache-Control': 'public, max-age=15'}
    raise HTTPException(status_code=404, detail="Unknown object type or searching is not supported")


@htmx_template("blocks.jinja2")
async def blocks_route(request: Request):
    db: Database = request.app.state.db
    try:
        page = request.query_params.get("p")
        if page is None:
            page = 1
        else:
            page = int(page)
    except:
        raise HTTPException(status_code=400, detail="Invalid page")
    total_blocks = await db.get_latest_height()
    if not total_blocks:
        raise HTTPException(status_code=550, detail="No blocks found")
    total_pages = (total_blocks // 50) + 1
    if page < 1 or page > total_pages:
        raise HTTPException(status_code=400, detail="Invalid page")
    start = total_blocks - 50 * (page - 1)
    blocks = await db.get_blocks_range_fast(start, start - 50)

    sync_info = await out_of_sync_check(request.app.state.session, db)
    ctx = {
        "blocks": blocks,
        "page": page,
        "total_pages": total_pages,
        "sync_info": sync_info,
    }
    return ctx, {'Cache-Control': 'public, max-age=15'}


@htmx_template("validators.jinja2")
async def validators_route(request: Request):
    db: Database = request.app.state.db
    try:
        page = request.query_params.get("p")
        if page is None:
            page = 1
        else:
            page = int(page)
    except:
        raise HTTPException(status_code=400, detail="Invalid page")
    latest_height = await db.get_latest_height()
    total_validators = await db.get_validator_count_at_height(latest_height)
    if not total_validators:
        raise HTTPException(status_code=550, detail="No validators found")
    total_pages = (total_validators // 50) + 1
    if page < 1 or page > total_pages:
        raise HTTPException(status_code=400, detail="Invalid page")
    start = 50 * (page - 1)
    validators_data = await db.get_validators_range_at_height(latest_height, start, start + 50)
    validators: list[dict[str, Any]] = []
    total_stake = 0
    for validator in validators_data:
        validators.append({
            "address": validator["address"],
            "stake": validator["stake"],
            "uptime": validator["uptime"] * 100,
        })
        total_stake += validator["stake"]

    sync_info = await out_of_sync_check(request.app.state.session, db)
    ctx = {
        "validators": validators,
        "total_stake": total_stake,
        "page": page,
        "total_pages": total_pages,
        "sync_info": sync_info,
    }
    return ctx, {'Cache-Control': 'public, max-age=15'}


@htmx_template("unconfirmed_transactions.jinja2")
async def unconfirmed_transactions_route(request: Request):
    db: Database = request.app.state.db
    try:
        page = request.query_params.get("p")
        if page is None:
            page = 1
        else:
            page = int(page)
    except:
        raise HTTPException(status_code=400, detail="Invalid page")
    total_transactions = await db.get_unconfirmed_transaction_count()
    total_pages = (total_transactions // 50) + 1
    if page < 1 or page > total_pages:
        raise HTTPException(status_code=400, detail="Invalid page")
    start = 50 * (page - 1)
    data = await db.get_unconfirmed_transactions_range(start, start + 50)
    transactions: list[dict[str, Any]] = []
    for tx in data:
        transactions.append({
            "tx_id": tx.id,
            "type": tx.type.name,
            "first_seen": await db.get_transaction_first_seen(str(tx.id)),
        })

    sync_info = await out_of_sync_check(request.app.state.session, db)
    ctx = {
        "transactions": transactions,
        "page": page,
        "total_pages": total_pages,
        "sync_info": sync_info,
    }
    return ctx, {'Cache-Control': 'public, max-age=5'}

@htmx_template("nodes.jinja2")
async def nodes_route(request: Request):
    lns: LightNodeState = request.app.state.lns
    lns.cleanup()
    nodes = lns.states
    res = {}
    for k, v in nodes.items():
        if k.startswith("127.0.0.1"):
            continue
        res[k] = copy.deepcopy(v)
        res[k]["last_ping"] = get_relative_time(v["last_ping"])
    validators = 0
    clients = 0
    provers = 0
    unknowns = 0
    connected = 0
    def sort_cmp(a: tuple[str, dict[str, Any]], b: tuple[str, dict[str, Any]]) -> int:
        # sort by: height, address, node type
        a_height = a[1].get("height", None)
        b_height = b[1].get("height", None)
        if a_height is not None and b_height is not None:
            return int(b_height) - int(a_height)
        if a_height is None and b_height is not None:
            return 1
        if a_height is not None and b_height is None:
            return -1
        a_address = a[1].get("address", None)
        b_address = b[1].get("address", None)
        if a_address is not None and b_address is not None:
            if a_address == b_address:
                return 0
            return 1 if a_address > b_address else -1
        if a_address is None and b_address is not None:
            return 1
        if a_address is not None and b_address is None:
            return -1
        a_type = a[1].get("node_type", None)
        b_type = b[1].get("node_type", None)
        if a_type is None and b_type is not None:
            return 1
        if a_type is not None and b_type is None:
            return -1
        if a_type == b_type:
            return 0
        return a_type.value - b_type.value

    res = OrderedDict(sorted(res.items(), key=functools.cmp_to_key(sort_cmp)))
    for node in res.values():
        node_type = node.get("node_type", None)
        if node_type is None:
            unknowns += 1
        elif node_type == NodeType.Validator:
            validators += 1
        elif node_type == NodeType.Client:
            clients += 1
        elif node_type == NodeType.Prover:
            provers += 1
        if node.get("peer_count", 0) > 0:
            connected += 1
    ctx = {
        "nodes": res,
        "validators": validators,
        "clients": clients,
        "provers": provers,
        "unknowns": unknowns,
        "connected": connected,
    }
    return ctx, {'Cache-Control': 'no-cache'}