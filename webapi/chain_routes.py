import math
from decimal import Decimal
from io import BytesIO
from typing import Any, Optional

import aleo_explorer_rust
from starlette.requests import Request

from aleo_types import u64, DeployTransaction, ExecuteTransaction, FeeTransaction, RejectedDeploy, RejectedExecute, Fee, \
    FinalizeOperation, UpdateKeyValue, RemoveKeyValue, Value, Plaintext
from aleo_types.cached import cached_get_mapping_id, cached_get_key_id
from aleo_types.vm_block import AcceptedDeploy, AcceptedExecute
from db import Database
from webapi.utils import CJSONResponse, public_cache_seconds, function_definition
from webui.classes import UIAddress


async def get_summary(db: Database):
    network_speed = await db.get_network_speed()
    validators = await db.get_current_validator_count()
    participation_rate = await db.get_network_participation_rate()
    block = await db.get_latest_block()
    summary = {
        "latest_height": block.height,
        "latest_timestamp": block.header.metadata.timestamp,
        "proof_target": block.header.metadata.proof_target,
        "coinbase_target": block.header.metadata.coinbase_target,
        "network_speed": network_speed,
        "validators": validators,
        "participation_rate": participation_rate,
    }
    return summary

@public_cache_seconds(5)
async def recent_blocks_route(request: Request):
    db: Database = request.app.state.db
    recent_blocks = await db.get_recent_blocks_fast(10)
    return CJSONResponse(recent_blocks)

@public_cache_seconds(5)
async def index_update_route(request: Request):
    db: Database = request.app.state.db
    last_block = request.query_params.get("last_block")
    if last_block is None:
        return CJSONResponse({"error": "Missing last_block parameter"}, status_code=400)
    try:
        last_block = int(last_block)
    except ValueError:
        return CJSONResponse({"error": "Invalid last_block parameter"}, status_code=400)
    if last_block < 0:
        return CJSONResponse({"error": "Negative last_block parameter"}, status_code=400)
    summary = await get_summary(db)
    result: dict[str, Any] = {"summary": summary}
    latest_height = await db.get_latest_height()
    if latest_height is None:
        return CJSONResponse({"error": "Database error"}, status_code=500)
    block_count = latest_height - last_block
    if block_count < 0:
        return CJSONResponse({"summary": summary})
    if block_count > 10:
        block_count = 10
    recent_blocks = await db.get_recent_blocks_fast(block_count)
    result["recent_blocks"] = recent_blocks
    return CJSONResponse(result)

@public_cache_seconds(5)
async def block_route(request: Request):
    db: Database = request.app.state.db
    height = request.path_params["height"]
    try:
        height = int(height)
    except ValueError:
        return CJSONResponse({"error": "Invalid height"}, status_code=400)
    block = await db.get_block_by_height(height)
    if block is None:
        return CJSONResponse({"error": "Block not found"}, status_code=404)

    coinbase_reward = await db.get_block_coinbase_reward_by_height(height)
    validators, all_validators_raw = await db.get_validator_by_height(height)
    all_validators: list[str] = []
    for v in all_validators_raw:
        all_validators.append(v["address"])
    css: list[dict[str, Any]] = []
    target_sum = 0
    if coinbase_reward is not None:
        solutions = await db.get_solution_by_height(height, 0, 100)
        for solution in solutions:
            css.append({
                "address": solution["address"],
                "counter": solution["counter"],
                "target": solution["target"],
                "reward": solution["reward"],
                "solution_id": solution["solution_id"],
            })
            target_sum += solution["target"]
    result: dict[str, Any] = {
        "block": block,
        "coinbase_reward": coinbase_reward,
        "validators": validators,
        "all_validators": all_validators,
        "solutions": css,
        "total_supply": Decimal(await db.get_total_supply_at_height(height)),
    }
    result["resolved_addresses"] = \
        await UIAddress.resolve_recursive_detached(
            result, db,
            await UIAddress.resolve_recursive_detached(
                result["solutions"], db, {}
            )
        )

    return CJSONResponse(result)


@public_cache_seconds(5)
async def blocks_route(request: Request):
    db: Database = request.app.state.db
    try:
        page = request.query_params.get("p")
        if page is None:
            page = 1
        else:
            page = int(page)
    except:
        return CJSONResponse({"error": "Invalid page"}, status_code=400)
    total_blocks = await db.get_latest_height()
    if not total_blocks:
        return CJSONResponse({"error": "No blocks found"}, status_code=500)
    total_blocks += 1
    total_pages = math.ceil(total_blocks / 20)
    if page < 1 or page > total_pages:
        return CJSONResponse({"error": "Invalid page"}, status_code=400)
    start = total_blocks - 1 - 20 * (page - 1)
    blocks = await db.get_blocks_range_fast(start, start - 20)

    return CJSONResponse({"blocks": blocks, "total_blocks": total_blocks, "total_pages": total_pages})

@public_cache_seconds(5)
async def validators_route(request: Request):
    db: Database = request.app.state.db
    try:
        page = request.query_params.get("p")
        if page is None:
            page = 1
        else:
            page = int(page)
    except:
        return CJSONResponse({"error": "Invalid page"}, status_code=400)
    latest_height = await db.get_latest_height()
    if latest_height is None:
        return CJSONResponse({"error": "No blocks found"}, status_code=500)
    total_validators = await db.get_validator_count_at_height(latest_height)
    if not total_validators:
        return CJSONResponse({"error": "No validators found"}, status_code=500)
    total_pages = (total_validators // 50) + 1
    if page < 1 or page > total_pages:
        return CJSONResponse({"error": "Invalid page"}, status_code=400)
    start = 50 * (page - 1)
    validators_data = await db.get_validators_range_at_height(latest_height, start, start + 50)
    validators: list[dict[str, Any]] = []
    total_stake = 0
    for validator in validators_data:
        validators.append({
            "address": validator["address"],
            "stake": u64(validator["stake"]),
            "uptime": validator["uptime"],
            "commission": validator["commission"],
            "open": validator["is_open"],
        })
        total_stake += validator["stake"]

    result = {
        "validators": validators,
        "total_stake": total_stake,
        "page": page,
        "total_pages": total_pages,
    }
    result["resolved_addresses"] = await UIAddress.resolve_recursive_detached(result, db, {})
    return CJSONResponse(result)

@public_cache_seconds(5)
async def transaction_route(request: Request):
    db: Database = request.app.state.db
    tx_id = request.path_params.get("id")
    if tx_id is None:
        return CJSONResponse({"error": "Missing transaction id"}, status_code=400)
    tx_id = await db.get_updated_transaction_id(tx_id)
    is_confirmed = await db.is_transaction_confirmed(tx_id)
    if is_confirmed is None:
        return CJSONResponse({"error": "Transaction not found"}, status_code=404)
    if is_confirmed:
        confirmed_transaction = await db.get_confirmed_transaction(tx_id)
        if confirmed_transaction is None:
            return CJSONResponse({"error": "Internal error: should have tx"}, status_code=500)
        transaction = confirmed_transaction.transaction
        aborted = None
    else:
        confirmed_transaction = None
        transaction = await db.get_unconfirmed_transaction(tx_id)
        if transaction is None:
            return CJSONResponse({"error": "Transaction not found"}, status_code=404)
        aborted = await db.is_transaction_aborted(tx_id)
    first_seen = await db.get_transaction_first_seen(tx_id)
    original_txid: Optional[str] = None
    program_info: Optional[dict[str, Any]] = None
    if isinstance(transaction, DeployTransaction):
        transaction_type = "Deploy"
        if is_confirmed:
            transaction_state = "Accepted"
            if confirmed_transaction is None:
                return CJSONResponse({"error": "Internal error: should have tx"}, status_code=500)
        else:
            if aborted:
                transaction_state = "Aborted"
            else:
                transaction_state = "Unconfirmed"
            program_info = await db.get_deploy_transaction_program_info(tx_id)
            if program_info is None:
                return CJSONResponse({"error": "Internal error: should have program info"}, status_code=500)
    elif isinstance(transaction, ExecuteTransaction):
        transaction_type = "Execute"
        if is_confirmed:
            transaction_state = "Accepted"
            if confirmed_transaction is None:
                return CJSONResponse({"error": "Internal error: should have tx"}, status_code=500)
        else:
            if aborted:
                transaction_state = "Aborted"
            else:
                transaction_state = "Unconfirmed"
    elif isinstance(transaction, FeeTransaction):
        if confirmed_transaction is None:
            return CJSONResponse({"error": "Internal error: should have tx"}, status_code=500)
        if isinstance(confirmed_transaction, RejectedDeploy):
            transaction_type = "Deploy"
            transaction_state = "Rejected"
            program_info = await db.get_deploy_transaction_program_info(tx_id)
        elif isinstance(confirmed_transaction, RejectedExecute):
            transaction_type = "Execute"
            transaction_state = "Rejected"
        else:
            return CJSONResponse({"error": "Internal error: invalid transaction type"}, status_code=500)
        original_txid = await db.get_rejected_transaction_original_id(tx_id)
    else:
        return CJSONResponse({"error": "Unsupported transaction type"}, status_code=500)

    # TODO: use proper fee calculation
    if aborted:
        height = await db.get_transaction_aborted_height(tx_id)
        if height is None:
            return CJSONResponse({"error": "Internal error: aborted tx missing"}, status_code=500)
        block = await db.get_block_by_height(height)
        if block is None:
            return CJSONResponse({"error": "Internal error: block missing"}, status_code=500)
        block_confirm_time = await db.get_block_confirm_time(height)
    elif confirmed_transaction is None:
        # storage_cost, namespace_cost, finalize_costs, priority_fee, burnt = await transaction.get_fee_breakdown(db)
        block = None
        block_confirm_time = None
    else:
        # storage_cost, namespace_cost, finalize_costs, priority_fee, burnt = await confirmed_transaction.get_fee_breakdown(db)
        block = await db.get_block_from_transaction_id(tx_id)
        if block is None:
            return CJSONResponse({"error": "Internal error: block missing"}, status_code=500)
        block_confirm_time = await db.get_block_confirm_time(block.height)

    fee = transaction.fee
    if isinstance(fee, Fee):
        storage_cost, priority_fee = fee.amount
    elif fee.value is not None:
        storage_cost, priority_fee = fee.value.amount
    else:
        storage_cost, priority_fee = 0, 0
    namespace_cost = 0
    finalize_costs: list[int] = []
    burnt = 0

    result: dict[str, Any] = {
        "tx_id": tx_id,
        "height": block.height if block is not None else None,
        "block_confirm_time": block_confirm_time,
        "block_timestamp": block.header.metadata.timestamp if block is not None else None,
        "type": transaction_type,
        "state": transaction_state,
        "total_fee": u64(storage_cost + namespace_cost + sum(finalize_costs) + priority_fee + burnt),
        "storage_cost": u64(storage_cost),
        "namespace_cost": u64(namespace_cost),
        "finalize_costs": list(map(u64, finalize_costs)),
        "priority_fee": u64(priority_fee),
        "burnt_fee": u64(burnt),
        "first_seen": first_seen,
        "original_txid": original_txid,
        "program_info": program_info,
        "reject_reason": await db.get_transaction_reject_reason(tx_id) if transaction_state == "Rejected" else None,
        "aborted": aborted,
    }
    if confirmed_transaction is not None:
        result["confirmed_transaction"] = confirmed_transaction
    else:
        result["transaction"] = transaction

    mapping_operations: Optional[list[dict[str, Any]]] = None
    if confirmed_transaction is not None and not aborted:
        if block is None:
            return CJSONResponse({"error": "Internal error: block missing"}, status_code=500)
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
            mapping_operations = []
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

    result["mapping_operations"] = mapping_operations
    result["resolved_addresses"] = await UIAddress.resolve_recursive_detached(result, db, {})

    return CJSONResponse(result)

@public_cache_seconds(5)
async def transition_route(request: Request):
    db: Database = request.app.state.db
    transition_id = request.path_params.get("id")
    if transition_id is None:
        return CJSONResponse({"error": "Missing transition id"}, status_code=400)
    transition = await db.get_transition(transition_id)
    if transition is None:
        return CJSONResponse({"error": "Transition not found"}, status_code=404)
    transaction_id = await db.get_transaction_id_from_transition_id(transition_id)
    if transaction_id is None:
        return CJSONResponse({"error": "Transaction not found"}, status_code=404)
    is_confirmed = await db.is_transaction_confirmed(transaction_id)
    is_aborted = await db.is_transaction_aborted(transaction_id)
    is_accepted = False
    if is_confirmed:
        confirmed_transaction = await db.get_confirmed_transaction(transaction_id)
        if confirmed_transaction is None:
            return CJSONResponse({"error": "Internal error: should have tx"}, status_code=500)
        if isinstance(confirmed_transaction, (AcceptedDeploy, AcceptedExecute)):
            is_accepted = True
    result: dict[str, Any] = {
        "transition": transition.json(),
        "transaction_id": transaction_id,
        "is_confirmed": is_confirmed,
        "is_aborted": is_aborted,
        "is_accepted": is_accepted,
        "function_definition": await function_definition(db, str(transition.program_id), str(transition.function_name)),
    }
    result["resolved_addresses"] = await UIAddress.resolve_recursive_detached(result, db, {})

    return CJSONResponse(result)