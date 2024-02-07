import copy
from collections import OrderedDict
from io import BytesIO
from typing import Any, cast, Optional, ParamSpec, TypeVar, Callable, Awaitable

import aleo_explorer_rust
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import RedirectResponse

from aleo_types import u32, Transition, ExecuteTransaction, PrivateTransitionInput, \
    RecordTransitionInput, TransitionOutput, RecordTransitionOutput, DeployTransaction, PublicTransitionInput, \
    PublicTransitionOutput, PrivateTransitionOutput, ExternalRecordTransitionInput, \
    ExternalRecordTransitionOutput, AcceptedDeploy, AcceptedExecute, RejectedExecute, \
    FeeTransaction, RejectedDeploy, RejectedExecution, Identifier, Entry, FutureTransitionOutput, Future, \
    PlaintextArgument, FutureArgument, StructPlaintext, Finalize, \
    PlaintextFinalizeType, StructPlaintextType, UpdateKeyValue, Value, Plaintext, RemoveKeyValue, FinalizeOperation, \
    NodeType
from db import Database
from node.light_node import LightNodeState
from util.global_cache import get_program
from .template import templates
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
async def block_route(request: Request):
    db: Database = request.app.state.db
    is_htmx = request.scope["htmx"].is_htmx()
    if is_htmx:
        template = "htmx/block.jinja2"
    else:
        template = "block.jinja2"
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
                "nonce": solution["nonce"],
                "commitment": solution["commitment"][:13] + "..." + solution["commitment"][-10:],
                "target": solution["target"],
                "reward": solution["reward"],
            })
            target_sum += solution["target"]

    txs: DictList = []
    total_base_fee = 0
    total_priority_fee = 0
    total_burnt_fee = 0
    for ct in block.transactions.transactions:
        fee_breakdown = await ct.get_fee_breakdown(db)
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
            additional_fee = tx.additional_fee.value
            if additional_fee is not None:
                base_fee, priority_fee = additional_fee.amount
            else:
                base_fee, priority_fee = 0, 0
            root_transition = tx.execution.transitions[-1]
            t = {
                "tx_id": tx.id,
                "index": ct.index,
                "type": "Execute",
                "state": "Accepted",
                "transitions_count": len(tx.execution.transitions) + bool(tx.additional_fee.value is not None),
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

    sync_info = await out_of_sync_check(request.app.state.session, db)
    ctx = {
        "request": request,
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
        "sync_info": sync_info,
    }
    return templates.TemplateResponse(template, ctx, headers={'Cache-Control': 'public, max-age=3600'}) # type: ignore

async def transaction_route(request: Request):
    db: Database = request.app.state.db
    is_htmx = request.scope["htmx"].is_htmx()
    if is_htmx:
        template = "htmx/transaction.jinja2"
    else:
        template = "transaction.jinja2"
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

    if confirmed_transaction is None:
        storage_cost, namespace_cost, finalize_costs, priority_fee, burnt = await transaction.get_fee_breakdown(db)
        block = None
        block_confirm_time = None
    else:
        storage_cost, namespace_cost, finalize_costs, priority_fee, burnt = await confirmed_transaction.get_fee_breakdown(db)
        block = await db.get_block_from_transaction_id(tx_id)
        block_confirm_time = await db.get_block_confirm_time(block.height)

    sync_info = await out_of_sync_check(request.app.state.session, db)
    ctx: dict[str, Any] = {
        "request": request,
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
        if transaction.additional_fee.value is not None:
            additional_fee = transaction.additional_fee.value
            transition = additional_fee.transition
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
        fos: list[FinalizeOperation] = []
        for ct in block.transactions:
            for fo in ct.finalize:
                if isinstance(fo, (UpdateKeyValue, RemoveKeyValue)):
                    fos.append(fo)
        mhs = await db.get_transaction_mapping_history_by_height(block.height)
        if len(fos) == len(mhs):
            indices: list[int] = []
            for fo in confirmed_transaction.finalize:
                if isinstance(fo, (UpdateKeyValue, RemoveKeyValue)):
                    indices.append(fos.index(fo))
            mapping_operations: Optional[list[dict[str, Any]]] = []
            for i in indices:
                fo = fos[i]
                mh = mhs[i]
                if str(fo.mapping_id) != str(mh["mapping_id"]):
                    mapping_operations = None
                    break
                if isinstance(fo, UpdateKeyValue):
                    if mh["value"] is None:
                        mapping_operations = None
                        break
                    key_id = aleo_explorer_rust.get_key_id(mh["program_id"], mh["mapping"], mh["key"])
                    value_id = aleo_explorer_rust.get_value_id(str(key_id), mh["value"])
                    if value_id != str(fo.value_id):
                        mapping_operations = None
                        break
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
                    })
                elif isinstance(fo, RemoveKeyValue):
                    if mh["value"] is not None:
                        mapping_operations = None
                        break
                    previous_value = await db.get_mapping_history_previous_value(mh["id"], mh["key_id"])
                    if previous_value is not None:
                        previous_value = str(Value.load(BytesIO(previous_value)))
                    else:
                        mapping_operations = None
                        break
                    mapping_operations.append({
                        "type": "Remove",
                        "program_id": mh["program_id"],
                        "mapping_name": mh["mapping"],
                        "key": str(Plaintext.load(BytesIO(mh["key"]))),
                        "previous_value": previous_value,
                    })

    ctx["mapping_operations"] = mapping_operations

    return templates.TemplateResponse(template, ctx, headers={'Cache-Control': 'public, max-age=15'}) # type: ignore


async def transition_route(request: Request):
    db: Database = request.app.state.db
    is_htmx = request.scope["htmx"].is_htmx()
    if is_htmx:
        template = "htmx/transition.jinja2"
    else:
        template = "transition.jinja2"
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
                if transaction_id is None and tx.additional_fee.value is not None:
                    ts = tx.additional_fee.value.transition
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
        "request": request,
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
    return templates.TemplateResponse(template, ctx, headers={'Cache-Control': 'public, max-age=3600'}) # type: ignore


async def search_route(request: Request):
    db: Database = request.app.state.db
    is_htmx = request.scope["htmx"].is_htmx()
    if is_htmx:
        template = "htmx/search_result.jinja2"
    else:
        template = "search_result.jinja2"
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
            "request": request,
            "query": query,
            "type": "block",
            "blocks": blocks,
            "too_many": too_many,
        }
        return templates.TemplateResponse(template, ctx, headers={'Cache-Control': 'public, max-age=15'}) # type: ignore
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
            "request": request,
            "query": query,
            "type": "transaction",
            "transactions": transactions,
            "too_many": too_many,
        }
        return templates.TemplateResponse(template, ctx, headers={'Cache-Control': 'public, max-age=15'}) # type: ignore
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
            "request": request,
            "query": query,
            "type": "transition",
            "transitions": transitions,
            "too_many": too_many,
        }
        return templates.TemplateResponse(template, ctx, headers={'Cache-Control': 'public, max-age=15'}) # type: ignore
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
            "request": request,
            "query": query,
            "type": "address",
            "addresses": addresses,
            "too_many": too_many,
        }
        return templates.TemplateResponse(template, ctx, headers={'Cache-Control': 'public, max-age=15'}) # type: ignore
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
                "request": request,
                "query": query,
                "type": "program",
                "programs": programs,
                "too_many": too_many,
            }
            return templates.TemplateResponse(template, ctx, headers={'Cache-Control': 'public, max-age=15'}) # type: ignore
    raise HTTPException(status_code=404, detail="Unknown object type or searching is not supported")


async def blocks_route(request: Request):
    db: Database = request.app.state.db
    is_htmx = request.scope["htmx"].is_htmx()
    if is_htmx:
        template = "htmx/blocks.jinja2"
    else:
        template = "blocks.jinja2"
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
        "request": request,
        "blocks": blocks,
        "page": page,
        "total_pages": total_pages,
        "sync_info": sync_info,
    }
    return templates.TemplateResponse(template, ctx, headers={'Cache-Control': 'public, max-age=15'}) # type: ignore

async def validators_route(request: Request):
    db: Database = request.app.state.db
    is_htmx = request.scope["htmx"].is_htmx()
    if is_htmx:
        template = "htmx/validators.jinja2"
    else:
        template = "validators.jinja2"
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
    for validator in validators_data:
        validators.append({
            "address": validator["address"],
            "stake": validator["stake"],
            "uptime": validator["uptime"] * 100,
        })


    sync_info = await out_of_sync_check(request.app.state.session, db)
    ctx = {
        "request": request,
        "validators": validators,
        "page": page,
        "total_pages": total_pages,
        "sync_info": sync_info,
    }
    return templates.TemplateResponse(template, ctx, headers={'Cache-Control': 'public, max-age=15'}) # type: ignore

async def unconfirmed_transactions_route(request: Request):
    db: Database = request.app.state.db
    is_htmx = request.scope["htmx"].is_htmx()
    if is_htmx:
        template = "htmx/unconfirmed_transactions.jinja2"
    else:
        template = "unconfirmed_transactions.jinja2"
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
        "request": request,
        "transactions": transactions,
        "page": page,
        "total_pages": total_pages,
        "sync_info": sync_info,
    }
    return templates.TemplateResponse(template, ctx, headers={'Cache-Control': 'public, max-age=5'}) # type: ignore

async def nodes_route(request: Request):
    lns: LightNodeState = request.app.state.lns
    lns.cleanup()
    is_htmx = request.scope["htmx"].is_htmx()
    if is_htmx:
        template = "htmx/nodes.jinja2"
    else:
        template = "nodes.jinja2"
    nodes = lns.states
    res = {}
    for k, v in nodes.items():
        res[k] = copy.deepcopy(v)
        res[k]["last_ping"] = get_relative_time(v["last_ping"])
    validators = 0
    clients = 0
    provers = 0
    unknowns = 0
    connected = 0
    def sort_key(item: tuple[str, dict[str, Any]]) -> int:
        if (x := item[1].get("height", 0)) is None:
            return 0
        if item[1].get("node_type", None) is None:
            return -2
        if item[1].get("peer_count", None) is None:
            return -1
        return x
    res = OrderedDict(sorted(res.items(), key=sort_key, reverse=True))
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
        "request": request,
        "nodes": res,
        "validators": validators,
        "clients": clients,
        "provers": provers,
        "unknowns": unknowns,
        "connected": connected,
    }
    return templates.TemplateResponse(template, ctx, headers={'Cache-Control': 'no-cache'})