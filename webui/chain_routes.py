from typing import Any, cast, Optional

from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import RedirectResponse

from aleo_types import u32, Transition, ExecuteTransaction, PrivateTransitionInput, \
    RecordTransitionInput, TransitionOutput, RecordTransitionOutput, DeployTransaction, PublicTransitionInput, \
    PublicTransitionOutput, PrivateTransitionOutput, ExternalRecordTransitionInput, \
    ExternalRecordTransitionOutput, AcceptedDeploy, AcceptedExecute, RejectedExecute, \
    FeeTransaction, RejectedDeploy, RejectedExecution, Identifier, Entry, ConfirmedTransaction, \
    Transaction, FutureTransitionOutput, Future, PlaintextArgument, FutureArgument, StructPlaintext, Finalize, \
    PlaintextFinalizeType, StructPlaintextType
from db import Database
from util.global_cache import get_program
from .template import templates
from .utils import function_signature, out_of_sync_check, function_definition

DictList = list[dict[str, Any]]

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
        match ct:
            case AcceptedDeploy():
                tx = ct.transaction
                if not isinstance(tx, DeployTransaction):
                    raise HTTPException(status_code=550, detail="Invalid transaction type")
                t = {
                    "tx_id": tx.id,
                    "index": ct.index,
                    "type": "Deploy",
                    "state": "Accepted",
                    "transitions_count": 1,
                    "base_fee": base_fee,
                    "priority_fee": priority_fee,
                    "burnt_fee": burnt_fee,
                }
                txs.append(t)
            case AcceptedExecute():
                tx = ct.transaction
                if not isinstance(tx, ExecuteTransaction):
                    raise HTTPException(status_code=550, detail="Invalid transaction type")
                additional_fee = tx.additional_fee.value
                if additional_fee is not None:
                    base_fee, priority_fee = additional_fee.amount
                else:
                    base_fee, priority_fee = 0, 0
                t = {
                    "tx_id": tx.id,
                    "index": ct.index,
                    "type": "Execute",
                    "state": "Accepted",
                    "transitions_count": len(tx.execution.transitions) + bool(tx.additional_fee.value is not None),
                    "base_fee": base_fee,
                    "priority_fee": priority_fee,
                    "burnt_fee": burnt_fee,
                }
                txs.append(t)
            case RejectedExecute():
                tx = ct.transaction
                if not isinstance(tx, FeeTransaction):
                    raise HTTPException(status_code=550, detail="Invalid transaction type")
                base_fee, priority_fee = tx.fee.amount
                t = {
                    "tx_id": tx.id,
                    "index": ct.index,
                    "type": "Execute",
                    "state": "Rejected",
                    "transitions_count": 1,
                    "base_fee": base_fee,
                    "priority_fee": priority_fee,
                    "burnt_fee": burnt_fee,
                }
                txs.append(t)
            case _:
                raise HTTPException(status_code=550, detail="Unsupported transaction type")
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
    block = await db.get_block_from_transaction_id(tx_id)
    if block is None:
        raise HTTPException(status_code=404, detail="Transaction not found")

    transaction: Transaction | None = None
    confirmed_transaction: ConfirmedTransaction | None = None
    transaction_type = ""
    transaction_state = ""
    index = -1
    for ct in block.transactions:
        match ct:
            case AcceptedDeploy():
                tx = ct.transaction
                if not isinstance(tx, DeployTransaction):
                    raise HTTPException(status_code=550, detail="Database inconsistent")
                transaction_type = "Deploy"
                transaction_state = "Accepted"
                if str(tx.id) == tx_id:
                    confirmed_transaction = ct
                    transaction = tx
                    index = ct.index
                    break
            case AcceptedExecute():
                tx = ct.transaction
                if not isinstance(tx, ExecuteTransaction):
                    raise HTTPException(status_code=550, detail="Database inconsistent")
                transaction_type = "Execute"
                transaction_state = "Accepted"
                if str(tx.id) == tx_id:
                    confirmed_transaction = ct
                    transaction = tx
                    index = ct.index
                    break
            case RejectedDeploy():
                raise HTTPException(status_code=550, detail="Unsupported transaction type")
            case RejectedExecute():
                tx = ct.transaction
                if not isinstance(tx, FeeTransaction):
                    raise HTTPException(status_code=550, detail="Database inconsistent")
                transaction_type = "Execute"
                transaction_state = "Rejected"
                if str(tx.id) == tx_id:
                    confirmed_transaction = ct
                    transaction = tx
                    index = ct.index
                    break
            case _:
                raise HTTPException(status_code=550, detail="Unsupported transaction type")
    if transaction is None:
        raise HTTPException(status_code=550, detail="Transaction not found in block")

    storage_cost, namespace_cost, finalize_costs, priority_fee, burnt = await confirmed_transaction.get_fee_breakdown(db)

    ctx: dict[str, Any] = {
        "request": request,
        "tx_id": tx_id,
        "tx_id_trunc": str(tx_id)[:12] + "..." + str(tx_id)[-6:],
        "block": block,
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
        "reject_reason": await db.get_transaction_reject_reason(tx_id) if transaction_state == "Rejected" else None,
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
                "action": await function_signature(db, str(fee_transition.program_id), str(fee_transition.function_name), False),
            }],
        })
    elif isinstance(transaction, ExecuteTransaction):
        global_state_root = transaction.execution.global_state_root
        proof = transaction.execution.proof.value
        transitions: DictList = []

        for transition in transaction.execution.transitions:
            transitions.append({
                "transition_id": transition.id,
                "action": await function_signature(db, str(transition.program_id), str(transition.function_name),False),
            })
        if transaction.additional_fee.value is not None:
            additional_fee = transaction.additional_fee.value
            transition = additional_fee.transition
            fee_transition = {
                "transition_id": transition.id,
                "action": await function_signature(db, str(transition.program_id), str(transition.function_name), False),
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
            "action": await function_signature(db, str(transition.program_id), str(transition.function_name), False),
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

    return templates.TemplateResponse(template, ctx, headers={'Cache-Control': 'public, max-age=3600'}) # type: ignore


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

    sync_info = await out_of_sync_check(db)
    ctx = {
        "request": request,
        "blocks": blocks,
        "page": page,
        "total_pages": total_pages,
        "sync_info": sync_info,
    }
    return templates.TemplateResponse(template, ctx, headers={'Cache-Control': 'public, max-age=15'}) # type: ignore
