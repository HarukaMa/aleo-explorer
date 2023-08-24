from io import BytesIO
from typing import Any, cast

from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import RedirectResponse

from aleo_types import u32, Transition, ExecuteTransaction, PrivateTransitionInput, \
    RecordTransitionInput, TransitionOutput, RecordTransitionOutput, DeployTransaction, Program, \
    PublicTransitionInput, \
    PublicTransitionOutput, PrivateTransitionOutput, PlaintextValue, ExternalRecordTransitionInput, \
    ExternalRecordTransitionOutput, AcceptedDeploy, AcceptedExecute, RejectedExecute, \
    FeeTransaction, RejectedDeploy, RejectedExecution, RecordValue, Identifier, Entry
from db import Database
from .template import templates
from .utils import function_signature, out_of_sync_check, function_definition, get_fee_amount_from_transition

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
    total_fee = 0
    for ct in block.transactions:
        match ct:
            case AcceptedDeploy():
                tx = ct.transaction
                if not isinstance(tx, DeployTransaction):
                    raise HTTPException(status_code=550, detail="Invalid transaction type")
                fee = get_fee_amount_from_transition(tx.fee.transition)
                t = {
                    "tx_id": tx.id,
                    "index": ct.index,
                    "type": "Deploy",
                    "state": "Accepted",
                    "transitions_count": 1,
                    "fee": fee,
                }
                txs.append(t)
                total_fee += fee
            case AcceptedExecute():
                tx = ct.transaction
                if not isinstance(tx, ExecuteTransaction):
                    raise HTTPException(status_code=550, detail="Invalid transaction type")
                fee_transition = tx.additional_fee.value
                if fee_transition is not None:
                    fee = get_fee_amount_from_transition(fee_transition.transition)
                else:
                    fee = 0
                t = {
                    "tx_id": tx.id,
                    "index": ct.index,
                    "type": "Execute",
                    "state": "Accepted",
                    "transitions_count": len(tx.execution.transitions) + bool(tx.additional_fee.value is not None),
                    "fee": fee,
                }
                txs.append(t)
                total_fee += fee
            case RejectedExecute():
                tx = ct.transaction
                if not isinstance(tx, FeeTransaction):
                    raise HTTPException(status_code=550, detail="Invalid transaction type")
                fee = get_fee_amount_from_transition(tx.fee.transition)
                t = {
                    "tx_id": tx.id,
                    "index": ct.index,
                    "type": "Execute",
                    "state": "Rejected",
                    "transitions_count": 1,
                    "fee": fee,
                }
                txs.append(t)
                total_fee += fee
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
        "total_fee": total_fee,
    }
    return templates.TemplateResponse(template, ctx, headers={'Cache-Control': 'public, max-age=3600'}) # type: ignore


async def get_transition_finalize_cost(db: Database, ts: Transition):
    if ts.program_id == "credits.aleo" and str(ts.function_name) in ["mint", "fee", "split"]:
        return 0
    else:
        pb = await db.get_program(str(ts.program_id))
        if pb is None:
            raise HTTPException(status_code=404, detail="Program not found")
        p = Program.load(BytesIO(pb))
        f = p.functions[ts.function_name]
        if f.finalize.value is not None:
            return f.finalize.value[1].cost
        else:
            return 0

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

    transaction = None
    confirmed_transaction = None
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

    ctx: dict[str, Any] = {
        "request": request,
        "tx_id": tx_id,
        "tx_id_trunc": str(tx_id)[:12] + "..." + str(tx_id)[-6:],
        "block": block,
        "index": index,
        "transaction": transaction,
        "type": transaction_type,
        "state": transaction_state,
        "reject_reason": await db.get_transaction_reject_reason(tx_id) if transaction_state == "Rejected" else None,
    }

    if isinstance(transaction, DeployTransaction):
        deployment = transaction.deployment
        program = deployment.program
        fee_transition = transaction.fee.transition
        storage_cost, namespace_cost = deployment.cost
        total_fee = get_fee_amount_from_transition(fee_transition)
        ctx.update({
            "edition": int(deployment.edition),
            "program_id": str(program.id),
            "total_fee": total_fee,
            "storage_cost": storage_cost,
            "namespace_cost": namespace_cost,
            "priority_fee": total_fee - storage_cost - namespace_cost,
            "transitions": [{
                "transition_id": transaction.fee.transition.id,
                "action": await function_signature(db, str(fee_transition.program_id), str(fee_transition.function_name)),
            }],
        })
    elif isinstance(transaction, ExecuteTransaction):
        global_state_root = transaction.execution.global_state_root
        proof = transaction.execution.proof.value
        transitions: DictList = []

        storage_cost, _ = transaction.execution.cost
        finalize_costs: list[int] = []

        for transition in transaction.execution.transitions:
            transitions.append({
                "transition_id": transition.id,
                "action": await function_signature(db, str(transition.program_id), str(transition.function_name)),
            })
            finalize_costs.append(await get_transition_finalize_cost(db, transition))
        if transaction.additional_fee.value is not None:
            transition = transaction.additional_fee.value.transition
            total_fee = get_fee_amount_from_transition(transition)
            fee_transition = {
                "transition_id": transition.id,
                "action": await function_signature(db, str(transition.program_id), str(transition.function_name)),
            }
        else:
            total_fee = 0
            fee_transition = None
        ctx.update({
            "global_state_root": global_state_root,
            "proof": proof,
            "proof_trunc": str(proof)[:30] + "..." + str(proof)[-30:] if proof else None,
            "total_fee": total_fee,
            "storage_cost": storage_cost,
            "finalize_costs": finalize_costs,
            "priority_fee": total_fee - storage_cost - sum(finalize_costs),
            "transitions": transitions,
            "fee_transition": fee_transition,
        })
    else:
        global_state_root = transaction.fee.global_state_root
        proof = transaction.fee.proof.value
        transitions = []
        rejected_transitions: DictList = []
        transition = transaction.fee.transition
        total_fee = get_fee_amount_from_transition(transition)
        transitions.append({
            "transition_id": transition.id,
            "action": await function_signature(db, str(transition.program_id), str(transition.function_name)),
        })
        if isinstance(confirmed_transaction, RejectedExecute):
            rejected = confirmed_transaction.rejected
            if not isinstance(rejected, RejectedExecution):
                raise HTTPException(status_code=550, detail="invalid rejected transaction")
            storage_cost, _ = rejected.execution.cost
            finalize_costs = []
            for transition in rejected.execution.transitions:
                transition: Transition
                finalize_costs.append(await get_transition_finalize_cost(db, transition))
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
            "total_fee": total_fee,
            "storage_cost": storage_cost,
            "finalize_costs": finalize_costs,
            "priority_fee": total_fee - storage_cost - sum(finalize_costs),
            "transitions": transitions,
            "rejected_transitions": rejected_transitions,
        })

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
        match input_:
            # TODO: report pycharm bug
            case PublicTransitionInput():
                # noinspection PyUnresolvedReferences
                inputs.append({
                    "type": "Public",
                    "plaintext_hash": input_.plaintext_hash,
                    "plaintext": input_.plaintext.value,
                })
            case PrivateTransitionInput():
                # noinspection PyUnresolvedReferences
                inputs.append({
                    "type": "Private",
                    "ciphertext_hash": input_.ciphertext_hash,
                    "ciphertext": input_.ciphertext.value,
                })
            case RecordTransitionInput():
                # noinspection PyUnresolvedReferences
                inputs.append({
                    "type": "Record",
                    "serial_number": input_.serial_number,
                    "tag": input_.tag,
                })
            case ExternalRecordTransitionInput():
                # noinspection PyUnresolvedReferences
                inputs.append({
                    "type": "External record",
                    "commitment": input_.input_commitment,
                })
            case _:
                raise HTTPException(status_code=550, detail="Not implemented")

    outputs: DictList = []
    for output in transition.outputs:
        output: TransitionOutput
        match output:
            case PublicTransitionOutput():
                # noinspection PyUnresolvedReferences
                outputs.append({
                    "type": "Public",
                    "plaintext_hash": output.plaintext_hash,
                    "plaintext": output.plaintext.value,
                })
            case PrivateTransitionOutput():
                # noinspection PyUnresolvedReferences
                outputs.append({
                    "type": "Private",
                    "ciphertext_hash": output.ciphertext_hash,
                    "ciphertext": output.ciphertext.value,
                })
            case RecordTransitionOutput():
                # noinspection PyUnresolvedReferences
                output_data: dict[str, Any] = {
                    "type": "Record",
                    "commitment": output.commitment,
                    "checksum": output.checksum,
                    "record": output.record_ciphertext.value,
                }
                # noinspection PyUnresolvedReferences
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
            case ExternalRecordTransitionOutput():
                # noinspection PyUnresolvedReferences
                outputs.append({
                    "type": "External record",
                    "commitment": output.commitment,
                })
            case _:
                raise HTTPException(status_code=550, detail="Not implemented")

    finalizes: list[str] = []
    if transition.finalize.value is not None:
        for finalize in transition.finalize.value:
            match finalize:
                case PlaintextValue():
                    # noinspection PyUnresolvedReferences
                    finalizes.append(str(finalize.plaintext))
                case RecordValue():
                    raise NotImplementedError
                case _:
                    raise HTTPException(status_code=550, detail="Not implemented")

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
    try:
        height = int(query)
        return RedirectResponse(f"/block?h={height}", status_code=302)
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
            return RedirectResponse(f"/block?bh={blocks[0]}", status_code=302)
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
            return RedirectResponse(f"/transaction?id={transactions[0]}", status_code=302)
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
    elif query.startswith("as1"):
        # transition id
        transitions = await db.search_transition_id(query)
        if not transitions:
            raise HTTPException(status_code=404, detail="Transition not found")
        if len(transitions) == 1:
            return RedirectResponse(f"/transition?id={transitions[0]}", status_code=302)
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
            return RedirectResponse(f"/address?a={addresses[0]}", status_code=302)
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
                return RedirectResponse(f"/program?id={programs[0]}", status_code=302)
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
