from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import RedirectResponse

from db import Database
from node.types import u32, Transaction, Transition, ExecuteTransaction, TransitionInput, PrivateTransitionInput, \
    RecordTransitionInput, TransitionOutput, RecordTransitionOutput, Record, DeployTransaction, Deployment, Program, \
    PublicTransitionInput, \
    PublicTransitionOutput, PrivateTransitionOutput, Value, PlaintextValue, ExternalRecordTransitionInput, \
    ExternalRecordTransitionOutput, ConfirmedTransaction, AcceptedDeploy, AcceptedExecute, RejectedExecute, \
    FeeTransaction
from .template import templates
from .utils import function_signature, out_of_sync_check, function_definition


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
    else:
        block = await db.get_block_by_hash(block_hash)
        if block is None:
            raise HTTPException(status_code=404, detail="Block not found")
        height = block.header.metadata.height
    height = int(height)

    coinbase_reward = await db.get_block_coinbase_reward_by_height(height)
    css = []
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

    txs = []
    total_fee = 0
    for ct in block.transactions:
        ct: ConfirmedTransaction
        match ct.type:
            case ConfirmedTransaction.Type.AcceptedDeploy:
                ct: AcceptedDeploy
                tx: Transaction = ct.transaction
                if tx.type != Transaction.Type.Deploy:
                    raise HTTPException(status_code=550, detail="Invalid transaction type")
                tx: DeployTransaction
                fee = int(tx.fee.transition.inputs[1].plaintext.value.literal.primitive)
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
            case ConfirmedTransaction.Type.AcceptedExecute:
                ct: AcceptedExecute
                tx: Transaction = ct.transaction
                if tx.type != Transaction.Type.Execute:
                    raise HTTPException(status_code=550, detail="Invalid transaction type")
                tx: ExecuteTransaction
                fee_transition = tx.additional_fee.value
                if fee_transition is not None:
                    fee = int(fee_transition.transition.inputs[1].plaintext.value.literal.primitive)
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
            case ConfirmedTransaction.Type.RejectedExecute:
                ct: RejectedExecute
                tx: Transaction = ct.transaction
                if tx.type != Transaction.Type.Fee:
                    raise HTTPException(status_code=550, detail="Invalid transaction type")
                tx: FeeTransaction
                fee = int(tx.fee.transition.inputs[1].plaintext.value.literal.primitive)
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
    return templates.TemplateResponse('block.jinja2', ctx, headers={'Cache-Control': 'public, max-age=3600'})


async def transaction_route(request: Request):
    db: Database = request.app.state.db
    tx_id = request.query_params.get("id")
    if tx_id is None:
        raise HTTPException(status_code=400, detail="Missing transaction id")
    block = await db.get_block_from_transaction_id(tx_id)
    if block is None:
        raise HTTPException(status_code=404, detail="Transaction not found")

    transaction: DeployTransaction | ExecuteTransaction | FeeTransaction | None = None
    transaction_type = ""
    transaction_state = ""
    index = -1
    for ct in block.transactions:
        match ct.type:
            case ConfirmedTransaction.Type.AcceptedDeploy:
                ct: AcceptedDeploy
                tx: Transaction = ct.transaction
                tx: DeployTransaction
                transaction_type = "Deploy"
                transaction_state = "Accepted"
                if str(tx.id) == tx_id:
                    confirmed_transaction: AcceptedDeploy = ct
                    transaction = tx
                    index = ct.index
                    break
            case ConfirmedTransaction.Type.AcceptedExecute:
                ct: AcceptedExecute
                tx: Transaction = ct.transaction
                tx: ExecuteTransaction
                transaction_type = "Execute"
                transaction_state = "Accepted"
                if str(tx.id) == tx_id:
                    confirmed_transaction: AcceptedExecute = ct
                    transaction = tx
                    index = ct.index
                    break
            case ConfirmedTransaction.Type.RejectedDeploy:
                raise HTTPException(status_code=550, detail="Unsupported transaction type")
            case ConfirmedTransaction.Type.RejectedExecute:
                ct: RejectedExecute
                tx: Transaction = ct.transaction
                tx: FeeTransaction
                transaction_type = "Execute"
                transaction_state = "Rejected"
                if str(tx.id) == tx_id:
                    confirmed_transaction: RejectedExecute = ct
                    transaction = tx
                    index = ct.index
                    break
            case _:
                raise HTTPException(status_code=550, detail="Unsupported transaction type")
    if transaction is None:
        raise HTTPException(status_code=550, detail="Transaction not found in block")

    ctx = {
        "request": request,
        "tx_id": tx_id,
        "tx_id_trunc": str(tx_id)[:12] + "..." + str(tx_id)[-6:],
        "block": block,
        "index": index,
        "transaction": transaction,
        "type": transaction_type,
        "state": transaction_state,
    }

    if transaction.type == Transaction.Type.Deploy:
        transaction: DeployTransaction
        deployment: Deployment = transaction.deployment
        program: Program = deployment.program
        fee_transition = transaction.fee.transition
        ctx.update({
            "edition": int(deployment.edition),
            "program_id": str(program.id),
            "total_fee": int(fee_transition.inputs[1].plaintext.value.literal.primitive),
            "transitions": [{
                "transition_id": transaction.fee.transition.id,
                "action": await function_signature(db, str(fee_transition.program_id), str(fee_transition.function_name)),
            }],
        })
    elif transaction.type == Transaction.Type.Execute:
        transaction: ExecuteTransaction
        global_state_root = transaction.execution.global_state_root
        proof = transaction.execution.proof.value
        transitions = []
        for transition in transaction.execution.transitions:
            transition: Transition
            transitions.append({
                "transition_id": transition.id,
                "action": await function_signature(db, str(transition.program_id), str(transition.function_name)),
            })
        if transaction.additional_fee.value is not None:
            transition = transaction.additional_fee.value.transition
            total_fee = int(transition.inputs[1].plaintext.value.literal.primitive)
            transitions.append({
                "transition_id": transition.id,
                "action": await function_signature(db, str(transition.program_id), str(transition.function_name)),
            })
        else:
            total_fee = 0
        ctx.update({
            "global_state_root": global_state_root,
            "proof": proof,
            "proof_trunc": str(proof)[:30] + "..." + str(proof)[-30:] if proof else None,
            "total_fee": total_fee,
            "transitions": transitions,
        })
    elif transaction.type == Transaction.Type.Fee:
        transaction: FeeTransaction
        global_state_root = transaction.fee.global_state_root
        proof = transaction.fee.proof.value
        transitions = []
        rejected_transitions = []
        transition = transaction.fee.transition
        total_fee = int(transition.inputs[1].plaintext.value.literal.primitive)
        transitions.append({
            "transition_id": transition.id,
            "action": await function_signature(db, str(transition.program_id), str(transition.function_name)),
        })
        # noinspection PyUnboundLocalVariable
        if confirmed_transaction.type == ConfirmedTransaction.Type.RejectedExecute:
            confirmed_transaction: RejectedExecute
            # noinspection PyUnboundLocalVariable
            for transition in confirmed_transaction.rejected.transitions:
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
            "total_fee": total_fee,
            "transitions": transitions,
            "rejected_transitions": rejected_transitions,
        })

    return templates.TemplateResponse('transaction.jinja2', ctx, headers={'Cache-Control': 'public, max-age=3600'})


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
        if ct.type == ConfirmedTransaction.Type.AcceptedDeploy:
            ct: AcceptedDeploy
            tx: Transaction = ct.transaction
            tx: DeployTransaction
            state = "Accepted"
            if str(tx.fee.transition.id) == ts_id:
                transition = tx.fee.transition
                transaction_id = tx.id
                break
        elif ct.type == ConfirmedTransaction.Type.AcceptedExecute:
            ct: AcceptedExecute
            tx: Transaction = ct.transaction
            tx: ExecuteTransaction
            state = "Accepted"
            for ts in tx.execution.transitions:
                ts: Transition
                if str(ts.id) == ts_id:
                    transition = ts
                    transaction_id = tx.id
                    break
            if transaction_id is None and tx.additional_fee.value is not None:
                ts: Transition = tx.additional_fee.value.transition
                if str(ts.id) == ts_id:
                    transition = ts
                    transaction_id = tx.id
                    break
        elif ct.type == ConfirmedTransaction.Type.RejectedExecute:
            ct: RejectedExecute
            tx: Transaction = ct.transaction
            tx: FeeTransaction
            if str(tx.fee.transition.id) == ts_id:
                transition = tx.fee.transition
                transaction_id = tx.id
                state = "Accepted"
            else:
                for ts in ct.rejected.transitions:
                    ts: Transition
                    if str(ts.id) == ts_id:
                        transition = ts
                        transaction_id = tx.id
                        state = "Rejected"
                        break
    if transaction_id is None:
        raise HTTPException(status_code=550, detail="Transition not found in block")

    program_id = transition.program_id
    function_name = transition.function_name
    tpk = transition.tpk
    tcm = transition.tcm

    inputs = []
    for input_ in transition.inputs:
        input_: TransitionInput
        match input_.type:
            case TransitionInput.Type.Public:
                input_: PublicTransitionInput
                inputs.append({
                    "type": "Public",
                    "plaintext_hash": input_.plaintext_hash,
                    "plaintext": input_.plaintext.value,
                })
            case TransitionInput.Type.Private:
                input_: PrivateTransitionInput
                inputs.append({
                    "type": "Private",
                    "ciphertext_hash": input_.ciphertext_hash,
                    "ciphertext": input_.ciphertext.value,
                })
            case TransitionInput.Type.Record:
                input_: RecordTransitionInput
                inputs.append({
                    "type": "Record",
                    "serial_number": input_.serial_number,
                    "tag": input_.tag,
                })
            case TransitionInput.Type.ExternalRecord:
                input_: ExternalRecordTransitionInput
                inputs.append({
                    "type": "External record",
                    "commitment": input_.input_commitment,
                })

    outputs = []
    for output in transition.outputs:
        output: TransitionOutput
        match output.type:
            case TransitionOutput.Type.Public:
                output: PublicTransitionOutput
                outputs.append({
                    "type": "Public",
                    "plaintext_hash": output.plaintext_hash,
                    "plaintext": output.plaintext.value,
                })
            case TransitionOutput.Type.Private:
                output: PrivateTransitionOutput
                outputs.append({
                    "type": "Private",
                    "ciphertext_hash": output.ciphertext_hash,
                    "ciphertext": output.ciphertext.value,
                })
            case TransitionOutput.Type.Record:
                output: RecordTransitionOutput
                output_data = {
                    "type": "Record",
                    "commitment": output.commitment,
                    "checksum": output.checksum,
                    "record": output.record_ciphertext.value,
                }
                record: Record = output.record_ciphertext.value
                if record is not None:
                    record_data = {
                        "owner": record.owner,
                    }
                    data = []
                    for identifier, entry in record.data:
                        data.append((identifier, entry))
                    record_data["data"] = data
                    output_data["record_data"] = record_data
                outputs.append(output_data)
            case TransitionOutput.Type.ExternalRecord:
                output: ExternalRecordTransitionOutput
                outputs.append({
                    "type": "External record",
                    "commitment": output.commitment,
                })

    finalizes = []
    if transition.finalize.value is not None:
        for finalize in transition.finalize.value:
            finalize: Value
            match finalize.type:
                case Value.Type.Plaintext:
                    finalize: PlaintextValue
                    finalizes.append(str(finalize.plaintext))
                case Value.Type.Record:
                    raise NotImplementedError

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
    return templates.TemplateResponse('transition.jinja2', ctx, headers={'Cache-Control': 'public, max-age=3600'})


async def search_route(request: Request):
    db: Database = request.app.state.db
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
        return templates.TemplateResponse('search_result.jinja2', ctx, headers={'Cache-Control': 'public, max-age=15'})
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
        return templates.TemplateResponse('search_result.jinja2', ctx, headers={'Cache-Control': 'public, max-age=15'})
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
        return templates.TemplateResponse('search_result.jinja2', ctx, headers={'Cache-Control': 'public, max-age=15'})
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
        return templates.TemplateResponse('search_result.jinja2', ctx, headers={'Cache-Control': 'public, max-age=15'})
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
            return templates.TemplateResponse('search_result.jinja2', ctx, headers={'Cache-Control': 'public, max-age=15'})
    raise HTTPException(status_code=404, detail="Unknown object type or searching is not supported")


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
    total_pages = (total_blocks // 50) + 1
    if page < 1 or page > total_pages:
        raise HTTPException(status_code=400, detail="Invalid page")
    start = total_blocks - 50 * (page - 1)
    blocks = await db.get_blocks_range_fast(start, start - 50)

    maintenance, info = await out_of_sync_check(db)
    ctx = {
        "request": request,
        "blocks": blocks,
        "page": page,
        "total_pages": total_pages,
        "maintenance": maintenance,
        "info": info,
    }
    return templates.TemplateResponse('blocks.jinja2', ctx, headers={'Cache-Control': 'public, max-age=15'})
