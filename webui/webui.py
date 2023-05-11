import asyncio
import copy
import datetime
import logging
import multiprocessing
import os
import time
from decimal import Decimal

import uvicorn
from asgi_logger import AccessLoggerMiddleware
from starlette.applications import Starlette
from starlette.exceptions import HTTPException
from starlette.middleware import Middleware
from starlette.requests import Request
from starlette.responses import RedirectResponse, FileResponse
from starlette.routing import Route, Mount
from starlette.staticfiles import StaticFiles
from starlette.templating import Jinja2Templates

import disasm.aleo
from db import Database
# from node.light_node import LightNodeState
from node.light_node import LightNodeState
from node.types import u32, Transaction, Transition, ExecuteTransaction, TransitionInput, PrivateTransitionInput, \
    RecordTransitionInput, TransitionOutput, RecordTransitionOutput, Record, KZGProof, Proof, WitnessCommitments, \
    G1Affine, Ciphertext, Owner, Entry, DeployTransaction, Deployment, Program, PublicTransitionInput, \
    PublicTransitionOutput, PrivateTransitionOutput, Value, PlaintextValue, ExternalRecordTransitionInput, \
    ExternalRecordTransitionOutput, ConfirmedTransaction, AcceptedDeploy, AcceptedExecute


class UvicornServer(multiprocessing.Process):

    def __init__(self, config: uvicorn.Config):
        super().__init__()
        self.server = uvicorn.Server(config=config)
        self.config = config

    def stop(self):
        self.terminate()

    def run(self, *args, **kwargs):
        self.server.run()

templates = Jinja2Templates(directory='webui/templates', trim_blocks=True, lstrip_blocks=True)

def get_env(name):
    return os.environ.get(name)

def format_time(epoch):
    time_str = datetime.datetime.fromtimestamp(epoch, tz=datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    return f"""<span class="time">{time_str}</span>"""

def format_aleo_credit(mc):
    if mc == "-":
        return "-"
    return "{:,}".format(Decimal(mc) / 1_000_000)

def format_number(number):
    if isinstance(number, Decimal):
        return f"{number:,.2f}"
    return "{:,}".format(number)

templates.env.filters["get_env"] = get_env
templates.env.filters["format_time"] = format_time
templates.env.filters["format_aleo_credit"] = format_aleo_credit
templates.env.filters["format_number"] = format_number

credits_functions = {
    "mint": {
        "input": ["address", "u64"],
        "input_mode": ["public", "public"],
        "output": ["credits"],
        "output_mode": ["private"],
        "finalize": [],
    },
    "transfer": {
        "input": ["credits", "address", "u64"],
        "input_mode": ["private", "private", "private"],
        "output": ["credits", "credits"],
        "output_mode": ["private", "private"],
        "finalize": [],
    },
    "join": {
        "input": ["credits", "credits"],
        "input_mode": ["private", "private"],
        "output": ["credits"],
        "output_mode": ["private"],
        "finalize": [],
    },
    "split": {
        "input": ["credits", "u64"],
        "input_mode": ["private", "private"],
        "output": ["credits", "credits"],
        "output_mode": ["private", "private"],
        "finalize": [],
    },
    "fee": {
        "input": ["credits", "u64"],
        "input_mode": ["private", "public"],
        "output": ["credits"],
        "output_mode": ["private"],
        "finalize": [],
    },
}

async def out_of_sync_check():
    last_block = await db.get_latest_block()
    last_timestamp = last_block.header.metadata.timestamp
    now = int(time.time())
    maintenance_info = os.environ.get("MAINTENANCE_INFO")
    if now - last_timestamp > 120:
        if maintenance_info:
            return True, maintenance_info
        return False, get_relative_time(last_timestamp)
    return None, None

async def function_signature(program_id: str, function_name: str):
    data = await function_definition(program_id, function_name)
    if isinstance(data, str):
        return data
    inputs = []
    for i in range(len(data["input"])):
        name = data["input"][i]
        mode = data["input_mode"][i]
        if mode == "private":
            inputs.append(name)
        else:
            inputs.append(f"{mode} {name}")
    outputs = []
    for i in range(len(data["output"])):
        name = data["output"][i]
        mode = data["output_mode"][i]
        if mode == "private":
            outputs.append(name)
        else:
            outputs.append(f"{mode} {name}")
    finalizes = data["finalize"]
    result = f"{program_id}/{function_name}({', '.join(inputs)})"
    if len(outputs) == 1:
        result += f" -> {outputs[0]}"
    else:
        result += f" -> ({', '.join(outputs)})"
    if len(finalizes) != 0:
        result += f" finalize({', '.join(finalizes)})"
    return result

async def function_definition(program_id: str, function_name: str):
    if program_id == "credits.aleo":
        if function_name not in credits_functions:
            return f"Unknown program {program_id}"
        return credits_functions[function_name]
    else:
        data = await db.get_function_definition(program_id, function_name)
        if data is None:
            return f"Unknown function {program_id}/{function_name}"
        return data

async def index_route(request: Request):
    recent_blocks = await db.get_recent_blocks_fast()
    network_speed = await db.get_network_speed()
    maintenance, info = await out_of_sync_check()
    ctx = {
        "latest_block": await db.get_latest_block(),
        "request": request,
        "recent_blocks": recent_blocks,
        "network_speed": network_speed,
        "maintenance": maintenance,
        "info": info,
    }
    return templates.TemplateResponse('index.jinja2', ctx, headers={'Cache-Control': 'public, max-age=10'})


async def block_route(request: Request):
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
                    "type": "AcceptedDeploy",
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
                    "type": "AcceptedExecute",
                    "transitions_count": len(tx.execution.transitions) + bool(tx.additional_fee.value is not None),
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
    tx_id = request.query_params.get("id")
    if tx_id is None:
        raise HTTPException(status_code=400, detail="Missing transaction id")
    block = await db.get_block_from_transaction_id(tx_id)
    if block is None:
        raise HTTPException(status_code=404, detail="Transaction not found")

    transaction: DeployTransaction | ExecuteTransaction | None = None
    transaction_type = ""
    index = -1
    for ct in block.transactions:
        match ct.type:
            case ConfirmedTransaction.Type.AcceptedDeploy:
                ct: AcceptedDeploy
                tx: Transaction = ct.transaction
                tx: DeployTransaction
                transaction_type = "AcceptedDeploy"
                if str(tx.id) == tx_id:
                    transaction = tx
                    index = ct.index
                    break
            case ConfirmedTransaction.Type.AcceptedExecute:
                ct: AcceptedExecute
                tx: Transaction = ct.transaction
                tx: ExecuteTransaction
                transaction_type = "AcceptedExecute"
                if str(tx.id) == tx_id:
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
                "action": await function_signature(str(fee_transition.program_id), str(fee_transition.function_name)),
            }],
        })
    elif transaction.type == Transaction.Type.Execute:
        transaction: ExecuteTransaction
        global_state_root = transaction.execution.global_state_root
        inclusion_proof = transaction.execution.inclusion_proof.value
        transitions = []
        for transition in transaction.execution.transitions:
            transition: Transition
            transitions.append({
                "transition_id": transition.id,
                "action": await function_signature(str(transition.program_id), str(transition.function_name)),
            })
        if transaction.additional_fee.value is not None:
            transition = transaction.additional_fee.value.transition
            total_fee = int(transition.inputs[1].plaintext.value.literal.primitive)
            transitions.append({
                "transition_id": transition.id,
                "action": await function_signature(str(transition.program_id), str(transition.function_name)),
            })
        else:
            total_fee = 0
        ctx.update({
            "request": request,
            "tx_id": tx_id,
            "tx_id_trunc": str(tx_id)[:12] + "..." + str(tx_id)[-6:],
            "block": block,
            "transaction": transaction,
            "type": transaction_type,
            "global_state_root": global_state_root,
            "inclusion_proof": inclusion_proof,
            "total_fee": total_fee,
            "transitions": transitions,
        })
    return templates.TemplateResponse('transaction.jinja2', ctx, headers={'Cache-Control': 'public, max-age=3600'})


async def transition_route(request: Request):
    ts_id = request.query_params.get("id")
    if ts_id is None:
        raise HTTPException(status_code=400, detail="Missing transition id")
    block = await db.get_block_from_transition_id(ts_id)
    if block is None:
        raise HTTPException(status_code=404, detail="Transition not found")

    transaction_id = None
    transition = None
    for tx in block.transactions:
        if tx.type == Transaction.Type.Deploy:
            tx: DeployTransaction
            if str(tx.fee.transition.id) == ts_id:
                transition = tx.fee.transition
                transaction_id = tx.id
                break
        elif tx.type == Transaction.Type.Execute:
            tx: ExecuteTransaction
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
    if transaction_id is None:
        raise HTTPException(status_code=550, detail="Transition not found in block")

    program_id = transition.program_id
    function_name = transition.function_name
    tpk = transition.tpk
    tcm = transition.tcm
    proof = transition.proof

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
        "program_id": program_id,
        "function_name": function_name,
        "tpk": tpk,
        "tcm": tcm,
        "proof": proof,
        "function_signature": await function_signature(str(transition.program_id), str(transition.function_name)),
        "function_definition": await function_definition(str(transition.program_id), str(transition.function_name)),
        "inputs": inputs,
        "outputs": outputs,
        "finalizes": finalizes,
    }
    return templates.TemplateResponse('transition.jinja2', ctx, headers={'Cache-Control': 'public, max-age=3600'})


async def search_route(request: Request):
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


def get_relative_time(timestamp):
    now = time.time()
    delta = now - timestamp
    if delta < 60:
        return f"{int(delta)} seconds ago"
    delta = delta // 60
    if delta < 60:
        return f"{int(delta)} minutes ago"
    delta = delta // 60
    return f"{int(delta)} hours ago"


async def nodes_route(request: Request):
    nodes = lns.states
    res = {}
    for k, v in nodes.items():
        if "address" in v:
            res[k] = copy.deepcopy(v)
            res[k]["last_ping"] = get_relative_time(v["last_ping"])
    ctx = {
        "request": request,
        "nodes": res,
    }
    return templates.TemplateResponse('nodes.jinja2', ctx, headers={'Cache-Control': 'no-cache'})


async def blocks_route(request: Request):
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

    maintenance, info = await out_of_sync_check()
    ctx = {
        "request": request,
        "blocks": blocks,
        "page": page,
        "total_pages": total_pages,
        "maintenance": maintenance,
        "info": info,
    }
    return templates.TemplateResponse('blocks.jinja2', ctx, headers={'Cache-Control': 'public, max-age=15'})


async def programs_route(request: Request):
    try:
        page = request.query_params.get("p")
        if page is None:
            page = 1
        else:
            page = int(page)
    except:
        raise HTTPException(status_code=400, detail="Invalid page")
    no_helloworld = request.query_params.get("no_helloworld")
    try:
        no_helloworld = bool(int(no_helloworld))
    except:
        no_helloworld = False
    total_programs = await db.get_program_count(no_helloworld=no_helloworld)
    total_pages = (total_programs // 50) + 1
    if page < 1 or page > total_pages:
        raise HTTPException(status_code=400, detail="Invalid page")
    start = 50 * (page - 1)
    programs = await db.get_programs(start, start + 50, no_helloworld=no_helloworld)

    maintenance, info = await out_of_sync_check()
    ctx = {
        "request": request,
        "programs": programs,
        "page": page,
        "total_pages": total_pages,
        "no_helloworld": no_helloworld,
        "maintenance": maintenance,
        "info": info,
    }
    return templates.TemplateResponse('programs.jinja2', ctx, headers={'Cache-Control': 'public, max-age=15'})


async def program_route(request: Request):
    program_id = request.query_params.get("id")
    if program_id is None:
        raise HTTPException(status_code=400, detail="Missing program id")
    block = await db.get_block_by_program_id(program_id)
    if block is None:
        raise HTTPException(status_code=404, detail="Program not found")
    transaction: DeployTransaction | None = None
    for tx in block.transactions:
        if tx.type == Transaction.Type.Deploy:
            tx: DeployTransaction
            if str(tx.deployment.program.id) == program_id:
                transaction = tx
                break
    if transaction is None:
        raise HTTPException(status_code=550, detail="Deploy transaction not found")
    deployment: Deployment = transaction.deployment
    program: Program = deployment.program
    functions = []
    for f in program.functions.keys():
        functions.append((await function_signature(str(program.id), str(f))).split("/", 1)[-1])
    ctx = {
        "request": request,
        "program_id": str(program.id),
        "transaction_id": str(transaction.id),
        "owner": str(transaction.owner.address),
        "signature": str(transaction.owner.signature),
        "times_called": await db.get_program_called_times(program_id),
        "imports": list(map(lambda i: str(i.program_id), program.imports)),
        "mappings": list(map(str, program.mappings.keys())),
        "structs": list(map(str, program.structs.keys())),
        "records": list(map(str, program.records.keys())),
        "closures": list(map(str, program.closures.keys())),
        "functions": functions,
        "source": disasm.aleo.disassemble_program(program),
        "recent_calls": await db.get_program_calls(program_id, 0, 30),
        "similar_count": await db.get_program_similar_count(program_id),
    }
    return templates.TemplateResponse('program.jinja2', ctx, headers={'Cache-Control': 'public, max-age=15'})


async def similar_programs_route(request: Request):
    try:
        page = request.query_params.get("p")
        if page is None:
            page = 1
        else:
            page = int(page)
    except:
        raise HTTPException(status_code=400, detail="Invalid page")
    program_id = request.query_params.get("id")
    if program_id is None:
        raise HTTPException(status_code=400, detail="Missing program id")
    feature_hash = await db.get_program_feature_hash(program_id)
    if feature_hash is None:
        raise HTTPException(status_code=404, detail="Program not found")
    total_programs = await db.get_program_similar_count(program_id)
    total_pages = (total_programs // 50) + 1
    if page < 1 or page > total_pages:
        raise HTTPException(status_code=400, detail="Invalid page")
    start = 50 * (page - 1)
    programs = await db.get_programs_with_feature_hash(feature_hash, start, start + 50)

    maintenance, info = await out_of_sync_check()
    ctx = {
        "request": request,
        "program_id": program_id,
        "programs": programs,
        "page": page,
        "total_pages": total_pages,
        "maintenance": maintenance,
        "info": info,
    }
    return templates.TemplateResponse('similar_programs.jinja2', ctx, headers={'Cache-Control': 'public, max-age=15'})


async def leaderboard_route(request: Request):
    try:
        page = request.query_params.get("p")
        if page is None:
            page = 1
        else:
            page = int(page)
    except:
        raise HTTPException(status_code=400, detail="Invalid page")
    address_count = await db.get_leaderboard_size()
    total_pages = (address_count // 50) + 1
    if page < 1 or page > total_pages:
        raise HTTPException(status_code=400, detail="Invalid page")
    start = 50 * (page - 1)
    leaderboard_data = await db.get_leaderboard(start, start + 50)
    data = []
    for line in leaderboard_data:
        data.append({
            "address": line["address"],
            "total_rewards": line["total_reward"],
            "total_incentive": line["total_incentive"],
        })
    now = int(time.time())
    total_credit = await db.get_leaderboard_total()
    target_credit = 37_500_000_000_000
    ratio = total_credit / target_credit * 100
    maintenance, info = await out_of_sync_check()
    ctx = {
        "request": request,
        "leaderboard": data,
        "page": page,
        "total_pages": total_pages,
        "total_credit": total_credit,
        "target_credit": target_credit,
        "ratio": ratio,
        "now": now,
        "maintenance": maintenance,
        "info": info,
    }
    return templates.TemplateResponse('leaderboard.jinja2', ctx, headers={'Cache-Control': 'public, max-age=15'})


async def address_route(request: Request):
    address = request.query_params.get("a")
    if address is None:
        raise HTTPException(status_code=400, detail="Missing address")
    solutions = await db.get_recent_solutions_by_address(address)
    programs = await db.get_recent_programs_by_address(address)
    if len(solutions) == 0 and len(programs) == 0:
        raise HTTPException(status_code=404, detail="Address not found")
    if len(solutions) > 0:
        solution_count = await db.get_solution_count_by_address(address)
        total_rewards, total_incentive = await db.get_leaderboard_rewards_by_address(address)
        speed, interval = await db.get_address_speed(address)
    else:
        solution_count = 0
        total_rewards = 0
        total_incentive = 0
        speed = 0
        interval = 0
    program_count = await db.get_program_count_by_address(address)
    interval_text = {
        0: "never",
        900: "15 minutes",
        1800: "30 minutes",
        3600: "1 hour",
        14400: "4 hours",
        43200: "12 hours",
        86400: "1 day",
    }
    recent_solutions = []
    for solution in solutions:
        recent_solutions.append({
            "height": solution["height"],
            "timestamp": solution["timestamp"],
            "reward": solution["reward"],
            "nonce": solution["nonce"],
            "target": solution["target"],
            "target_sum": solution["target_sum"],
        })
    recent_programs = []
    for program in programs:
        program_block = await db.get_block_by_program_id(program)
        program_tx = None
        for tx in program_block.transactions.transactions:
            if tx.type == Transaction.Type.Deploy and str(tx.deployment.program.id) == program:
                program_tx = tx
                break
        if program_tx is None:
            raise HTTPException(status_code=550, detail="Program transaction not found")
        recent_programs.append({
            "program_id": program,
            "height": program_block.header.metadata.height,
            "timestamp": program_block.header.metadata.timestamp,
            "transaction_id": program_tx.id,
        })
    maintenance, info = await out_of_sync_check()
    ctx = {
        "request": request,
        "address": address,
        "address_trunc": address[:14] + "..." + address[-6:],
        "solutions": recent_solutions,
        "programs": recent_programs,
        "total_rewards": total_rewards,
        "total_incentive": total_incentive,
        "total_solutions": solution_count,
        "total_programs": program_count,
        "speed": speed,
        "timespan": interval_text[interval],
        "maintenance": maintenance,
        "info": info,
    }
    return templates.TemplateResponse('address.jinja2', ctx, headers={'Cache-Control': 'public, max-age=15'})


async def address_solution_route(request: Request):
    address = request.query_params.get("a")
    if address is None:
        raise HTTPException(status_code=400, detail="Missing address")
    try:
        page = request.query_params.get("p")
        if page is None:
            page = 1
        else:
            page = int(page)
    except:
        raise HTTPException(status_code=400, detail="Invalid page")
    solution_count = await db.get_solution_count_by_address(address)
    total_pages = (solution_count // 50) + 1
    if page < 1 or page > total_pages:
        raise HTTPException(status_code=400, detail="Invalid page")
    start = 50 * (page - 1)
    solutions = await db.get_solution_by_address(address, start, start + 50)
    data = []
    for solution in solutions:
        data.append({
            "height": solution["height"],
            "timestamp": solution["timestamp"],
            "reward": solution["reward"],
            "nonce": solution["nonce"],
            "target": solution["target"],
            "target_sum": solution["target_sum"],
        })
    maintenance, info = await out_of_sync_check()
    ctx = {
        "request": request,
        "address": address,
        "address_trunc": address[:14] + "..." + address[-6:],
        "solutions": data,
        "page": page,
        "total_pages": total_pages,
        "maintenance": maintenance,
        "info": info,
    }
    return templates.TemplateResponse('address_solution.jinja2', ctx, headers={'Cache-Control': 'public, max-age=15'})


def get_proof_data(proof: Proof):
    def G1Affine_to_html(affine: G1Affine, depth: int) -> str:
        return "{<br>" + "&nbsp;" * depth * 2 + f"x: {affine.x},<br>" + "&nbsp;" * depth * 2 + "y: Not implemented<br>" + "&nbsp;" * (depth - 1) * 2 + "}"

    data = {"Batch size": proof.batch_sizes}
    commitments = {}
    witness_commitments = proof.commitments.witness_commitments
    witness_commitment_list = []
    for witness_commitment in witness_commitments:
        witness_commitment: WitnessCommitments
        witness_commitment_str = f"{{<br>&nbsp;&nbsp;&nbsp;&nbsp;w: {G1Affine_to_html(witness_commitment.w.element, 3)},<br>" \
                                 f"&nbsp;&nbsp;&nbsp;&nbsp;z_a: {G1Affine_to_html(witness_commitment.z_a.element, 3)},<br>" \
                                 f"&nbsp;&nbsp;&nbsp;&nbsp;z_b: {G1Affine_to_html(witness_commitment.z_b.element, 3)}<br>&nbsp;&nbsp;}}"
        witness_commitment_list.append(witness_commitment_str)
    commitments["Witness commitments"] = "[<br>&nbsp;&nbsp;" + ",<br>&nbsp;&nbsp;".join(witness_commitment_list) + "<br>]"
    commitments["Mask polynomial commitment"] = "-" if proof.commitments.mask_poly.value is None else \
        G1Affine_to_html(proof.commitments.mask_poly.value.element, 1)
    commitments["g_1 commitment"] = G1Affine_to_html(proof.commitments.g_1.element, 1)
    commitments["h_1 commitment"] = G1Affine_to_html(proof.commitments.h_1.element, 1)
    commitments["g_a commitment"] = G1Affine_to_html(proof.commitments.g_a.element, 1)
    commitments["g_b commitment"] = G1Affine_to_html(proof.commitments.g_b.element, 1)
    commitments["g_c commitment"] = G1Affine_to_html(proof.commitments.g_c.element, 1)
    commitments["h_2 commitment"] = G1Affine_to_html(proof.commitments.h_2.element, 1)
    data["Commitments"] = commitments
    evaluations = {
        "z_b evaluations": "<br>".join(map(lambda x: str(x), proof.evaluations.z_b_evals)),
        "g_1 evaluation": str(proof.evaluations.g_1_eval),
        "g_a evaluation": str(proof.evaluations.g_a_eval),
        "g_b evaluation": str(proof.evaluations.g_b_eval),
        "g_c evaluation": str(proof.evaluations.g_c_eval),
    }
    data["Evaluations"] = evaluations
    msg = {
        "sum_a": str(proof.msg.sum_a),
        "sum_b": str(proof.msg.sum_b),
        "sum_c": str(proof.msg.sum_c),
    }
    data["Prover messages"] = msg
    pc_proof = {}
    proof_list = []
    for proof in proof.pc_proof.proof.proof:
        proof: KZGProof
        proof_str = f"{{<br>&nbsp;&nbsp;&nbsp;&nbsp;w: {G1Affine_to_html(proof.w, 3)},<br>" \
                    f"&nbsp;&nbsp;&nbsp;&nbsp;random_v: {str(proof.random_v.value)},<br>" \
                    f"&nbsp;&nbsp;}}"
        proof_list.append(proof_str)
    pc_proof["Proof"] = "[<br>&nbsp;&nbsp;" + ",<br>&nbsp;&nbsp;".join(proof_list) + "<br>]"
    data["Polynomial commitment proof"] = pc_proof

    return data

def get_ciphertext_data(ciphertext: Ciphertext):
    return {
        "ciphertext": "<br>".join(map(lambda x: str(x), ciphertext.ciphertext)),
    }


async def advanced_route(request: Request):
    scope = request.query_params.get("scope")
    if scope is None:
        raise HTTPException(status_code=400, detail="Missing scope")
    if scope not in ["transaction", "transition"]:
        raise HTTPException(status_code=400, detail="Invalid scope")
    obj = request.query_params.get("object")
    if obj is None:
        raise HTTPException(status_code=400, detail="Missing object")
    data = {}
    object_type = ""
    id_ = ""
    if scope == "transaction":
        transaction: Transaction
        block = await db.get_block_from_transaction_id(obj)
        if block is None:
            raise HTTPException(status_code=404, detail="Transaction not found")
        for tx in block.transactions:
            match tx.type:
                case Transaction.Type.Execute:
                    tx: ExecuteTransaction
                    transaction: ExecuteTransaction
                    if str(tx.id) == obj:
                        transaction = tx
                        break
                case _:
                    raise HTTPException(status_code=400, detail="Invalid transaction type")
        type_ = request.query_params.get("type")
        if type_ is None:
            raise HTTPException(status_code=400, detail="Missing type")
        if type_ != "inclusion_proof":
            raise HTTPException(status_code=400, detail="Invalid type")
        object_type = "Inclusion proof"
        # noinspection PyUnboundLocalVariable
        inclusion_proof: Proof = transaction.execution.inclusion_proof.value
        if inclusion_proof is None:
            raise HTTPException(status_code=400, detail="Transaction doesn't have an inclusion proof")
        id_ = str(inclusion_proof)
        data = get_proof_data(inclusion_proof)

    elif scope == "transition":
        transition: Transition | None = None
        block = await db.get_block_from_transition_id(obj)
        if block is None:
            raise HTTPException(status_code=404, detail="Transition not found")
        for tx in block.transactions:
            if tx.type == Transaction.Type.Execute:
                tx: ExecuteTransaction
                for ts in tx.execution.transitions:
                    if str(ts.id) == obj:
                        transition = ts
                        break
            elif tx.type == Transaction.Type.Deploy:
                tx: DeployTransaction
                if str(tx.fee.transition.id) == obj:
                    transition = tx.fee.transition
                    break
        type_ = request.query_params.get("type")
        if type_ is None:
            raise HTTPException(status_code=400, detail="Missing type")
        if type_ == "proof":
            object_type = "Proof"
            id_ = str(transition.proof)
            data = get_proof_data(transition.proof)
        elif type_.startswith("input") or type_.startswith("output"):
            object_type = "Ciphertext"
            index = request.query_params.get("index")
            if index is None:
                raise HTTPException(status_code=400, detail="Missing index")
            index = int(index)
            if type_.startswith("input"):
                if index < 0 or index >= len(transition.inputs):
                    raise HTTPException(status_code=400, detail="Invalid index")
                input_ = transition.inputs[index]
                if type_ == "input_private":
                    if input_.type != TransitionInput.Type.Private:
                        raise HTTPException(status_code=400, detail="Invalid input type")
                    input_: PrivateTransitionInput
                    if input_.ciphertext.value is None:
                        raise HTTPException(status_code=400, detail="Input doesn't have a ciphertext")
                    id_ = str(input_.ciphertext.value)
                    data = get_ciphertext_data(input_.ciphertext.value)
                else:
                    raise HTTPException(status_code=550, detail="Not Implemented")
            else:
                if index < 0 or index >= len(transition.outputs):
                    raise HTTPException(status_code=400, detail="Invalid index")
                output = transition.outputs[index]
                if type_ == "output_record":
                    if output.type != TransitionOutput.Type.Record:
                        raise HTTPException(status_code=400, detail="Invalid output type")
                    output: RecordTransitionOutput
                    if output.record_ciphertext.value is None:
                        raise HTTPException(status_code=400, detail="Output doesn't have a record ciphertext")
                    record: Record = output.record_ciphertext.value
                    field = request.query_params.get("field")
                    if field is None:
                        raise HTTPException(status_code=400, detail="Missing field")
                    if field == "owner":
                        if record.owner.type == Owner.Type.Public:
                            raise HTTPException(status_code=400, detail="Owner is public")
                        if record.owner.Private != Ciphertext:
                            raise HTTPException(status_code=400, detail="Owner is not a ciphertext")
                        # noinspection PyUnresolvedReferences
                        id_ = str(record.owner.owner)
                        # noinspection PyUnresolvedReferences
                        data = get_ciphertext_data(record.owner.owner)
                    else:
                        for identifier, entry in record.data:
                            if str(identifier) == field:
                                if entry.type == Entry.Type.Public:
                                    raise HTTPException(status_code=400, detail="Entry is public")
                                if entry.Private != Ciphertext:
                                    raise HTTPException(status_code=400, detail="Entry is not a ciphertext")
                                id_ = str(entry.plaintext)
                                data = get_ciphertext_data(entry.plaintext)
                                break
                    if id_ == "":
                        raise HTTPException(status_code=400, detail="Invalid field")
                else:
                    raise HTTPException(status_code=550, detail="Not Implemented")
        else:
            raise HTTPException(status_code=400, detail="Invalid type")

    id_prefix_size = id_.index("1")

    ctx = {
        "request": request,
        "type": object_type,
        "id": id_,
        "id_trunc": id_[:id_prefix_size + 6] + "..." + id_[-6:],
        "data": data,
    }
    return templates.TemplateResponse('advanced.jinja2', ctx, headers={'Cache-Control': 'public, max-age=3600'})


async def faq_route(request: Request):
    ctx = {
        "request": request,
    }
    return templates.TemplateResponse('faq.jinja2', ctx, headers={'Cache-Control': 'public, max-age=3600'})


async def privacy_route(request: Request):
    ctx = {
        "request": request,
    }
    return templates.TemplateResponse('privacy.jinja2', ctx, headers={'Cache-Control': 'public, max-age=3600'})


async def calc_route(request: Request):
    proof_target = (await db.get_latest_block()).header.metadata.proof_target
    ctx = {
        "request": request,
        "proof_target": proof_target,
    }
    return templates.TemplateResponse('calc.jinja2', ctx, headers={'Cache-Control': 'public, max-age=60'})


async def robots_route(_: Request):
    return FileResponse("webui/robots.txt", headers={'Cache-Control': 'public, max-age=3600'})


async def bad_request(request: Request, exc: HTTPException):
    return templates.TemplateResponse('400.jinja2', {'request': request, "exc": exc}, status_code=400)


async def not_found(request: Request, exc: HTTPException):
    return templates.TemplateResponse('404.jinja2', {'request': request, "exc": exc}, status_code=404)


async def internal_error(request: Request, exc: HTTPException):
    return templates.TemplateResponse('500.jinja2', {'request': request, "exc": exc}, status_code=500)


async def cloudflare_error_page(request: Request):
    placeholder = request.query_params.get("placeholder")
    return templates.TemplateResponse('cf.jinja2', {'request': request, "placeholder": placeholder})


routes = [
    Route("/", index_route),
    # Blockchain
    Route("/block", block_route),
    Route("/transaction", transaction_route),
    Route("/transition", transition_route),
    Route("/search", search_route),
    Route("/nodes", nodes_route),
    Route("/blocks", blocks_route),
    Route("/programs", programs_route),
    Route("/program", program_route),
    Route("/similar_programs", similar_programs_route),
    # Proving
    Route("/calc", calc_route),
    Route("/leaderboard", leaderboard_route),
    Route("/address", address_route),
    Route("/address_solution", address_solution_route),
    # Other
    Route("/advanced", advanced_route),
    Route("/faq", faq_route),
    Route("/privacy", privacy_route),
    Route("/robots.txt", robots_route),
    Route("/cf", cloudflare_error_page),
    Mount("/static", StaticFiles(directory="webui/static"), name="static"),
]

exc_handlers = {
    400: bad_request,
    404: not_found,
    550: internal_error,
}

async def startup():
    async def noop(_): pass

    global db
    # different thread so need to get a new database instance
    db = Database(server=os.environ["DB_HOST"], user=os.environ["DB_USER"], password=os.environ["DB_PASS"],
                  database=os.environ["DB_DATABASE"], schema=os.environ["DB_SCHEMA"],
                  message_callback=noop)
    await db.connect()


AccessLoggerMiddleware.DEFAULT_FORMAT = '\033[92mACCESS\033[0m: \033[94m%(client_addr)s\033[0m - - %(t)s \033[96m"%(request_line)s"\033[0m \033[93m%(s)s\033[0m %(B)s "%(f)s" "%(a)s" %(L)s'
# noinspection PyTypeChecker
app = Starlette(
    debug=True if os.environ.get("DEBUG") else False,
    routes=routes,
    on_startup=[startup],
    exception_handlers=exc_handlers,
    middleware=[Middleware(AccessLoggerMiddleware)]
)
lns: LightNodeState | None = None


async def run(_):
    config = uvicorn.Config("webui:app", reload=True, log_level="info", port=int(os.environ.get("PORT", 8000)))
    logging.getLogger("uvicorn.access").handlers = []
    server = UvicornServer(config=config)
    global lns
    # lns = light_node_state

    server.start()
    while True:
        await asyncio.sleep(3600)
