import asyncio
import contextlib
import copy
import datetime
import os
import threading
import time
from decimal import Decimal

import uvicorn
from starlette.applications import Starlette
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import RedirectResponse
from starlette.routing import Route
from starlette.templating import Jinja2Templates

from db import Database
# from node.light_node import LightNodeState
from node.types import u32, Transaction, Transition, ExecuteTransaction, Block


class Server(uvicorn.Server):
    # https://stackoverflow.com/a/64521239
    def install_signal_handlers(self):
        pass

    @contextlib.contextmanager
    def run_in_thread(self):
        thread = threading.Thread(target=self.run)
        thread.start()
        try:
            while not self.started:
                time.sleep(1e-3)
            yield
        finally:
            self.should_exit = True
            thread.join()


templates = Jinja2Templates(directory='webui/templates')

def get_env(name):
    return os.environ.get(name)

def format_time(epoch):
    return datetime.datetime.fromtimestamp(epoch, tz=datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def format_aleo_credit(gates):
    if gates == "-":
        return "-"
    return str(Decimal(gates) / 1_000_000)

templates.env.filters["get_env"] = get_env
templates.env.filters["format_time"] = format_time
templates.env.filters["format_aleo_credit"] = format_aleo_credit

async def get_block_list_info(block: Block) -> dict:
    b = {
        "timestamp": block.header.metadata.timestamp,
        "height": block.header.metadata.height,
        "transactions": len(block.transactions.transactions),
        "validator": "Not implemented", # await db.get_miner_from_block_hash(block.block_hash),
        "block_hash": block.block_hash,
    }
    reward = await db.get_block_coinbase_reward_by_height(block.header.metadata.height)
    if reward is not None:
        b["coinbase_rewards"] = str(reward // 2)
        b["coinbase_solutions"] = str(len(block.coinbase.value.partial_solutions))
    else:
        b["coinbase_rewards"] = "-"
        b["coinbase_solutions"] = "-"
    return b

async def index_route(request: Request):
    recent_blocks = await db.get_recent_blocks()
    data = []
    for block in recent_blocks:
        data.append(await get_block_list_info(block))
    ctx = {
        "latest_block": await db.get_latest_block(),
        "request": request,
        "recent_blocks": data,
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
        is_canonical = True
        block_hash = block.block_hash
    else:
        block = await db.get_block_by_hash(block_hash)
        if block is None:
            raise HTTPException(status_code=404, detail="Block not found")
        height = block.header.metadata.height
    height = int(height)
    latest_block_height = await db.get_latest_height()
    confirmations = latest_block_height - height + 1

    mining_reward = 0
    fee = 0
    txs = []
    for tx in block.transactions.transactions:
        tx: Transaction
        match tx.type:
            case Transaction.Type.Deploy:
                raise NotImplementedError
            case Transaction.Type.Execute:
                tx: ExecuteTransaction
                t = {
                    "tx_id": tx.id,
                }
                balance = 0
                if balance >= 0:
                    fee += balance
                txs.append(t)

    ctx = {
        "request": request,
        "block": block,
        "block_hash_trunc": str(block_hash)[:12] + "..." + str(block_hash)[-6:],
        "confirmations": confirmations,
        "validator": "Not implemented", # await db.get_miner_from_block_hash(block.block_hash),
        "pow_reward": mining_reward,
        "transactions": txs,
        "fee": fee,
    }
    return templates.TemplateResponse('block.jinja2', ctx, headers={'Cache-Control': 'public, max-age=30'})


async def transaction_route(request: Request):
    tx_id = request.query_params.get("id")
    if tx_id is None:
        raise HTTPException(status_code=400, detail="Missing transaction id")
    block = await db.get_block_from_transaction_id(tx_id)
    if block is None:
        raise HTTPException(status_code=404, detail="Transaction not found")

    latest_block_height = await db.get_latest_height()
    confirmations = latest_block_height - block.header.metadata.height + 1

    transaction = None
    for tx in block.transactions.transactions:
        match tx.type:
            case Transaction.Type.Deploy:
                raise NotImplementedError
            case Transaction.Type.Execute:
                tx: ExecuteTransaction
                if str(tx.id) == tx_id:
                    transaction = tx
                    break
    if transaction is None:
        raise HTTPException(status_code=550, detail="Transaction not found in block")

    ctx = {
        "request": request,
        "tx_id": tx_id,
        "tx_id_trunc": str(tx_id)[:12] + "..." + str(tx_id)[-6:],
        "block": block,
        "confirmations": confirmations,
        "transaction": transaction,
    }
    return templates.TemplateResponse('transaction.jinja2', ctx, headers={'Cache-Control': 'public, max-age=30'})


async def transition_route(request: Request):
    ts_id = request.query_params.get("id")
    if ts_id is None:
        raise HTTPException(status_code=400, detail="Missing transition id")
    block = await db.get_block_from_transition_id(ts_id)
    if block is None:
        raise HTTPException(status_code=404, detail="Transition not found")

    transaction_id = None
    transition = None
    for tx in block.transactions.transactions:
        tx: Transaction
        for ts in tx.transitions:
            ts: Transition
            if str(ts.transition_id) == ts_id:
                transition = ts
                transaction_id = tx.transaction_id
                break
    if transaction_id is None:
        raise HTTPException(status_code=550, detail="Transition not found in block")

    public_record = [False, False]
    records = [None, None]
    is_dummy = [False, False]
    for event in transition.events:
        event: Event
        if event.type == Event.Type.RecordViewKey:
            public_record[event.event.index] = True
            record = await db.get_record_from_commitment(transition.commitments[event.event.index])
            if record.value == 0 and record.payload.is_empty() and record.program_id == Testnet2.noop_program_id:
                is_dummy[event.event.index] = True
            records[event.event.index] = record

    ctx = {
        "request": request,
        "ts_id": ts_id,
        "ts_id_trunc": str(ts_id)[:12] + "..." + str(ts_id)[-6:],
        "transaction_id": transaction_id,
        "transition": transition,
        "public_record": public_record,
        "records": records,
        "is_dummy": is_dummy,
        "noop_program_id": Testnet2.noop_program_id,
    }
    return templates.TemplateResponse('transition.jinja2', ctx, headers={'Cache-Control': 'public, max-age=900'})


async def search_route(request: Request):
    query = request.query_params.get("q")
    if query is None:
        raise HTTPException(status_code=400, detail="Missing query")
    query = query.lower()
    try:
        height = int(query)
        return RedirectResponse(f"/block?h={height}", status_code=302)
    except ValueError:
        pass
    if query.startswith("ab1"):
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
    nodes = copy.deepcopy(lns.states)
    res = {}
    for k, v in nodes.items():
        if "status" in v:
            res[k] = v
            res[k]["last_ping"] = get_relative_time(v["last_ping"])
    latest_height = await db.get_latest_canonical_height()
    latest_weight = await db.get_latest_canonical_weight()
    ctx = {
        "request": request,
        "nodes": res,
        "latest_height": latest_height,
        "latest_weight": latest_weight,
    }
    return templates.TemplateResponse('nodes.jinja2', ctx, headers={'Cache-Control': 'no-cache'})


async def orphan_route(request: Request):
    height = request.query_params.get("h")
    if height is None:
        raise HTTPException(status_code=400, detail="Missing height")
    blocks = await db.get_orphaned_blocks_on_height_fast(int(height))
    data = []
    for block in blocks:
        owner = await db.get_miner_from_block_hash(block["block_hash"])
        b = {
            "timestamp": block["timestamp"],
            "height": block["height"],
            "transactions": block["transaction_count"],
            "transitions": block["transition_count"],
            "owner": owner,
            "owner_trunc": owner[:14] + "..." + owner[-6:],
            "block_hash": block["block_hash"],
            "block_hash_trunc": block["block_hash"][:12] + "..." + block["block_hash"][-6:],
        }
        data.append(b)
    ctx = {
        "request": request,
        "blocks": data,
        "height": height,
    }
    return templates.TemplateResponse('orphan.jinja2', ctx, headers={'Cache-Control': 'public, max-age=15'})


async def blocks_route(request: Request):
    try:
        page = request.query_params.get("p")
        if page is None:
            page = 1
        else:
            page = int(page)
    except:
        raise HTTPException(status_code=400, detail="Invalid page")
    total_blocks = await db.get_latest_canonical_height()
    total_pages = (total_blocks // 50) + 1
    if page < 1 or page > total_pages:
        raise HTTPException(status_code=400, detail="Invalid page")
    start = total_blocks - 50 * (page - 1)
    blocks = await db.get_blocks_range_fast(start, start - 50)
    data = []
    for block in blocks:
        owner = await db.get_miner_from_block_hash(block["block_hash"])
        b = {
            "timestamp": block["timestamp"],
            "height": block["height"],
            "transactions": block["transaction_count"],
            "transitions": block["transition_count"],
            "owner": owner,
            "owner_trunc": owner[:14] + "..." + owner[-6:],
            "block_hash": block["block_hash"],
            "block_hash_trunc": block["block_hash"][:12] + "..." + block["block_hash"][-6:],
            "orphan_count": block["orphan_count"],
        }
        data.append(b)
    ctx = {
        "request": request,
        "blocks": data,
        "page": page,
        "total_pages": total_pages,
    }
    return templates.TemplateResponse('blocks.jinja2', ctx, headers={'Cache-Control': 'public, max-age=15'})


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
        })
    ctx = {
        "request": request,
        "leaderboard": data,
        "page": page,
        "total_pages": total_pages,
    }
    return templates.TemplateResponse('leaderboard.jinja2', ctx, headers={'Cache-Control': 'public, max-age=15'})



async def bad_request(request: Request, exc: HTTPException):
    return templates.TemplateResponse('400.jinja2', {'request': request, "exc": exc}, status_code=400)


async def not_found(request: Request, exc: HTTPException):
    return templates.TemplateResponse('404.jinja2', {'request': request, "exc": exc}, status_code=404)


async def internal_error(request: Request, exc: HTTPException):
    return templates.TemplateResponse('500.jinja2', {'request': request, "exc": exc}, status_code=500)


routes = [
    Route("/", index_route),
    Route("/block", block_route),
    Route("/transaction", transaction_route),
    # Route("/transition", transition_route),
    Route("/search", search_route),
    # Route("/nodes", nodes_route),
    Route("/orphan", orphan_route),
    Route("/blocks", blocks_route),
    # Route("/miner", miner_stats),
    # Route("/calc", calc),
    Route("/leaderboard", leaderboard_route),
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


# noinspection PyTypeChecker
app = Starlette(debug=True if os.environ.get("DEBUG") else False, routes=routes, on_startup=[startup], exception_handlers=exc_handlers)
db: Database
# lns: LightNodeState | None = None


async def run():
    config = uvicorn.Config("webui:app", reload=True, log_level="info")
    server = Server(config=config)
    # global lns
    # lns = light_node_state

    with server.run_in_thread():
        while True:
            await asyncio.sleep(3600)
