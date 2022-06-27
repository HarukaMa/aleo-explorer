import asyncio
import contextlib
import copy
import os
import threading
import time

import uvicorn
from starlette.applications import Starlette
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import RedirectResponse
from starlette.routing import Route
from starlette.templating import Jinja2Templates

from db import Database
from node.light_node import LightNodeState
from node.testnet2 import Testnet2
from node.type import u32, Transaction, Transition, Event, AleoAmount


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


async def index_route(request: Request):
    recent_blocks = await db.get_recent_canonical_blocks_fast()
    data = []
    for block in recent_blocks:
        b = {
            "timestamp": block["timestamp"],
            "height": block["height"],
            "transactions": block["transaction_count"],
            "transitions": block["transition_count"],
            "owner": await db.get_miner_from_block_hash(block["block_hash"]),
            "block_hash": block["block_hash"],
        }
        data.append(b)
    ctx = {
        "latest_block": await db.get_latest_canonical_block_fast(),
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
        block = await db.get_canonical_block_by_height(u32(int(height)))
        if block is None:
            raise HTTPException(status_code=404, detail="Block not found")
        is_canonical = True
    else:
        block = await db.get_block_by_hash(block_hash)
        if block is None:
            raise HTTPException(status_code=404, detail="Block not found")
        is_canonical = await db.is_block_hash_canonical(block.block_hash)
    latest_block_height = await db.get_latest_canonical_height()
    confirmations = latest_block_height - block.header.metadata.height + 1
    ledger_root = await db.get_ledger_root_from_block_hash(block.block_hash)

    testnet2_bug = False
    mining_reward = 0
    fee = 0
    txs = []
    for tx in block.transactions.transactions:
        tx: Transaction
        if str(tx.ledger_root) in [
            "al1gesxhq6vwwa2xx3uh8w5pcfsg7zh942c3tlqhdn5c8wt8lh5tvqqpu882x",
            "al1c2agd5u90kpvcjvk70rgrhnkagfw4kfgusym9t0mjyr8yl0m6qyqez0q6j",
            "al1cp6qpzgt0elcpyk95p9vfme7p29ytp0rccnctx602fdc9uf2zv9qjtx35r",
            "al1tm27a5wukgxv6ks5fr7aa90zsvstu2ruplxkp5tena3tae8nxgfqldxqec",
            "al1fyspkzz7uq594k5wkmktwujlawkl0tf0w3tatarpwq5gs37lj5rs06f30h",
            "al1rwsj7nydhunppu268aqdaxd465rel9wx9wrqmamtymmrxt3j0ypsg25usd",
        ]:
            testnet2_bug = True
        t = {
            "tx_id": tx.transaction_id,
            "transitions": len(tx.transitions),
        }
        balance = 0
        for ts in tx.transitions:
            ts: Transition
            balance += ts.value_balance
            if ts.value_balance < 0:
                mining_reward = -ts.value_balance
        t["balance"] = balance
        if balance >= 0:
            fee += balance
        txs.append(t)
    height = block.header.metadata.height
    if height <= 4730400:
        standard_mining_reward = AleoAmount(100000000)
    elif height <= 9460800:
        standard_mining_reward = AleoAmount(50000000)
    else:
        standard_mining_reward = AleoAmount(25000000)

    ctx = {
        "request": request,
        "block": block,
        "is_canonical": is_canonical,
        "confirmations": confirmations,
        "ledger_root": ledger_root,
        "owner": await db.get_miner_from_block_hash(block.block_hash),
        "testnet2_bug": testnet2_bug,
        "mining_reward": AleoAmount(mining_reward),
        "transactions": txs,
        "fee": AleoAmount(fee),
        "burned": standard_mining_reward + fee - mining_reward,
    }
    return templates.TemplateResponse('block.jinja2', ctx, headers={'Cache-Control': 'public, max-age=30'})


async def transaction_route(request: Request):
    tx_id = request.query_params.get("id")
    if tx_id is None:
        raise HTTPException(status_code=400, detail="Missing transaction id")
    block = await db.get_best_block_from_transaction_id(tx_id)
    if block is None:
        raise HTTPException(status_code=404, detail="Transaction not found")
    is_canonical = await db.is_block_hash_canonical(block.block_hash)

    latest_block_height = await db.get_latest_canonical_height()
    confirmations = latest_block_height - block.header.metadata.height + 1

    transaction = None
    for tx in block.transactions.transactions:
        tx: Transaction
        if str(tx.transaction_id) == tx_id:
            transaction = tx
            break
    if transaction is None:
        raise HTTPException(status_code=550, detail="Transaction not found in block")

    ctx = {
        "request": request,
        "tx_id": tx_id,
        "tx_id_trunc": str(tx_id)[:9] + "..." + str(tx_id)[-6:],
        "block": block,
        "is_canonical": is_canonical,
        "confirmations": confirmations,
        "transaction": transaction,
    }
    return templates.TemplateResponse('transaction.jinja2', ctx, headers={'Cache-Control': 'public, max-age=30'})

async def transition_route(request: Request):
    ts_id = request.query_params.get("id")
    if ts_id is None:
        raise HTTPException(status_code=400, detail="Missing transition id")
    block = await db.get_best_block_from_transition_id(ts_id)
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
        "ts_id_trunc": str(ts_id)[:9] + "..." + str(ts_id)[-6:],
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
        return templates.TemplateResponse('search_result.jinja2', ctx, headers={'Cache-Control': 'public, max-age=30'})
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
        return templates.TemplateResponse('search_result.jinja2', ctx, headers={'Cache-Control': 'public, max-age=30'})
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
        return templates.TemplateResponse('search_result.jinja2', ctx, headers={'Cache-Control': 'public, max-age=30'})
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
    Route("/transition", transition_route),
    Route("/search", search_route),
    Route("/nodes", nodes_route),
    # Route("/miner", miner_stats),
    # Route("/calc", calc),
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
app = Starlette(debug=True, routes=routes, on_startup=[startup], exception_handlers=exc_handlers)
db: Database
lns: LightNodeState | None = None


async def run(light_node_state: LightNodeState):
    config = uvicorn.Config("webui:app", reload=True, log_level="info")
    server = Server(config=config)
    global lns
    lns = light_node_state

    with server.run_in_thread():
        while True:
            await asyncio.sleep(3600)
