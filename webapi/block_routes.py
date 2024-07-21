from typing import Any

from starlette.requests import Request

from db import Database
from webapi.utils import SJSONResponse, public_cache_seconds


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
    return SJSONResponse(recent_blocks)

@public_cache_seconds(5)
async def index_update_route(request: Request):
    db: Database = request.app.state.db
    last_block = request.query_params.get("last_block")
    if last_block is None:
        return SJSONResponse({"error": "Missing last_block parameter"}, status_code=400)
    try:
        last_block = int(last_block)
    except ValueError:
        return SJSONResponse({"error": "Invalid last_block parameter"}, status_code=400)
    if last_block < 0:
        return SJSONResponse({"error": "Negative last_block parameter"}, status_code=400)
    summary = await get_summary(db)
    result: dict[str, Any] = {"summary": summary}
    latest_height = await db.get_latest_height()
    if latest_height is None:
        return SJSONResponse({"error": "Database error"}, status_code=500)
    block_count = latest_height - last_block
    if block_count < 0:
        return SJSONResponse({"summary": summary})
    if block_count > 10:
        block_count = 10
    recent_blocks = await db.get_recent_blocks_fast(block_count)
    result["recent_blocks"] = recent_blocks
    return SJSONResponse(result)

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
        return SJSONResponse({"error": "Invalid page"}, status_code=400)
    total_blocks = await db.get_latest_height()
    if not total_blocks:
        return SJSONResponse({"error": "No blocks found"}, status_code=550)
    total_blocks += 1
    total_pages = (total_blocks // 20) + 1
    if page < 1 or page > total_pages:
        return SJSONResponse({"error": "Invalid page"}, status_code=400)
    start = total_blocks - 20 * (page - 1)
    blocks = await db.get_blocks_range_fast(start, start - 20)

    return SJSONResponse({"blocks": blocks, "total_blocks": total_blocks, "total_pages": total_pages})