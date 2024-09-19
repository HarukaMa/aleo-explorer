from io import BytesIO
from typing import cast

from starlette.requests import Request
from starlette.responses import JSONResponse

from aleo_types.vm_block import LiteralPlaintext, StructPlaintext, Value, PlaintextValue
from api.utils import parse_history_params
from db import Database


async def address_staking_route(request: Request):
    db: Database = request.app.state.db
    version = request.path_params["version"]
    if version <= 1:
        return JSONResponse({"error": "This endpoint is not supported in this version"}, status_code=400)
    address = request.path_params["address"]
    height = request.query_params.get("height")
    time_str = request.query_params.get("time")

    parse_result = await parse_history_params(db, height, time_str)
    if isinstance(parse_result, JSONResponse):
        return parse_result
    height, _, block_timestamp = parse_result

    value_bytes = await db.get_mapping_value_at_height("credits.aleo", "bonded", address, height)
    if value_bytes is None:
        return JSONResponse(None)
    value = cast(PlaintextValue, Value.load(BytesIO(value_bytes)))
    plaintext = cast(StructPlaintext, value.plaintext)
    validator = str(plaintext["validator"])
    amount = int(str(plaintext["microcredits"]).replace("u64", ""))

    return JSONResponse({
        "height": height,
        "timestamp": block_timestamp,
        "validator": validator,
        "amount": amount,
    })


async def address_delegated_route(request: Request):
    db: Database = request.app.state.db
    version = request.path_params["version"]
    if version <= 1:
        return JSONResponse({"error": "This endpoint is not supported in this version"}, status_code=400)
    address = request.path_params["address"]
    height = request.query_params.get("height")
    time_str = request.query_params.get("time")

    parse_result = await parse_history_params(db, height, time_str)
    if isinstance(parse_result, JSONResponse):
        return parse_result
    height, _, block_timestamp = parse_result

    value_bytes = await db.get_mapping_value_at_height("credits.aleo", "delegated", address, height)
    if value_bytes is None:
        return JSONResponse(None)
    value = cast(PlaintextValue, Value.load(BytesIO(value_bytes)))
    plaintext = cast(LiteralPlaintext, value.plaintext)
    amount = int(str(plaintext).replace("u64", ""))

    return JSONResponse({
        "height": height,
        "timestamp": block_timestamp,
        "amount": amount,
    })