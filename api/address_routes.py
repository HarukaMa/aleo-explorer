import datetime
from io import BytesIO
from typing import cast

from starlette.requests import Request
from starlette.responses import JSONResponse

from aleo_types.vm_block import StructPlaintext, Value, PlaintextValue
from db import Database


async def address_staking_route(request: Request):
    db: Database = request.app.state.db
    version = request.path_params["version"]
    if version <= 1:
        return JSONResponse({"error": "This endpoint is not supported in this version"}, status_code=400)
    address = request.path_params["address"]
    height = request.query_params.get("height")
    time_str = request.query_params.get("time")

    if height is not None and time_str is not None:
        return JSONResponse({"error": "Only one of height or time can be specified"}, status_code=400)

    if height is not None:
        try:
            height = int(height)
        except ValueError:
            return JSONResponse({"error": "Invalid height"}, status_code=400)
        block = await db.get_block_by_height(height)
        if block is None:
            return JSONResponse({"error": "Invalid height"}, status_code=400)
        block_timestamp = block.header.metadata.timestamp
    elif time_str is not None:
        try:
            time = int(time_str)
        except ValueError:
            try:
                time = int(datetime.datetime.fromisoformat(time_str).timestamp())
            except ValueError:
                return JSONResponse({"error": "Invalid time"}, status_code=400)
        block = await db.get_block_from_timestamp(time)
        if block is None:
            return JSONResponse({"error": "No block found for the specified time"}, status_code=404)
        height = block.height
        block_timestamp = block.header.metadata.timestamp
    else:
        height = await db.get_latest_height()
        if height is None:
            return JSONResponse({"error": "Database uninitialized"}, status_code=500)
        block = await db.get_block_by_height(height)
        if block is None:
            return JSONResponse({"error": "Database uninitialized"}, status_code=500)
        block_timestamp = block.header.metadata.timestamp

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
