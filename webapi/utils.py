import asyncio
import os
import time
from typing import Any

import aiohttp
import simplejson
from starlette.responses import Response

from db import Database


class SJSONResponse(Response):
    media_type = "application/json"

    def render(self, content: Any):
        return simplejson.dumps(
            content,
            ensure_ascii=False,
            allow_nan=False,
            indent=None,
            separators=(",", ":"),
            use_decimal=True
        ).encode("utf-8")

async def get_remote_height(session: aiohttp.ClientSession, rpc_root: str) -> str:
    try:
        async with session.get(f"{rpc_root}/testnet/latest/height") as resp:
            if resp.status == 200:
                remote_height = await resp.text()
            else:
                remote_height = "?"
    except:
        remote_height = "?"
    return remote_height


async def out_of_sync_check(session: aiohttp.ClientSession, db: Database):
    last_timestamp, last_height = await asyncio.gather(
        db.get_latest_block_timestamp(),
        db.get_latest_height()
    )
    now = int(time.time())
    maintenance_info = os.environ.get("MAINTENANCE_INFO")
    out_of_sync = now - last_timestamp > 120
    node_height = None
    reference_height = None
    if out_of_sync:
        if rpc_root := os.environ.get("RPC_URL_ROOT"):
            node_height = await get_remote_height(session, rpc_root)
        if ref_rpc_root := os.environ.get("REF_RPC_URL_ROOT"):
            reference_height = await get_remote_height(session, ref_rpc_root)

    return {
        "out_of_sync": out_of_sync,
        "maintenance_info": maintenance_info,
        "explorer_height": last_height,
        "node_height": node_height,
        "reference_height": reference_height,
    }


async def function_signature(db: Database, program_id: str, function_name: str):
    data = await function_definition(db, program_id, function_name)
    if isinstance(data, str):
        return data
    inputs: list[str] = []
    for i in range(len(data["input"])):
        name = data["input"][i]
        mode = data["input_mode"][i]
        if mode == "private":
            inputs.append(name)
        else:
            inputs.append(f"{mode} {name}")
    outputs: list[str] = []
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

async def function_definition(db: Database, program_id: str, function_name: str):
    data = await db.get_function_definition(program_id, function_name)
    if data is None:
        return f"Unknown function {program_id}/{function_name}"
    return data