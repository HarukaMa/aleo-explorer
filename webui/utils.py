
import os
import time

import aiohttp

from db import Database


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


async def out_of_sync_check(db: Database):
    last_block = await db.get_latest_block()
    last_timestamp = last_block.header.metadata.timestamp
    now = int(time.time())
    maintenance_info = os.environ.get("MAINTENANCE_INFO")
    out_of_sync = False
    remote_height = None
    if now - last_timestamp > 120:
        out_of_sync = True
        if rpc_root := os.environ.get("RPC_URL_ROOT"):
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{rpc_root}/testnet3/latest/height") as resp:
                    if resp.status == 200:
                        remote_height = await resp.text()
    return {
        "out_of_sync": out_of_sync,
        "maintenance_info": maintenance_info,
        "local_height": last_block.header.metadata.height,
        "remote_height": remote_height,
        "relative_time": get_relative_time(last_timestamp),
    }


async def function_signature(db: Database, program_id: str, function_name: str):
    data = await function_definition(db, program_id, function_name)
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

async def function_definition(db: Database, program_id: str, function_name: str):
    data = await db.get_function_definition(program_id, function_name)
    if data is None:
        return f"Unknown function {program_id}/{function_name}"
    return data
