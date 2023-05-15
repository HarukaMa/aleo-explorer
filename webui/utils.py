
import os
import time

from db import Database

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
    if now - last_timestamp > 120:
        if maintenance_info:
            return True, maintenance_info
        return False, get_relative_time(last_timestamp)
    return None, None


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
    if program_id == "credits.aleo":
        if function_name not in credits_functions:
            return f"Unknown program {program_id}"
        return credits_functions[function_name]
    else:
        data = await db.get_function_definition(program_id, function_name)
        if data is None:
            return f"Unknown function {program_id}/{function_name}"
        return data
