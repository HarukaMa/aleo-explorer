from io import BytesIO
from typing import cast

from starlette.requests import Request
from starlette.responses import JSONResponse

from aleo_types import Program, Value, LiteralPlaintextType, LiteralPlaintext, \
    Literal, StructPlaintextType, StructPlaintext, Block
from aleo_types.cached import cached_get_key_id
from api.utils import async_check_sync, use_program_cache, parse_history_params
from db import Database


@async_check_sync
@use_program_cache
async def mapping_route(request: Request, program_cache: dict[str, Program]):
    db: Database = request.app.state.db
    version = request.path_params["version"]
    program_id = request.path_params["program_id"]
    mapping = request.path_params["mapping"]
    key = request.path_params["key"]
    height = request.query_params.get("height")
    time_str = request.query_params.get("time")

    if (height or time_str) and version < 3:
        return JSONResponse({"error": "This endpoint does not support height or time parameter in this version"}, status_code=400)

    try:
        program = program_cache[program_id]
    except KeyError:
        program = await db.get_program(program_id)
        if not program:
            return JSONResponse({"error": "Program not found"}, status_code=404)
        program = Program.load(BytesIO(program))
        program_cache[program_id] = program
    if mapping not in program.mappings:
        return JSONResponse({"error": "Mapping not found"}, status_code=404)
    map_key_type = program.mappings[mapping].key.plaintext_type
    if isinstance(map_key_type, LiteralPlaintextType):
        primitive_type = map_key_type.literal_type.primitive_type
        try:
            key = primitive_type.loads(key)
        except:
            return JSONResponse({"error": "Invalid key"}, status_code=400)
        key = LiteralPlaintext(literal=Literal(type_=Literal.reverse_primitive_type_map[primitive_type], primitive=key))
    elif isinstance(map_key_type, StructPlaintextType):
        structs = program.structs
        struct_type = structs[map_key_type.struct]
        try:
            value = StructPlaintext.loads(key, struct_type, structs)
        except Exception as e:
            return JSONResponse({"error": f"Invalid struct key: {e} (experimental feature, if you believe this is an error please submit a feedback)"}, status_code=400)
        key = value
    else:
        return JSONResponse({"error": "Unknown key type"}, status_code=500)
    key_id = cached_get_key_id(program_id, mapping, key.dump())


    if height or time_str:
        parse_result = await parse_history_params(db, height, time_str)
        if isinstance(parse_result, JSONResponse):
            return parse_result
        height, _, block_timestamp = parse_result
        value_bytes, data_height = await db.get_mapping_value_at_height(program_id, mapping, key_id, height)

    else:
        value_bytes = await db.get_mapping_value(program_id, mapping, key_id)
        height = await db.get_latest_height()
        block_timestamp = await db.get_latest_block_timestamp()
        data_height = height

    if value_bytes is None:
        value = None
    else:
        value = Value.load(BytesIO(value_bytes))

    if version < 3:
        return JSONResponse(str(value))

    if data_height is not None and data_height != height:
        block_timestamp = cast(Block, (await db.get_block_by_height(data_height))).header.metadata.timestamp
        height = data_height
    return JSONResponse({
        "height": height,
        "timestamp": block_timestamp,
        "value": None if value is None else str(value),
    })

@async_check_sync
@use_program_cache
async def mapping_list_route(request: Request, program_cache: dict[str, Program]):
    db: Database = request.app.state.db
    _ = request.path_params["version"]
    program_id = request.path_params["program_id"]
    try:
        program = program_cache[program_id]
    except KeyError:
        program = await db.get_program(program_id)
        if not program:
            return JSONResponse({"error": "Program not found"}, status_code=404)
        program = Program.load(BytesIO(program))
        program_cache[program_id] = program
    mappings = program.mappings
    return JSONResponse(list(map(str, mappings.keys())))

@async_check_sync
@use_program_cache
async def mapping_value_list_route(request: Request, program_cache: dict[str, Program]):
    db: Database = request.app.state.db
    version = request.path_params["version"]
    program_id = request.path_params["program_id"]
    mapping = request.path_params["mapping"]
    try:
        program = program_cache[program_id]
    except KeyError:
        program = await db.get_program(program_id)
        if not program:
            return JSONResponse({"error": "Program not found"}, status_code=404)
        program = Program.load(BytesIO(program))
        program_cache[program_id] = program
    mappings = program.mappings
    if mapping not in mappings:
        return JSONResponse({"error": "Mapping not found"}, status_code=404)

    if version <= 1:
        mapping_cache = await db.get_mapping_cache(program_id, mapping)
        res: dict[str, dict[str, str]] = {}
        for key_id, item in mapping_cache.items():
            res[str(key_id)] = {
                "key": str(item["key"]),
                "value": str(item["value"]),
            }
        return JSONResponse(res)

    else:
        count = int(request.query_params.get("count", 50))
        if count > 100:
            count = 100
        cursor = int(request.query_params.get("cursor", 0))
        mapping_data = await db.get_mapping_key_value(program_id, mapping, count, cursor)
        res: list[dict[str, str]] = []
        for key_id, item in mapping_data[0].items():
            res.append({
                "key": str(item["key"]),
                "value": str(item["value"]),
            })

        return JSONResponse({"result": res, "cursor": mapping_data[1]})

@async_check_sync
@use_program_cache
async def mapping_key_count_route(request: Request, program_cache: dict[str, Program]):
    db: Database = request.app.state.db
    version = request.path_params["version"]
    if version <= 1:
        return JSONResponse({"error": "This endpoint is not supported in this version"}, status_code=400)
    program_id = request.path_params["program_id"]
    mapping = request.path_params["mapping"]
    try:
        program = program_cache[program_id]
    except KeyError:
        program = await db.get_program(program_id)
        if not program:
            return JSONResponse({"error": "Program not found"}, status_code=404)
        program = Program.load(BytesIO(program))
        program_cache[program_id] = program
    mappings = program.mappings
    if mapping not in mappings:
        return JSONResponse({"error": "Mapping not found"}, status_code=404)
    return JSONResponse(await db.get_mapping_key_count(program_id, mapping))