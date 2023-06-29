import asyncio
import logging
import multiprocessing
import os
import time

import aleo
import uvicorn
from starlette.applications import Starlette
from starlette.exceptions import HTTPException
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from db import Database
from middleware.api_quota import APIQuotaMiddleware
from middleware.asgi_logger import AccessLoggerMiddleware
from middleware.server_timing import ServerTimingMiddleware
from node.types import Program, Identifier, PlaintextType, LiteralPlaintextType, StructPlaintextType, LiteralPlaintext, \
    Literal, Value


class UvicornServer(multiprocessing.Process):

    def __init__(self, config: uvicorn.Config):
        super().__init__()
        self.server = uvicorn.Server(config=config)
        self.config = config

    def stop(self):
        self.terminate()

    def run(self, *args, **kwargs):
        self.server.run()

async def commitment_route(request: Request):
    db = request.app.state.db
    if time.time() >= 1675209600:
        return JSONResponse(None)
    commitment = request.query_params.get("commitment")
    if not commitment:
        return HTTPException(400, "Missing commitment")
    return JSONResponse(await db.get_puzzle_commitment(commitment))

async def mapping_route(request: Request):
    db = request.app.state.db
    version = request.path_params["version"]
    program_id = request.path_params["program_id"]
    mapping = request.path_params["mapping"]
    key = request.path_params["key"]

    try:
        program = Program.load(bytearray(await db.get_program(program_id)))
    except:
        return JSONResponse({"error": "Program not found"}, status_code=404)
    mapping_name = Identifier.loads(mapping)
    if mapping_name not in program.mappings:
        return JSONResponse({"error": "Mapping not found"}, status_code=404)
    map_key_type = program.mappings[mapping_name].key.plaintext_type
    if map_key_type.type == PlaintextType.Type.Literal:
        map_key_type: LiteralPlaintextType
        primitive_type = map_key_type.literal_type.get_primitive_type()
        try:
            key = primitive_type.loads(key)
        except:
            return JSONResponse({"error": "Invalid key"}, status_code=400)
        key = LiteralPlaintext(literal=Literal(type_=Literal.reverse_primitive_type_map[primitive_type], primitive=key))
    elif map_key_type.type == PlaintextType.Type.Struct:
        map_key_type: StructPlaintextType
        return JSONResponse({"error": "Struct keys not supported yet"}, status_code=500)
    else:
        return JSONResponse({"error": "Unknown key type"}, status_code=500)
    mapping_id = aleo.get_mapping_id(program_id, mapping)
    key_id = aleo.get_key_id(mapping_id, key.dump())
    value = await db.get_mapping_value(program_id, mapping, key_id)
    if value is None:
        return JSONResponse({"value": None})
    return JSONResponse({"value": str(Value.load(bytearray(value)))})

routes = [
    Route("/commitment", commitment_route),
    Route("/v{version:int}/mapping/{program_id}/{mapping}/{key}", mapping_route),
]

async def startup():
    async def noop(_): pass

    # different thread so need to get a new database instance
    db = Database(server=os.environ["DB_HOST"], user=os.environ["DB_USER"], password=os.environ["DB_PASS"],
                  database=os.environ["DB_DATABASE"], schema=os.environ["DB_SCHEMA"],
                  message_callback=noop)
    await db.connect()
    app.state.db = db


log_format = '\033[92mAPI\033[0m: \033[94m%(client_addr)s\033[0m - - %(t)s \033[96m"%(request_line)s"\033[0m \033[93m%(s)s\033[0m %(B)s "%(f)s" "%(a)s" %(L)s'
# noinspection PyTypeChecker
app = Starlette(
    debug=True if os.environ.get("DEBUG") else False,
    routes=routes,
    on_startup=[startup],
    middleware=[
        Middleware(AccessLoggerMiddleware, format=log_format),
        Middleware(CORSMiddleware, allow_origins=['*']),
        Middleware(ServerTimingMiddleware),
        Middleware(APIQuotaMiddleware)
    ]
)


async def run():
    config = uvicorn.Config("api:app", reload=True, log_level="info", port=int(os.environ.get("API_PORT", 8001)))
    logging.getLogger("uvicorn.access").handlers = []
    server = UvicornServer(config=config)

    server.start()
    while True:
        await asyncio.sleep(3600)
