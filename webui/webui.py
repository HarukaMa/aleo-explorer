import asyncio
import logging
import multiprocessing
import os

import uvicorn
from asgi_logger import AccessLoggerMiddleware
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.responses import FileResponse
from starlette.routing import Route, Mount
from starlette.staticfiles import StaticFiles

# from node.light_node import LightNodeState
from .chain_routes import *
from .error_routes import *
from .program_routes import *
from .proving_routes import *
from .template import templates
from .utils import out_of_sync_check


class UvicornServer(multiprocessing.Process):

    def __init__(self, config: uvicorn.Config):
        super().__init__()
        self.server = uvicorn.Server(config=config)
        self.config = config

    def stop(self):
        self.terminate()

    def run(self, *args, **kwargs):
        self.server.run()

async def index_route(request: Request):
    db: Database = request.app.state.db
    recent_blocks = await db.get_recent_blocks_fast()
    network_speed = await db.get_network_speed()
    maintenance, info = await out_of_sync_check(db)
    ctx = {
        "latest_block": await db.get_latest_block(),
        "request": request,
        "recent_blocks": recent_blocks,
        "network_speed": network_speed,
        "maintenance": maintenance,
        "info": info,
    }
    return templates.TemplateResponse('index.jinja2', ctx, headers={'Cache-Control': 'public, max-age=10'})

async def tools_route(request: Request):
    ctx = {
        "request": request,
    }
    return templates.TemplateResponse('tools.jinja2', ctx, headers={'Cache-Control': 'public, max-age=3600'})

async def faq_route(request: Request):
    ctx = {
        "request": request,
    }
    return templates.TemplateResponse('faq.jinja2', ctx, headers={'Cache-Control': 'public, max-age=3600'})


async def privacy_route(request: Request):
    ctx = {
        "request": request,
    }
    return templates.TemplateResponse('privacy.jinja2', ctx, headers={'Cache-Control': 'public, max-age=3600'})

async def robots_route(_: Request):
    return FileResponse("webui/robots.txt", headers={'Cache-Control': 'public, max-age=3600'})


routes = [
    Route("/", index_route),
    # Blockchain
    Route("/block", block_route),
    Route("/transaction", transaction_route),
    Route("/transition", transition_route),
    Route("/search", search_route),
    Route("/blocks", blocks_route),
    # Programs
    Route("/programs", programs_route),
    Route("/program", program_route),
    Route("/similar_programs", similar_programs_route),
    Route("/upload_source", upload_source_route, methods=["GET", "POST"]),
    Route("/submit_source", submit_source_route, methods=["POST"]),
    # Proving
    Route("/calc", calc_route),
    Route("/leaderboard", leaderboard_route),
    Route("/address", address_route),
    Route("/address_solution", address_solution_route),
    # Other
    Route("/tools", tools_route),
    Route("/faq", faq_route),
    Route("/privacy", privacy_route),
    Route("/robots.txt", robots_route),
    Route("/cf", cloudflare_error_page),
    Mount("/static", StaticFiles(directory="webui/static"), name="static"),
]

exc_handlers = {
    400: bad_request,
    404: not_found,
    550: internal_error,
}

async def startup():
    async def noop(_): pass

    # different thread so need to get a new database instance
    db = Database(server=os.environ["DB_HOST"], user=os.environ["DB_USER"], password=os.environ["DB_PASS"],
                  database=os.environ["DB_DATABASE"], schema=os.environ["DB_SCHEMA"],
                  message_callback=noop)
    await db.connect()
    app.state.db = db


log_format = '\033[92mACCESS\033[0m: \033[94m%(client_addr)s\033[0m - - %(t)s \033[96m"%(request_line)s"\033[0m \033[93m%(s)s\033[0m %(B)s "%(f)s" "%(a)s" %(L)s'
# noinspection PyTypeChecker
app = Starlette(
    debug=True if os.environ.get("DEBUG") else False,
    routes=routes,
    on_startup=[startup],
    exception_handlers=exc_handlers,
    middleware=[Middleware(AccessLoggerMiddleware, format=log_format)]
)


async def run(_):
    config = uvicorn.Config("webui:app", reload=True, log_level="info", port=int(os.environ.get("PORT", 8000)))
    logging.getLogger("uvicorn.access").handlers = []
    server = UvicornServer(config=config)

    server.start()
    while True:
        await asyncio.sleep(3600)
