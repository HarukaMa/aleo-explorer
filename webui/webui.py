import asyncio
import logging
import multiprocessing
import os

import aiohttp
import uvicorn
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.responses import FileResponse
from starlette.routing import Route, Mount
from starlette.staticfiles import StaticFiles

from middleware.asgi_logger import AccessLoggerMiddleware
from middleware.minify import MinifyMiddleware
from middleware.server_timing import ServerTimingMiddleware
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
    sync_info = await out_of_sync_check(db)
    ctx = {
        "latest_block": await db.get_latest_block(),
        "request": request,
        "recent_blocks": recent_blocks,
        "network_speed": network_speed,
        "sync_info": sync_info,
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


async def feedback_route(request: Request):
    if request.method == "POST":
        form = await request.form()
        contact = form.get("contact")
        content = form.get("content")
    else:
        contact = ""
        content = ""
    success = request.query_params.get("success")
    message = request.query_params.get("message")
    ctx = {
        "request": request,
        "success": success,
        "message": message,
        "contact": contact,
        "content": content,
    }
    return templates.TemplateResponse('feedback.jinja2', ctx, headers={'Cache-Control': 'public, max-age=3600'})

async def submit_feedback_route(request: Request):
    db: Database = request.app.state.db
    form = await request.form()
    contact = form.get("contact")
    content = form.get("content")
    turnstile_response = form.get("cf-turnstile-response")
    async with aiohttp.ClientSession() as session:
        data = {
            "secret": os.environ.get("TURNSTILE_SECRET_KEY"),
            "response": turnstile_response,
        }
        async with session.post("https://challenges.cloudflare.com/turnstile/v0/siteverify", data=data) as resp:
            if not resp.ok:
                return RedirectResponse(url="/feedback?message=Failed to verify captcha")
            json = await resp.json()
            if not json["success"]:
                return RedirectResponse(url=f"/feedback?message=Failed to verify captcha: {json['error-codes']}")
    await db.save_feedback(contact, content)
    return RedirectResponse(url="/feedback?success=1", status_code=303)

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
    Route("/feedback", feedback_route, methods=["GET", "POST"]),
    Route("/submit_feedback", submit_feedback_route, methods=["POST"]),
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
    # noinspection PyUnresolvedReferences
    app.state.db = db


log_format = '\033[92mACCESS\033[0m: \033[94m%(client_addr)s\033[0m - - %(t)s \033[96m"%(request_line)s"\033[0m \033[93m%(s)s\033[0m %(B)s "%(f)s" "%(a)s" %(L)s'
# noinspection PyTypeChecker
app = Starlette(
    debug=True if os.environ.get("DEBUG") else False,
    routes=routes,
    on_startup=[startup],
    exception_handlers=exc_handlers,
    middleware=[
        Middleware(AccessLoggerMiddleware, format=log_format),
        Middleware(MinifyMiddleware),
        Middleware(ServerTimingMiddleware),
    ]
)


async def run():
    config = uvicorn.Config("webui:app", reload=True, log_level="info", port=int(os.environ.get("PORT", 8000)))
    logging.getLogger("uvicorn.access").handlers = []
    server = UvicornServer(config=config)

    server.start()
    while True:
        await asyncio.sleep(3600)
