import asyncio
import logging
import multiprocessing
import os

import aiohttp
import uvicorn
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.responses import FileResponse
from starlette.routing import Route

from middleware.asgi_logger import AccessLoggerMiddleware
from middleware.htmx import HtmxMiddleware
from middleware.minify import MinifyMiddleware
from middleware.server_timing import ServerTimingMiddleware
# from node.light_node import LightNodeState
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

    def run(self, *args: Any, **kwargs: Any):
        self.server.run()

async def index_route(request: Request):
    db: Database = request.app.state.db
    is_htmx = request.scope["htmx"].is_htmx()
    if is_htmx:
        template = "htmx/index.jinja2"
    else:
        template = "index.jinja2"
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
    return templates.TemplateResponse(template, ctx, headers={'Cache-Control': 'public, max-age=10'}) # type: ignore

async def tools_route(request: Request):
    ctx = {
        "request": request,
    }
    return templates.TemplateResponse('tools.jinja2', ctx, headers={'Cache-Control': 'public, max-age=3600'}) # type: ignore

async def faq_route(request: Request):
    is_htmx = request.scope["htmx"].is_htmx()
    if is_htmx:
        template = "htmx/faq.jinja2"
    else:
        template = "faq.jinja2"
    ctx = {
        "request": request,
    }
    return templates.TemplateResponse(template, ctx, headers={'Cache-Control': 'public, max-age=3600'}) # type: ignore


async def feedback_route(request: Request):
    is_htmx = request.scope["htmx"].is_htmx()
    if is_htmx:
        template = "htmx/feedback.jinja2"
    else:
        template = "feedback.jinja2"
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
    return templates.TemplateResponse(template, ctx, headers={'Cache-Control': 'public, max-age=3600'}) # type: ignore

async def submit_feedback_route(request: Request):
    db: Database = request.app.state.db
    form = await request.form()
    contact = form.get("contact")
    if isinstance(contact, UploadFile):
        return RedirectResponse(url="/feedback?message=Invalid contact")
    if contact == "" or contact is None:
        contact = "Anonymous"
    content = form.get("content")
    if not content or isinstance(content, UploadFile):
        return RedirectResponse(url="/feedback?message=Invalid content")
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
    is_htmx = request.scope["htmx"].is_htmx()
    if is_htmx:
        template = "htmx/privacy.jinja2"
    else:
        template = "privacy.jinja2"
    ctx = {
        "request": request,
    }
    return templates.TemplateResponse(template, ctx, headers={'Cache-Control': 'public, max-age=3600'}) # type: ignore

async def robots_route(_: Request):
    return FileResponse("webui/robots.txt", headers={'Cache-Control': 'public, max-age=3600'})

async def down_route(request: Request):
    return templates.TemplateResponse("down.jinja2", ctx={"request": request}, headers={'Cache-Control': 'no-cache'})

routes = [
    Route("/{path:path}", down_route),
    # Route("/", index_route),
    # Blockchain
    # Route("/block", block_route),
    # Route("/transaction", transaction_route),
    # Route("/transition", transition_route),
    # Route("/search", search_route),
    # Route("/blocks", blocks_route),
    # # Programs
    # Route("/programs", programs_route),
    # Route("/program", program_route),
    # Route("/similar_programs", similar_programs_route),
    # Route("/upload_source", upload_source_route, methods=["GET", "POST"]),
    # Route("/submit_source", submit_source_route, methods=["POST"]),
    # # Proving
    # Route("/calc", calc_route),
    # Route("/leaderboard", leaderboard_route),
    # Route("/address", address_route),
    # Route("/address_solution", address_solution_route),
    # # Other
    # Route("/tools", tools_route),
    # Route("/faq", faq_route),
    # Route("/feedback", feedback_route, methods=["GET", "POST"]),
    # Route("/submit_feedback", submit_feedback_route, methods=["POST"]),
    # Route("/privacy", privacy_route),
    # Route("/robots.txt", robots_route),
    # Route("/cf", cloudflare_error_page),
    # Mount("/static", StaticFiles(directory="webui/static"), name="static"),
]

exc_handlers = {
    400: bad_request,
    404: not_found,
    550: internal_error,
}

async def startup():
    async def noop(_: Any): pass

    # different thread so need to get a new database instance
    db = Database(server=os.environ["DB_HOST"], user=os.environ["DB_USER"], password=os.environ["DB_PASS"],
                  database=os.environ["DB_DATABASE"], schema=os.environ["DB_SCHEMA"],
                  message_callback=noop)
    await db.connect()
    # noinspection PyUnresolvedReferences
    app.state.db = db


log_format = '\033[92mACCESS\033[0m: \033[94m%(client_addr)s\033[0m - - %(t)s \033[96m"%(request_line)s"\033[0m \033[93m%(s)s\033[0m %(B)s "%(f)s" "%(a)s" %(L)s \033[95m%(htmx)s\033[0m'
# noinspection PyTypeChecker
app = Starlette(
    debug=True if os.environ.get("DEBUG") else False,
    routes=routes,
    on_startup=[startup],
    exception_handlers=exc_handlers,
    middleware=[
        Middleware(AccessLoggerMiddleware, format=log_format),
        Middleware(HtmxMiddleware),
        Middleware(MinifyMiddleware),
        Middleware(ServerTimingMiddleware),
    ]
)


async def run():
    host = os.environ.get("HOST", "127.0.0.1")
    port = int(os.environ.get("PORT", 8000))
    config = uvicorn.Config("webui:app", reload=True, log_level="info", host=host, port=port)
    logging.getLogger("uvicorn.access").handlers = []
    server = UvicornServer(config=config)

    server.start()
    while True:
        await asyncio.sleep(3600)
