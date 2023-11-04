from starlette.requests import Request

from .template import templates
from .utils import out_of_sync_check


async def bad_request(request: Request, exc: Exception):
    db = request.app.state.db
    is_htmx = request.scope["htmx"].is_htmx()
    if is_htmx:
        template = "htmx/400.jinja2"
    else:
        template = "400.jinja2"
    sync_info = await out_of_sync_check(db)
    return templates.TemplateResponse(template, {'request': request, "exc": exc, "sync_info": sync_info}, status_code=400) # type: ignore


async def not_found(request: Request, exc: Exception):
    db = request.app.state.db
    is_htmx = request.scope["htmx"].is_htmx()
    if is_htmx:
        template = "htmx/404.jinja2"
    else:
        template = "404.jinja2"
    sync_info = await out_of_sync_check(db)
    return templates.TemplateResponse(template, {'request': request, "exc": exc, "sync_info": sync_info}, status_code=404) # type: ignore


async def internal_error(request: Request, exc: Exception):
    db = request.app.state.db
    is_htmx = request.scope["htmx"].is_htmx()
    if is_htmx:
        template = "htmx/500.jinja2"
    else:
        template = "500.jinja2"
    sync_info = await out_of_sync_check(db)
    return templates.TemplateResponse(template, {'request': request, "exc": exc, "sync_info": sync_info}, status_code=500) # type: ignore


async def cloudflare_error_page(request: Request):
    placeholder = request.query_params.get("placeholder")
    return templates.TemplateResponse('cf.jinja2', {'request': request, "placeholder": placeholder}) # type: ignore

