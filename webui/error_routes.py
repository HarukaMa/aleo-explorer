from starlette.requests import Request

from .template import templates


async def bad_request(request: Request, exc: Exception):
    return templates.TemplateResponse('400.jinja2', {'request': request, "exc": exc}, status_code=400) # type: ignore


async def not_found(request: Request, exc: Exception):
    return templates.TemplateResponse('404.jinja2', {'request': request, "exc": exc}, status_code=404) # type: ignore


async def internal_error(request: Request, exc: Exception):
    return templates.TemplateResponse('500.jinja2', {'request': request, "exc": exc}, status_code=500) # type: ignore


async def cloudflare_error_page(request: Request):
    placeholder = request.query_params.get("placeholder")
    return templates.TemplateResponse('cf.jinja2', {'request': request, "placeholder": placeholder}) # type: ignore

