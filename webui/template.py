import datetime
import functools
import os
import traceback
from decimal import Decimal
from typing import Callable, Coroutine, Any

from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import Response
from starlette.templating import Jinja2Templates

templates = Jinja2Templates(directory='webui/templates', trim_blocks=True, lstrip_blocks=True)

def get_env(name: str):
    return os.environ.get(name)

def format_time(epoch: int):
    time_str = datetime.datetime.fromtimestamp(epoch, tz=datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    return f"""<span class="time">{time_str}</span>"""

def format_time_delta(delta: int):
    minutes, seconds = divmod(delta, 60)
    hours, minutes = divmod(minutes, 60)
    days, hours = divmod(hours, 24)
    if days > 0:
        return f"{days} days, {hours} hours, {minutes} minutes, {seconds} seconds"
    if hours > 0:
        return f"{hours} hours, {minutes} minutes, {seconds} seconds"
    if minutes > 0:
        return f"{minutes} minutes, {seconds} seconds"
    return f"{seconds} seconds"

def format_number(number: int | Decimal, decimal_places: int = 2):
    if not isinstance(number, Decimal):
        number = Decimal(number)
    integer = str(int(number))
    integer_parts: list[str] = []
    for i in range(len(integer) - 1, -1, -3):
        integer_parts.append(integer[max(i - 2, 0):i + 1])
    integer_parts.reverse()
    decimal = str(number % 1)
    if decimal_places > 3 and decimal_places % 3 != 0:
        decimal_places += 3 - (decimal_places % 3)
    if decimal != "0" and len(decimal) - 2 < decimal_places:
        decimal += "0" * (decimal_places - len(decimal) + 2)
    decimal = decimal[2:2 + decimal_places]
    decimal_parts: list[str] = []
    for i in range(0, len(decimal), 3):
        decimal_parts.append(decimal[i:i + 3])
    while decimal_parts and decimal_parts[-1] == "000":
        decimal_parts.pop()
    if not decimal_parts:
        return '<span class="formatted-number">' + \
            '<span class="number-separator">,</span>'.join(map(lambda x: f'<span class="number-part">{x}</span>', integer_parts)) + \
            '</span>'

    return '<span class="formatted-number">' + \
        '<span class="number-separator">,</span>'.join(map(lambda x: f'<span class="number-part">{x}</span>', integer_parts)) + \
        '<span class="number-dot">.</span>' + \
        "".join(map(lambda x: f'<span class="number-part">{x}</span>', decimal_parts)) + \
        '</span>'

def format_aleo_credit(mc: int | Decimal):
    if mc == "-":
        return "-"
    return format_number(Decimal(mc) / 1_000_000, 6)

def network_tag(_: str):
    names = {
        "testnet": "Testnet Beta",
        "canary": "Canary",
    }
    network = os.environ.get("NETWORK", "unknown")
    if network not in names:
        return network
    return names[network]

templates.env.filters["get_env"] = get_env # type: ignore
templates.env.filters["format_time"] = format_time # type: ignore
templates.env.filters["format_time_delta"] = format_time_delta # type: ignore
templates.env.filters["format_aleo_credit"] = format_aleo_credit # type: ignore
templates.env.filters["format_number"] = format_number # type: ignore
templates.env.filters["network_tag"] = network_tag # type: ignore

def htmx_template(template: str):
    def decorator(func: Callable[[Request], Coroutine[Any, Any, tuple[dict[str, Any], dict[str, str]] | dict[str, str] | Response]]):
        @functools.wraps(func)
        async def wrapper(request: Request):
            is_htmx = request.scope["htmx"].is_htmx()
            if is_htmx:
                t = f"htmx/{template}"
            else:
                t = template
            try:
                result = await func(request)
            except HTTPException:
                raise
            except Exception as e:
                tb = e.__traceback__
                if tb is None:
                    msg = f"uncaught exception: {e.__class__.__name__}: {str(e)}"
                else:
                    while tb.tb_next:
                        tb = tb.tb_next
                    frame = tb.tb_frame
                    print(frame.f_locals)
                    msg = f"uncaught exception at {frame.f_code.co_filename.rsplit('/', 1)[-1]}:{frame.f_lineno}: {e.__class__.__name__}: {str(e)}\n"
                    for k, v in frame.f_locals.items():
                        msg += f"  {k} = {str(v)[:50]}{'...' if len(str(v)) > 50 else ''}\n"
                print(msg)
                traceback.print_exc()
                raise HTTPException(status_code=550, detail=msg) from e
            if isinstance(result, tuple):
                context, headers = result
            else:
                return result
            context["request"] = request
            try:
                return templates.TemplateResponse(t, context, headers=headers)
            except Exception as e:
                tb = e.__traceback__
                if tb is None:
                    raise HTTPException(status_code=550, detail=f"template error: {e.__class__.__name__}: {str(e)}") from e
                frame = tb.tb_frame
                while tb.tb_next:
                    tb = tb.tb_next
                    if tb.tb_frame.f_code.co_filename.endswith(".jinja2"):
                        frame = tb.tb_frame
                raise HTTPException(status_code=550, detail=f"template error at {frame.f_code.co_filename.rsplit('/', 1)[-1]}:{frame.f_lineno}: {e.__class__.__name__}: {str(e)}") from e
        return wrapper
    return decorator