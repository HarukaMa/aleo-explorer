import datetime
import functools
import os
from decimal import Decimal
from typing import Callable, Coroutine, Any

from starlette.exceptions import HTTPException
from starlette.requests import Request
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
            "".join(map(lambda x: f'<span class="number-part">{x}</span>', integer_parts)) + \
            '</span>'

    return '<span class="formatted-number">' + \
        "".join(map(lambda x: f'<span class="number-part">{x}</span>', integer_parts)) + \
        '<span class="number-dot">.</span>' + \
        "".join(map(lambda x: f'<span class="number-part">{x}</span>', decimal_parts)) + \
        '</span>'

def format_aleo_credit(mc: int | Decimal):
    if mc == "-":
        return "-"
    return format_number(Decimal(mc) / 1_000_000, 6)

templates.env.filters["get_env"] = get_env # type: ignore
templates.env.filters["format_time"] = format_time # type: ignore
templates.env.filters["format_time_delta"] = format_time_delta # type: ignore
templates.env.filters["format_aleo_credit"] = format_aleo_credit # type: ignore
templates.env.filters["format_number"] = format_number # type: ignore

def htmx_template(template: str):
    def decorator(func: Callable[[Request], Coroutine[Any, Any, dict[str, Any]]]):
        @functools.wraps(func)
        async def wrapper(request: Request):
            is_htmx = request.scope["htmx"].is_htmx()
            if is_htmx:
                t = f"htmx/{template}"
            else:
                t = template
            try:
                result = await func(request)
            except Exception as e:
                tb = e.__traceback__
                while tb.tb_next:
                    tb = tb.tb_next
                frame = tb.tb_frame
                raise HTTPException(status_code=550, detail=f"error in {frame.f_code.co_filename.rsplit('/', 1)[-1]}:{frame.f_lineno}: {e.__class__.__name__}: {str(e)}") from e
            if isinstance(result, tuple):
                context, headers = result
            else:
                return result
            context["request"] = request
            return templates.TemplateResponse(t, context, headers=headers)
        return wrapper
    return decorator