import datetime
import os
from decimal import Decimal

from starlette.templating import Jinja2Templates

templates = Jinja2Templates(directory='webui/templates', trim_blocks=True, lstrip_blocks=True)

def get_env(name):
    return os.environ.get(name)

def format_time(epoch):
    time_str = datetime.datetime.fromtimestamp(epoch, tz=datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    return f"""<span class="time">{time_str}</span>"""

def format_number(number, decimal_places=2):
    if not isinstance(number, Decimal):
        number = Decimal(number)
    integer = str(int(number))
    integer_parts = []
    for i in range(len(integer) - 1, -1, -3):
        integer_parts.append(integer[max(i - 2, 0):i + 1])
    integer_parts.reverse()
    decimal = str(number % 1)[2:2 + decimal_places]
    decimal_parts = []
    for i in range(0, len(decimal), 3):
        decimal_parts.append(decimal[i:i + 3])
    if not decimal_parts:
        return '<span class="formatted-number">' + \
            "".join(map(lambda x: f'<span class="number-part">{x}</span>', integer_parts)) + \
            '</span>'

    return '<span class="formatted-number">' + \
        "".join(map(lambda x: f'<span class="number-part">{x}</span>', integer_parts)) + \
        '<span class="number-dot">.</span>' + \
        "".join(map(lambda x: f'<span class="number-part">{x}</span>', decimal_parts)) + \
        '</span>'

def format_aleo_credit(mc):
    if mc == "-":
        return "-"
    return format_number(Decimal(mc) / 1_000_000, 6)

templates.env.filters["get_env"] = get_env
templates.env.filters["format_time"] = format_time
templates.env.filters["format_aleo_credit"] = format_aleo_credit
templates.env.filters["format_number"] = format_number