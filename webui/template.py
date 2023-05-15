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

def format_aleo_credit(mc):
    if mc == "-":
        return "-"
    return "{:,}".format(Decimal(mc) / 1_000_000)

def format_number(number):
    if isinstance(number, Decimal):
        return f"{number:,.2f}"
    return "{:,}".format(number)

templates.env.filters["get_env"] = get_env
templates.env.filters["format_time"] = format_time
templates.env.filters["format_aleo_credit"] = format_aleo_credit
templates.env.filters["format_number"] = format_number