import asyncio
import functools
import json
import os
import time
from decimal import Decimal
# noinspection PyProtectedMember
from json.encoder import encode_basestring_ascii, encode_basestring, _make_iterencode, INFINITY  # type: ignore
from typing import Any, Callable, Coroutine

import aiohttp
from starlette.requests import Request
from starlette.responses import Response

from aleo_types import Int
from aleo_types.serialize import JSONSerialize
from db import Database
from webui.classes import UIAddress


class CustomEncoder(json.JSONEncoder):

    def encode(self, o: Any):
        if isinstance(o, Int):
            return str(o.json())
        return super().encode(o)

    def iterencode(self, o: Any, _one_shot: bool = False):
        """Encode the given object and yield each string
        representation as available.

        For example::

            for chunk in JSONEncoder().iterencode(bigobject):
                mysocket.write(chunk)

        """
        if self.check_circular:
            markers = {}
        else:
            markers = None
        if self.ensure_ascii:
            _encoder = encode_basestring_ascii
        else:
            _encoder = encode_basestring

        def floatstr(o, allow_nan=self.allow_nan,
                     _repr=float.__repr__, _inf=INFINITY, _neginf=-INFINITY):
            # Check for specials.  Note that this type of test is processor
            # and/or platform-specific, so do tests which don't depend on the
            # internals.

            if o != o:
                text = 'NaN'
            elif o == _inf:
                text = 'Infinity'
            elif o == _neginf:
                text = '-Infinity'
            else:
                return _repr(o)

            if not allow_nan:
                raise ValueError(
                    "Out of range float values are not JSON compliant: " +
                    repr(o))

            return text

        def _isinstance(o, c):
            if isinstance(o, Int):
                return False
            return isinstance(o, c)

        _iterencode = _make_iterencode(
            markers, self.default, _encoder, self.indent, floatstr,
            self.key_separator, self.item_separator, self.sort_keys,
            self.skipkeys, _one_shot, isinstance=_isinstance)
        return _iterencode(o, 0)

    def default(self, o: Any):
        if isinstance(o, Decimal):
            return str(o)
        elif isinstance(o, UIAddress):
            return {
                "address": o.address,
                "name": o.name,
                "tag": o.tag,
                "link": o.link,
                "logo": o.logo,
            }
        elif isinstance(o, JSONSerialize):
            return o.json()
        return super().default(o)

class CJSONResponse(Response):
    media_type = "application/json"

    def render(self, content: Any):
        return json.dumps(content, cls=CustomEncoder).encode("utf-8")

async def get_remote_height(session: aiohttp.ClientSession, rpc_root: str) -> str:
    try:
        async with session.get(f"{rpc_root}/{os.environ.get('NETWORK', 'unknown')}/latest/height") as resp:
            if resp.status == 200:
                remote_height = await resp.text()
            else:
                remote_height = "?"
    except:
        remote_height = "?"
    return remote_height


async def out_of_sync_check(session: aiohttp.ClientSession, db: Database):
    last_timestamp, last_height = await asyncio.gather(
        db.get_latest_block_timestamp(),
        db.get_latest_height()
    )
    now = int(time.time())
    maintenance_info = os.environ.get("MAINTENANCE_INFO")
    out_of_sync = now - last_timestamp > 300
    node_height = None
    reference_height = None
    if out_of_sync:
        if rpc_root := os.environ.get("RPC_URL_ROOT"):
            node_height = await get_remote_height(session, rpc_root)
        if ref_rpc_root := os.environ.get("REF_RPC_URL_ROOT"):
            reference_height = await get_remote_height(session, ref_rpc_root)

    return {
        "out_of_sync": out_of_sync,
        "maintenance_info": maintenance_info,
        "explorer_height": last_height,
        "node_height": node_height,
        "reference_height": reference_height,
        "last_timestamp": last_timestamp,
    }


async def function_signature(db: Database, program_id: str, function_name: str):
    data = await function_definition(db, program_id, function_name)
    if isinstance(data, str):
        return data
    inputs: list[str] = []
    for i in range(len(data["input"])):
        name = data["input"][i]
        mode = data["input_mode"][i]
        if mode == "private":
            inputs.append(name)
        else:
            inputs.append(f"{mode} {name}")
    outputs: list[str] = []
    for i in range(len(data["output"])):
        name = data["output"][i]
        mode = data["output_mode"][i]
        if mode == "private":
            outputs.append(name)
        else:
            outputs.append(f"{mode} {name}")
    finalizes = data["finalize"]
    result = f"{program_id}/{function_name}({', '.join(inputs)})"
    if len(outputs) == 1:
        result += f" -> {outputs[0]}"
    else:
        result += f" -> ({', '.join(outputs)})"
    if len(finalizes) != 0:
        result += f" finalize({', '.join(finalizes)})"
    return result

async def function_definition(db: Database, program_id: str, function_name: str):
    data = await db.get_function_definition(program_id, function_name)
    if data is None:
        return f"Unknown function {program_id}/{function_name}"
    return data

def public_cache_seconds(seconds: int):
    def decorator(func: Callable[[Request], Coroutine[Any, Any, Response]]):
        @functools.wraps(func)
        async def wrapper(request: Request):
            response = await func(request)
            response.headers["Cache-Control"] = f"public, max-age={seconds}"
            return response
        return wrapper
    return decorator