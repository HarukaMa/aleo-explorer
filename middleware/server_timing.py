import functools
import os
import time
from collections import defaultdict
from types import TracebackType
from typing import Optional

from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Scope, Receive, Send


class RequestTiming:
    start_ns: int = 0
    end_ns: int = 0


async def timing_receive(timing: RequestTiming, receive: Receive) -> Message:
    message = await receive()
    if not message.get("more_body", False):
        timing.start_ns = time.perf_counter_ns()
    return message


async def timing_send(message: Message, scope: Scope, timing: RequestTiming, send: Send) -> None:
    timing.end_ns = time.perf_counter_ns()
    if message["type"] != "http.response.start":
        await send(message)
    else:
        headers = MutableHeaders(scope=message)
        res: list[str] = []
        for name, duration in scope["timings"].items():
            res.append(f"{name};dur={duration:.3f}")
        res.append(f"t;dur={((timing.end_ns - timing.start_ns) / 1e6):.3f}")
        headers["Server-Timing"] = ", ".join(res)
        await send(message)


class ServerTimingMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            return await self.app(scope, receive, send)

        if not(os.environ.get("DEBUG", False) or os.environ.get("BETA", False) or os.environ.get("TIMING")):
            return await self.app(scope, receive, send)

        scope["timings"] = defaultdict(float)
        timing = RequestTiming()
        receive = functools.partial(timing_receive, timing=timing, receive=receive)
        send = functools.partial(timing_send, scope=scope, timing=timing, send=send)
        timing.start_ns = time.perf_counter_ns()
        await self.app(scope, receive, send)

class TimingContext:
    def __init__(self, name: str, scope: Scope) -> None:
        self.name = name
        self.scope = scope
        if os.environ.get("DEBUG", False) or os.environ.get("BETA", False) or os.environ.get("TIMING"):
            if "timings" not in self.scope:
                raise Exception("TimingContext used without ServerTimingMiddleware")
            self.timing = True
        else:
            self.timing = False

    def __enter__(self) -> None:
        self.start_ns = time.perf_counter_ns()

    def __exit__(self, exc_type: Optional[type[BaseException]], exc_val: Optional[BaseException], exc_tb: Optional[TracebackType]) -> None:
        self.end_ns = time.perf_counter_ns()
        if self.timing:
            self.scope["timings"][self.name] += (self.end_ns - self.start_ns) / 1e6