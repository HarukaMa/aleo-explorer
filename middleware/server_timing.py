import functools
import time

from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Scope, Receive, Send


class RequestTiming:
    start_ns: int = 0
    end_ns: int = 0

class ServerTimingMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            return await self.app(scope, receive, send)

        timing = RequestTiming()
        receive = functools.partial(self.receive, timing=timing, receive=receive)
        send = functools.partial(self.send, timing=timing, send=send)
        await self.app(scope, receive, send)

    async def receive(self, timing: RequestTiming, receive: Receive) -> Message:
        message = await receive()
        print(message)
        if not message.get("more_body", False):
            timing.start_ns = time.perf_counter_ns()
        return message

    async def send(self, message: Message, timing: RequestTiming, send: Send) -> None:
        if message["type"] != "http.response.start":
            await send(message)
        else:
            timing.end_ns = time.perf_counter_ns()
            headers = MutableHeaders(scope=message)
            headers["Server-Timing"] = f"e;dur={(timing.end_ns - timing.start_ns) / 1e6}"
            await send(message)

