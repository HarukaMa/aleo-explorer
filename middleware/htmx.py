from typing import Any, MutableMapping

from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Scope, Receive, Send


class HtmxMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] not in ["http", "websocket"]:
            return await self.app(scope, receive, send)
        scope["htmx"] = HtmxData(scope)

        async def send_vary(message: MutableMapping[str, Any]) -> None:
            if message["type"] == "http.response.start":
                headers = MutableHeaders(scope=message)
                headers.append("Vary", "HX-Request")
            await send(message)

        await self.app(scope, receive, send_vary)

class HtmxData:

    def __init__(self, scope: Scope):
        self.scope = scope

    def is_htmx(self):
        return b"hx-request" in dict(self.scope["headers"])