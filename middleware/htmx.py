from starlette.types import ASGIApp, Scope, Receive, Send


class HtmxMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] not in ["http", "websocket"]:
            return await self.app(scope, receive, send)
        scope["htmx"] = HtmxData(scope)
        await self.app(scope, receive, send)

class HtmxData:

    def __init__(self, scope: Scope):
        self.scope = scope

    def is_htmx(self):
        return b"hx-request" in dict(self.scope["headers"])