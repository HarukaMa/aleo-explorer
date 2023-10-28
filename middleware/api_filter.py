from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Scope, Receive, Send


class APIFilterMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] not in ["http", "websocket"]:
            return await self.app(scope, receive, send)
        request = Request(scope)
        if request.headers.get("user-agent", "") == "":
            response = JSONResponse({"error": "You must provide a sensible user agent to use this API."}, status_code=403)
            return await response(scope, receive, send)
        await self.app(scope, receive, send)