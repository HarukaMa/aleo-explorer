from starlette.responses import Response
from starlette.types import ASGIApp, Scope, Receive, Send


class AuthMiddleware:
    def __init__(self, app: ASGIApp, token: str) -> None:
        self.app = app
        self.token = token

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] not in ["http", "websocket"]:
            return await self.app(scope, receive, send)
        if dict(scope["headers"]).get(b"authorization", b"").decode() != f"Token {self.token}":
            return await Response(status_code=401)(scope, receive, send)
        await self.app(scope, receive, send)
