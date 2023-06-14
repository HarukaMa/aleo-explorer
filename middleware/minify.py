import minify_html
from starlette.datastructures import MutableHeaders

from starlette.types import ASGIApp, Message, Scope, Receive, Send

class MinifyMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        await MinifyWrapper(self.app)(scope, receive, send)

class MinifyWrapper:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            return await self.app(scope, receive, send)

        async def minify_send(message: Message) -> None:
            if message["type"] == "http.response.start":
                self.start_message = message
                self.has_trailers = message.get("trailers", False)
                self.body = b""
                self.non_html = False
                headers = MutableHeaders(scope=message)
                if not headers.get("content-type", "").startswith("text/html"):
                    self.non_html = True
                    await send(message)
                    return
                if self.has_trailers:
                    raise NotImplementedError("MinifyMiddleware does not support trailers yet")

            elif message["type"] == "http.response.body":
                if self.non_html:
                    await send(message)
                    return
                body = self.body + message.get("body", b"")
                if not message.get("more_body", False):
                    body = minify_html.minify(
                        body.decode("utf-8"),
                        do_not_minify_doctype=True,
                        ensure_spec_compliant_unquoted_attribute_values=True,
                        keep_closing_tags=True,
                        keep_html_and_head_opening_tags=True,
                        minify_css=True,
                        minify_js=True,
                    ).encode("utf-8")
                    message["body"] = body
                    headers = MutableHeaders(scope=self.start_message)
                    headers["content-length"] = str(len(body))
                    await send(self.start_message)
                    await send(message)
                else:
                    self.body = body

        await self.app(scope, receive, minify_send)