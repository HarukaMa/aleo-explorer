import asyncio
import functools
import time
from collections import defaultdict

from starlette.datastructures import MutableHeaders
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Message, Scope, Receive, Send


class RequestTiming:
    start_ns: int = 0
    end_ns: int = 0


class APIQuotaMiddleware:
    def __init__(self, app: ASGIApp, *, max_call_time = 10.0, recover_rate = 0.1) -> None:
        self.app = app
        self.max_call_time = max_call_time
        self.recover_rate = recover_rate
        self.ip_remaining_time = defaultdict(lambda: (max_call_time, -1.0, 0))

    def start_call(self, ip):
        remaining, last_call, outstanding_call = self.ip_remaining_time[ip]
        print(f"ip {ip} has quota {remaining}s, last call {last_call}, outstanding call {outstanding_call}")
        if outstanding_call == 0 and last_call != -1:
            quota = remaining + (time.monotonic() - last_call) * self.recover_rate
            if quota > self.max_call_time:
                quota = self.max_call_time
            print(f"ip {ip} quota recovered to {quota}")
        else:
            quota = remaining
        # save current quota subtract 1 second right now to avoid flood attack
        self.ip_remaining_time[ip] = (quota - 1, last_call, outstanding_call + 1)
        return quota

    def get_quota_for_header(self, ip, cost):
        remaining, _, outstanding_call = self.ip_remaining_time[ip]
        return remaining - cost + outstanding_call

    def end_call(self, ip, cost):
        remaining, _, outstanding_call = self.ip_remaining_time[ip]
        if cost == -1:
            # timeout
            self.ip_remaining_time[ip] = (0, time.monotonic(), outstanding_call - 1)
        elif cost == -2:
            # honestly, unknown exception, refunding time used
            self.ip_remaining_time[ip] = (remaining + 1, time.monotonic(), outstanding_call - 1)
        else:
            self.ip_remaining_time[ip] = (remaining - cost + 1, time.monotonic(), outstanding_call - 1)
        remaining, last_call, outstanding_call = self.ip_remaining_time[ip]
        print(f"ip {ip} used {cost}s, remaining {remaining}s, last call {last_call}, outstanding call {outstanding_call}")


    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            return await self.app(scope, receive, send)

        ip = scope["client"][0]
        remaining = self.start_call(ip)
        timing = RequestTiming()
        cost = -2 # final catch if try except borked
        try:
            receive = functools.partial(self.timing_receive, timing=timing, receive=receive)
            send = functools.partial(self.timing_send, scope=scope, timing=timing, send=send)
            timing.start_ns = time.perf_counter_ns()
            await asyncio.wait_for(self.app(scope, receive, send), timeout=remaining)
            cost = (timing.end_ns - timing.start_ns) / 1e9
        except TimeoutError:
            headers = {
                "Retry-After": str(int(1 / self.recover_rate)),
            }
            response = JSONResponse({"error": "Request timed out - API quota exceeded"}, status_code=429, headers=headers)
            await response(scope, receive, send)
            cost = -1
        except:
            if timing.end_ns == 0:
                timing.end_ns = time.perf_counter_ns()
            cost = (timing.end_ns - timing.start_ns) / 1e9
        finally:
            self.end_call(ip, cost)

    # noinspection PyMethodMayBeStatic
    async def timing_receive(self, timing: RequestTiming, receive: Receive) -> Message:
        message = await receive()
        if not message.get("more_body", False):
            timing.start_ns = time.perf_counter_ns()
        return message


    async def timing_send(self, message: Message, scope: Scope, timing: RequestTiming, send: Send) -> None:
        timing.end_ns = time.perf_counter_ns()
        if message["type"] != "http.response.start":
            await send(message)
        else:
            cost = (timing.end_ns - timing.start_ns) / 1e9
            remaining = self.get_quota_for_header(scope["client"][0], cost)
            headers = MutableHeaders(scope=message)
            headers["Quota-Used"] = str(cost)
            headers["Quota-Remaining"] = str(remaining)
            headers["Quota-Max"] = str(self.max_call_time)
            headers["Quota-Recover-Rate"] = str(self.recover_rate)
            await send(message)