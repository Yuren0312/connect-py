from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from pyqwest import Client
from pyqwest.testing import ASGITransport

from connectrpc.client import ConnectClient, ConnectClientSync
from connectrpc.method import IdempotencyLevel, MethodInfo, StreamType
from connectrpc.server import Endpoint, EndpointSync

from ._util import call
from .connectrpc.example.haberdasher_connect import (
    Haberdasher,
    HaberdasherASGIApplication,
    HaberdasherClient,
)
from .connectrpc.example.haberdasher_pb import Hat, Size

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

    from connectrpc.request import RequestContext


def _method(stream_type: StreamType | None = None) -> MethodInfo[Size, Hat]:
    return MethodInfo(
        name="MakeHat",
        service_name="connectrpc.example.Haberdasher",
        input=Size,
        output=Hat,
        idempotency_level=IdempotencyLevel.NO_SIDE_EFFECTS,
        stream_type=stream_type,
    )


async def _async_unary(_request: Size, _ctx: RequestContext[Size, Hat]) -> Hat:
    return Hat()


async def _async_client_stream(
    _request: AsyncIterator[Size], _ctx: RequestContext[Size, Hat]
) -> Hat:
    return Hat()


async def _async_server_stream(
    _request: Size, _ctx: RequestContext[Size, Hat]
) -> AsyncIterator[Hat]:
    yield Hat()


async def _async_bidi_stream(
    _request: AsyncIterator[Size], _ctx: RequestContext[Size, Hat]
) -> AsyncIterator[Hat]:
    yield Hat()


def _sync_unary(_request: Size, _ctx: RequestContext[Size, Hat]) -> Hat:
    return Hat()


def _sync_client_stream(
    _request: Iterator[Size], _ctx: RequestContext[Size, Hat]
) -> Hat:
    return Hat()


def _sync_server_stream(
    _request: Size, _ctx: RequestContext[Size, Hat]
) -> Iterator[Hat]:
    yield Hat()


def _sync_bidi_stream(
    _request: Iterator[Size], _ctx: RequestContext[Size, Hat]
) -> Iterator[Hat]:
    yield Hat()


async def _one_hat() -> AsyncIterator[Hat]:
    yield Hat()


async def _one_size() -> AsyncIterator[Size]:
    yield Size(inches=1)


class HaberdasherService(Haberdasher):
    """A Haberdasher implementation covering every method kind."""

    async def make_hat(self, request, _ctx):
        return Hat(size=request.inches, color="green")

    async def make_flexible_hat(self, request, _ctx):
        size = 0
        async for s in request:
            size += s.inches
        return Hat(size=size, color="red")

    async def make_similar_hats(self, request, _ctx):
        yield Hat(size=request.inches, color="orange")
        yield Hat(size=request.inches, color="blue")

    async def make_various_hats(self, request, _ctx):
        async for s in request:
            yield Hat(size=s.inches, color="black")


class CapturingInterceptor:
    """An async interceptor recording method info and short-circuiting the RPC."""

    def __init__(self) -> None:
        self.methods: list[MethodInfo[Size, Hat]] = []

    async def intercept_unary(self, _call_next, _request, ctx) -> Hat:
        self.methods.append(ctx.method)
        return Hat()

    async def intercept_client_stream(self, _call_next, _request, ctx) -> Hat:
        self.methods.append(ctx.method)
        return Hat()

    def intercept_server_stream(self, _call_next, _request, ctx) -> AsyncIterator[Hat]:
        self.methods.append(ctx.method)
        return _one_hat()

    def intercept_bidi_stream(self, _call_next, _request, ctx) -> AsyncIterator[Hat]:
        self.methods.append(ctx.method)
        return _one_hat()


class CapturingInterceptorSync:
    """A sync interceptor recording method info and short-circuiting the RPC."""

    def __init__(self) -> None:
        self.methods: list[MethodInfo[Size, Hat]] = []

    def intercept_unary_sync(self, _call_next, _request, ctx) -> Hat:
        self.methods.append(ctx.method)
        return Hat()

    def intercept_client_stream_sync(self, _call_next, _request, ctx) -> Hat:
        self.methods.append(ctx.method)
        return Hat()

    def intercept_server_stream_sync(self, _call_next, _request, ctx) -> Iterator[Hat]:
        self.methods.append(ctx.method)
        return iter([Hat()])

    def intercept_bidi_stream_sync(self, _call_next, _request, ctx) -> Iterator[Hat]:
        self.methods.append(ctx.method)
        return iter([Hat()])


class StreamTypeMetadataInterceptor:
    """A metadata interceptor recording the stream type of each RPC it sees."""

    def __init__(self) -> None:
        self.stream_types: list[StreamType | None] = []

    async def on_start(self, ctx: RequestContext) -> None:
        self.stream_types.append(ctx.method.stream_type)

    async def on_end(self, token, ctx, error) -> None:
        pass


def test_stream_type_values() -> None:
    assert StreamType.UNARY.value == "unary"
    assert StreamType.CLIENT_STREAM.value == "client_stream"
    assert StreamType.SERVER_STREAM.value == "server_stream"
    assert StreamType.BIDI_STREAM.value == "bidi_stream"


def test_method_info_stream_type_defaults_to_none() -> None:
    assert _method().stream_type is None


def test_method_info_accepts_stream_type() -> None:
    method = _method(StreamType.SERVER_STREAM)
    assert method.stream_type is StreamType.SERVER_STREAM
    assert method.idempotency_level is IdempotencyLevel.NO_SIDE_EFFECTS


def test_endpoint_unary_fills_in_stream_type() -> None:
    endpoint = Endpoint.unary(method=_method(), function=_async_unary)
    assert endpoint.method.stream_type is StreamType.UNARY


def test_endpoint_client_stream_fills_in_stream_type() -> None:
    endpoint = Endpoint.client_stream(method=_method(), function=_async_client_stream)
    assert endpoint.method.stream_type is StreamType.CLIENT_STREAM


def test_endpoint_server_stream_fills_in_stream_type() -> None:
    endpoint = Endpoint.server_stream(method=_method(), function=_async_server_stream)
    assert endpoint.method.stream_type is StreamType.SERVER_STREAM


def test_endpoint_bidi_stream_fills_in_stream_type() -> None:
    endpoint = Endpoint.bidi_stream(method=_method(), function=_async_bidi_stream)
    assert endpoint.method.stream_type is StreamType.BIDI_STREAM


def test_endpoint_sync_unary_fills_in_stream_type() -> None:
    endpoint = EndpointSync.unary(method=_method(), function=_sync_unary)
    assert endpoint.method.stream_type is StreamType.UNARY


def test_endpoint_sync_client_stream_fills_in_stream_type() -> None:
    endpoint = EndpointSync.client_stream(
        method=_method(), function=_sync_client_stream
    )
    assert endpoint.method.stream_type is StreamType.CLIENT_STREAM


def test_endpoint_sync_server_stream_fills_in_stream_type() -> None:
    endpoint = EndpointSync.server_stream(
        method=_method(), function=_sync_server_stream
    )
    assert endpoint.method.stream_type is StreamType.SERVER_STREAM


def test_endpoint_sync_bidi_stream_fills_in_stream_type() -> None:
    endpoint = EndpointSync.bidi_stream(method=_method(), function=_sync_bidi_stream)
    assert endpoint.method.stream_type is StreamType.BIDI_STREAM


def test_endpoint_keeps_existing_stream_type() -> None:
    method = _method(StreamType.SERVER_STREAM)
    endpoint = Endpoint.unary(method=method, function=_async_unary)
    assert endpoint.method.stream_type is StreamType.SERVER_STREAM


def test_endpoint_sync_keeps_existing_stream_type() -> None:
    method = _method(StreamType.CLIENT_STREAM)
    endpoint = EndpointSync.unary(method=method, function=_sync_unary)
    assert endpoint.method.stream_type is StreamType.CLIENT_STREAM


def test_endpoint_fill_in_does_not_mutate_original_method_info() -> None:
    method = _method()
    endpoint = Endpoint.unary(method=method, function=_async_unary)
    assert method.stream_type is None
    assert endpoint.method is not method


@pytest.mark.asyncio
async def test_client_execute_unary_fills_in_stream_type() -> None:
    interceptor = CapturingInterceptor()
    async with ConnectClient(
        "http://localhost:8000", interceptors=[interceptor]
    ) as client:
        await client.execute_unary(request=Size(inches=1), method=_method())
    assert [m.stream_type for m in interceptor.methods] == [StreamType.UNARY]


@pytest.mark.asyncio
async def test_client_execute_client_stream_fills_in_stream_type() -> None:
    interceptor = CapturingInterceptor()
    async with ConnectClient(
        "http://localhost:8000", interceptors=[interceptor]
    ) as client:
        await client.execute_client_stream(request=_one_size(), method=_method())
    assert [m.stream_type for m in interceptor.methods] == [StreamType.CLIENT_STREAM]


@pytest.mark.asyncio
async def test_client_execute_server_stream_fills_in_stream_type() -> None:
    interceptor = CapturingInterceptor()
    async with ConnectClient(
        "http://localhost:8000", interceptors=[interceptor]
    ) as client:
        async for _hat in client.execute_server_stream(
            request=Size(inches=1), method=_method()
        ):
            pass
    assert [m.stream_type for m in interceptor.methods] == [StreamType.SERVER_STREAM]


@pytest.mark.asyncio
async def test_client_execute_bidi_stream_fills_in_stream_type() -> None:
    interceptor = CapturingInterceptor()
    async with ConnectClient(
        "http://localhost:8000", interceptors=[interceptor]
    ) as client:
        async for _hat in client.execute_bidi_stream(
            request=_one_size(), method=_method()
        ):
            pass
    assert [m.stream_type for m in interceptor.methods] == [StreamType.BIDI_STREAM]


def test_client_sync_execute_unary_fills_in_stream_type() -> None:
    interceptor = CapturingInterceptorSync()
    with ConnectClientSync(
        "http://localhost:8000", interceptors=[interceptor]
    ) as client:
        client.execute_unary(request=Size(inches=1), method=_method())
    assert [m.stream_type for m in interceptor.methods] == [StreamType.UNARY]


def test_client_sync_execute_client_stream_fills_in_stream_type() -> None:
    interceptor = CapturingInterceptorSync()
    with ConnectClientSync(
        "http://localhost:8000", interceptors=[interceptor]
    ) as client:
        client.execute_client_stream(request=iter([Size(inches=1)]), method=_method())
    assert [m.stream_type for m in interceptor.methods] == [StreamType.CLIENT_STREAM]


def test_client_sync_execute_server_stream_fills_in_stream_type() -> None:
    interceptor = CapturingInterceptorSync()
    with ConnectClientSync(
        "http://localhost:8000", interceptors=[interceptor]
    ) as client:
        for _hat in client.execute_server_stream(
            request=Size(inches=1), method=_method()
        ):
            pass
    assert [m.stream_type for m in interceptor.methods] == [StreamType.SERVER_STREAM]


def test_client_sync_execute_bidi_stream_fills_in_stream_type() -> None:
    interceptor = CapturingInterceptorSync()
    with ConnectClientSync(
        "http://localhost:8000", interceptors=[interceptor]
    ) as client:
        for _hat in client.execute_bidi_stream(
            request=iter([Size(inches=1)]), method=_method()
        ):
            pass
    assert [m.stream_type for m in interceptor.methods] == [StreamType.BIDI_STREAM]


@pytest.mark.asyncio
async def test_client_execute_keeps_existing_stream_type() -> None:
    interceptor = CapturingInterceptor()
    async with ConnectClient(
        "http://localhost:8000", interceptors=[interceptor]
    ) as client:
        async for _hat in client.execute_server_stream(
            request=Size(inches=1), method=_method(StreamType.UNARY)
        ):
            pass
    assert [m.stream_type for m in interceptor.methods] == [StreamType.UNARY]


def test_client_sync_execute_keeps_existing_stream_type() -> None:
    interceptor = CapturingInterceptorSync()
    with ConnectClientSync(
        "http://localhost:8000", interceptors=[interceptor]
    ) as client:
        for _hat in client.execute_bidi_stream(
            request=iter([Size(inches=1)]), method=_method(StreamType.UNARY)
        ):
            pass
    assert [m.stream_type for m in interceptor.methods] == [StreamType.UNARY]


@pytest.mark.asyncio
async def test_generated_client_method_carries_stream_type() -> None:
    interceptor = CapturingInterceptor()
    async with HaberdasherClient(
        "http://localhost:8000", interceptors=[interceptor]
    ) as client:
        await client.make_hat(Size(inches=1))
    assert [m.stream_type for m in interceptor.methods] == [StreamType.UNARY]


@pytest.mark.asyncio
async def test_generated_client_streaming_method_carries_stream_type() -> None:
    interceptor = CapturingInterceptor()
    async with HaberdasherClient(
        "http://localhost:8000", interceptors=[interceptor]
    ) as client:
        async for _hat in client.make_similar_hats(Size(inches=1)):
            pass
    assert [m.stream_type for m in interceptor.methods] == [StreamType.SERVER_STREAM]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method_name", "request_", "expected"),
    [
        pytest.param("make_hat", Size(inches=10), StreamType.UNARY, id="unary"),
        pytest.param(
            "make_flexible_hat",
            [Size(inches=10), Size(inches=20)],
            StreamType.CLIENT_STREAM,
            id="client_stream",
        ),
        pytest.param(
            "make_similar_hats",
            Size(inches=15),
            StreamType.SERVER_STREAM,
            id="server_stream",
        ),
        pytest.param(
            "make_various_hats",
            [Size(inches=25), Size(inches=35)],
            StreamType.BIDI_STREAM,
            id="bidi_stream",
        ),
    ],
)
async def test_metadata_interceptor_sees_stream_type(
    method_name: str, request_: Size | list[Size], expected: StreamType
) -> None:
    client_interceptor = StreamTypeMetadataInterceptor()
    server_interceptor = StreamTypeMetadataInterceptor()
    app = HaberdasherASGIApplication(
        HaberdasherService(), interceptors=[server_interceptor]
    )
    async with HaberdasherClient(
        "http://localhost",
        http_client=Client(ASGITransport(app)),
        interceptors=[client_interceptor],
    ) as client:
        await call(getattr(client, method_name), request_)
    assert client_interceptor.stream_types == [expected]
    assert server_interceptor.stream_types == [expected]
