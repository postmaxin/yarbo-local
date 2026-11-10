"""Cloud broker resolution and the credentialed transport, against fakes (no network)."""

from __future__ import annotations

import base64
from http.client import IncompleteRead
import http.server
import json
from pathlib import Path
import ssl
import threading
import time
from typing import Any, ClassVar

import pytest

from yarbo_local import cloud, transport
from yarbo_local.cloud import CloudSessionError, CloudTransport, resolve_cloud_broker
from yarbo_local.exceptions import ConnectionLostError
from yarbo_local.transport import MqttTransport

NOW = 1_800_000_000.0
SN = "TESTSERIAL0001"


def jwt(exp: float | None) -> str:
    def enc(obj: dict[str, Any]) -> str:
        return base64.urlsafe_b64encode(json.dumps(obj).encode()).decode().rstrip("=")

    return f"{enc({'alg': 'none'})}.{enc({} if exp is None else {'exp': exp})}.sig"


def write_session(tmp_path: Path, token: str, user: str = "Me@Example.com") -> Path:
    path = tmp_path / "session.json"
    path.write_text(json.dumps({"username": user, "token": token, "refresh_token": "r"}))
    return path


class FakeHttp:
    def __init__(self, *, migrated: bool = True, query: dict[str, Any] | None = None) -> None:
        self.calls: list[tuple[str, dict[str, str] | None]] = []
        self.cfg: dict[str, Any] = {
            "mqtt_host": "legacy.example",
            "mqtt_port": 15525,
            "mqtt_use_tls": True,
        }
        self.query = query or {"success": True, "code": "00000", "data": {"migrated": migrated}}

    def __call__(self, url: str, headers: dict[str, str] | None, timeout: float) -> dict[str, Any]:
        self.calls.append((url, headers))
        if url.endswith("/sdk/config"):
            return {"code": 0, "data": self.cfg}
        return self.query


def test_new_broker_uses_email_and_jwt(tmp_path: Path) -> None:
    token = jwt(NOW + 86400)
    http = FakeHttp(migrated=True)
    broker = resolve_cloud_broker(write_session(tmp_path, token), SN, http_get=http, now=NOW)
    assert (broker.host, broker.port, broker.tls) == (cloud.NEW_MQTT_HOST, 8883, True)
    assert (broker.username, broker.password) == ("Me@Example.com", token)
    assert broker.client_id.startswith("HA_me@example.com_")
    assert broker.migrated is True
    # the query is authenticated with the token and names the serial
    url, headers = http.calls[1]
    assert f"sn={SN}" in url
    assert headers == {"Authorization": f"Bearer {token}"}


def test_new_broker_prefers_hosts_from_the_config(tmp_path: Path) -> None:
    http = FakeHttp(migrated=True)
    http.cfg |= {"new_mqtt_host": "new.example", "new_mqtt_port": "9999", "new_mqtt_use_tls": True}
    broker = resolve_cloud_broker(
        write_session(tmp_path, jwt(NOW + 9999)), SN, http_get=http, now=NOW
    )
    assert (broker.host, broker.port) == ("new.example", 9999)


def test_legacy_broker_uses_jwt_as_username_and_empty_password(tmp_path: Path) -> None:
    token = jwt(NOW + 86400)
    broker = resolve_cloud_broker(
        write_session(tmp_path, token), SN, http_get=FakeHttp(migrated=False), now=NOW
    )
    assert (broker.host, broker.port, broker.tls) == ("legacy.example", 15525, True)
    assert (broker.username, broker.password) == (token, "")
    assert broker.migrated is False


@pytest.mark.parametrize("migrated", [True, False])
def test_repr_never_shows_the_token(tmp_path: Path, migrated: bool) -> None:
    # legacy puts the token in `username`, new puts it in `password`
    token = jwt(NOW + 86400)
    http = FakeHttp(migrated=migrated)
    broker = resolve_cloud_broker(write_session(tmp_path, token), SN, http_get=http, now=NOW)
    assert token not in repr(broker)
    assert token not in str(broker)


@pytest.mark.parametrize(
    "query",
    [
        {"success": True, "data": None},
        {"success": True, "data": {}},
        {"success": True},
        {"success": True, "data": {"migrated": "yes"}},
        {"success": True, "data": {"migrated": None}},
    ],
)
def test_missing_or_malformed_routing_decision_is_an_error(
    tmp_path: Path, query: dict[str, Any]
) -> None:
    path = write_session(tmp_path, jwt(NOW + 9999))
    with pytest.raises(CloudSessionError, match="migrated"):
        resolve_cloud_broker(path, SN, http_get=FakeHttp(query=query), now=NOW)


@pytest.mark.parametrize("migrated", [True, False])
@pytest.mark.parametrize("port", ["abc", [1], 70000])
def test_bad_port_in_the_config_is_a_session_error(
    tmp_path: Path, migrated: bool, port: Any
) -> None:
    http = FakeHttp(migrated=migrated)
    http.cfg |= {"new_mqtt_port": port, "mqtt_port": port}
    path = write_session(tmp_path, jwt(NOW + 9999))
    with pytest.raises(CloudSessionError, match="port"):
        resolve_cloud_broker(path, SN, http_get=http, now=NOW)


def test_truncated_http_body_is_a_session_error(monkeypatch: pytest.MonkeyPatch) -> None:
    class Truncated:
        def __enter__(self) -> Truncated:
            return self

        def __exit__(self, *exc: object) -> None:
            return None

        def read(self, *_: object) -> bytes:
            raise IncompleteRead(b"partial")

    monkeypatch.setattr(cloud.urllib.request, "urlopen", lambda *_a, **_k: Truncated())
    with pytest.raises(CloudSessionError, match="failed"):
        cloud._http_get_json("https://example.invalid/x?sn=1", None, 1.0)


def test_expired_token_is_refused_without_any_request(tmp_path: Path) -> None:
    http = FakeHttp()
    path = write_session(tmp_path, jwt(NOW - 1))
    with pytest.raises(CloudSessionError, match="expired"):
        resolve_cloud_broker(path, SN, http_get=http, now=NOW)
    assert http.calls == []  # it never reaches for a refresh, or anything else


def test_token_inside_the_margin_counts_as_expired(tmp_path: Path) -> None:
    path = write_session(tmp_path, jwt(NOW + 30))
    with pytest.raises(CloudSessionError, match="expired"):
        resolve_cloud_broker(path, SN, http_get=FakeHttp(), now=NOW)


def test_opaque_token_without_exp_is_passed_through(tmp_path: Path) -> None:
    broker = resolve_cloud_broker(
        write_session(tmp_path, "not-a-jwt"), SN, http_get=FakeHttp(), now=NOW
    )
    assert broker.password == "not-a-jwt"  # noqa: S105


@pytest.mark.parametrize("content", ["", "{}", "not json", '{"username": "a"}'])
def test_bad_session_file_is_a_session_error(tmp_path: Path, content: str) -> None:
    path = tmp_path / "session.json"
    path.write_text(content)
    with pytest.raises(CloudSessionError):
        resolve_cloud_broker(path, SN, http_get=FakeHttp(), now=NOW)


def test_missing_session_file(tmp_path: Path) -> None:
    with pytest.raises(CloudSessionError):
        resolve_cloud_broker(tmp_path / "nope.json", SN, http_get=FakeHttp(), now=NOW)


def test_failed_routing_query_does_not_fall_back_to_legacy(tmp_path: Path) -> None:
    http = FakeHttp(query={"success": False, "code": "A0230", "message": "bad token"})
    with pytest.raises(CloudSessionError, match="A0230"):
        resolve_cloud_broker(write_session(tmp_path, jwt(NOW + 9999)), SN, http_get=http, now=NOW)


# -- transport ---------------------------------------------------------------------------


class FakeAioClient:
    instances: ClassVar[list[FakeAioClient]] = []

    def __init__(self, host: str, **kwargs: Any) -> None:
        self.host, self.kwargs = host, kwargs
        self.subscribed: list[str] = []
        FakeAioClient.instances.append(self)

    async def __aenter__(self) -> FakeAioClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None

    async def subscribe(self, topic_filter: str) -> None:
        self.subscribed.append(topic_filter)


@pytest.fixture
def aio(monkeypatch: pytest.MonkeyPatch) -> type[FakeAioClient]:
    FakeAioClient.instances = []
    monkeypatch.setattr(transport.aiomqtt, "Client", FakeAioClient)
    return FakeAioClient


async def test_transport_passes_credentials_and_keeps_empty_password(
    aio: type[FakeAioClient],
) -> None:
    await MqttTransport("h", 1, username="u", password="").connect()
    assert aio.instances[0].kwargs["username"] == "u"
    assert aio.instances[0].kwargs["password"] == ""


async def test_transport_without_credentials_sends_none(aio: type[FakeAioClient]) -> None:
    await MqttTransport("robot", 1883).connect()
    kw = aio.instances[0].kwargs
    assert kw["username"] is None
    assert kw["password"] is None
    assert kw["tls_context"] is None


async def test_local_tls_stays_unverified(aio: type[FakeAioClient]) -> None:
    await MqttTransport("robot", 8883, tls=True).connect()
    ctx = aio.instances[0].kwargs["tls_context"]
    assert ctx.verify_mode == ssl.CERT_NONE
    assert ctx.check_hostname is False


async def test_tls_verify_checks_chain_and_hostname(aio: type[FakeAioClient]) -> None:
    await MqttTransport("cloud", 8883, tls=True, tls_verify=True).connect()
    ctx = aio.instances[0].kwargs["tls_context"]
    assert ctx.verify_mode == ssl.CERT_REQUIRED
    assert ctx.check_hostname is True


async def test_cloud_transport_resolves_on_every_connect(
    aio: type[FakeAioClient], tmp_path: Path
) -> None:
    path = write_session(tmp_path, jwt(time.time() + 86400))
    t = CloudTransport(path, SN, http_get=FakeHttp(migrated=True))
    await t.connect()
    # rotate the token on disk; the next connect must use the new one
    new_token = jwt(time.time() + 2 * 86400)
    write_session(tmp_path, new_token)
    await t.connect()
    first, second = aio.instances
    assert first.kwargs["password"] != second.kwargs["password"]
    assert second.kwargs["password"] == new_token
    assert second.host == cloud.NEW_MQTT_HOST
    assert second.kwargs["tls_context"].verify_mode == ssl.CERT_REQUIRED
    assert second.kwargs["identifier"].startswith("HA_me@example.com_")


async def test_cloud_transport_turns_resolution_failure_into_connection_lost(
    aio: type[FakeAioClient], tmp_path: Path
) -> None:
    t = CloudTransport(write_session(tmp_path, jwt(1.0)), SN, http_get=FakeHttp())
    with pytest.raises(ConnectionLostError, match="cloud broker"):
        await t.connect()
    assert aio.instances == []
    assert not t.connected


# -- hardening -----------------------------------------------------------------------------


@pytest.mark.parametrize("key", ["new_mqtt_use_tls", "mqtt_use_tls"])
def test_plaintext_broker_is_refused(tmp_path: Path, key: str) -> None:
    http = FakeHttp(migrated=key == "new_mqtt_use_tls")
    http.cfg |= {key: False}
    path = write_session(tmp_path, jwt(NOW + 9999))
    with pytest.raises(CloudSessionError, match="plaintext"):
        resolve_cloud_broker(path, SN, http_get=http, now=NOW)


@pytest.mark.parametrize("migrated", [True, False])
def test_null_tls_means_tls_not_plaintext(tmp_path: Path, migrated: bool) -> None:
    http = FakeHttp(migrated=migrated)
    http.cfg |= {"new_mqtt_use_tls": None, "mqtt_use_tls": None}
    path = write_session(tmp_path, jwt(NOW + 9999))
    assert resolve_cloud_broker(path, SN, http_get=http, now=NOW).tls is True


def test_non_https_migration_url_is_refused_before_the_token_is_sent(tmp_path: Path) -> None:
    http = FakeHttp()
    http.cfg |= {"emqx_migration_query_url": "http://evil.example/query"}
    path = write_session(tmp_path, jwt(NOW + 9999))
    with pytest.raises(CloudSessionError, match="non-https"):
        resolve_cloud_broker(path, SN, http_get=http, now=NOW)
    assert len(http.calls) == 1  # only the config fetch; no request carried the token


def test_non_https_api_base_is_refused(tmp_path: Path) -> None:
    path = write_session(tmp_path, jwt(NOW + 9999))
    with pytest.raises(CloudSessionError, match="non-https"):
        resolve_cloud_broker(path, SN, api_base="http://x.example", http_get=FakeHttp(), now=NOW)


async def test_malformed_url_becomes_connection_lost(
    aio: type[FakeAioClient], tmp_path: Path
) -> None:
    http = FakeHttp()
    http.cfg |= {"emqx_migration_query_url": "https://[bad"}
    t = CloudTransport(write_session(tmp_path, jwt(time.time() + 9999)), SN, http_get=http)
    with pytest.raises(ConnectionLostError):
        await t.connect()


def test_malformed_url_in_the_default_fetcher(tmp_path: Path) -> None:
    with pytest.raises(CloudSessionError):
        cloud._http_get_json("https://[bad", None, 1.0)


def test_redirects_are_not_followed() -> None:
    seen: list[str] = []

    class Target(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            seen.append(self.path)
            self.send_response(200)
            self.end_headers()

        def log_message(self, *_: object) -> None:
            return None

    target = http.server.HTTPServer(("127.0.0.1", 0), Target)

    class Redirector(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            self.send_response(302)
            self.send_header("Location", f"http://127.0.0.1:{target.server_port}/leak")
            self.end_headers()

        def log_message(self, *_: object) -> None:
            return None

    redirector = http.server.HTTPServer(("127.0.0.1", 0), Redirector)
    threads = [threading.Thread(target=s.serve_forever, daemon=True) for s in (target, redirector)]
    for th in threads:
        th.start()
    try:
        request = cloud.urllib.request.Request(
            f"http://127.0.0.1:{redirector.server_port}/", headers={"Authorization": "Bearer x"}
        )
        with pytest.raises(cloud.urllib.error.HTTPError) as exc:
            cloud._OPENER.open(request, timeout=5)
        assert exc.value.code == 302
    finally:
        target.shutdown()
        redirector.shutdown()
    assert seen == []
