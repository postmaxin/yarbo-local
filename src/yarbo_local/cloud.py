"""Yarbo's cloud MQTT broker, for when the robot's own LAN broker is closed.

Firmware 3.14.41 closed the anonymous broker on the base station, so a LAN-only client sees
nothing. The robot still talks to Yarbo's cloud broker, which this module reaches with the
account session (``username``, ``token``, ``refresh_token``) that the vendor app and the
vendor SDK also use. The topics are the same; only the broker and the credentials differ.

This module never refreshes a token. Refresh tokens rotate, so spending one outside the
job that owns the session file can lock the account out. It reads the access token that is
on disk and uses it until it expires; a connect with an expired token fails and the session
retries, re-reading the file each time, so a token the refresh job rotated in is picked up
without a restart.

Standard library only: the broker hosts and the per-robot routing decision come from two
plain HTTPS GETs, the same ones the vendor's ``yarbo-data-sdk`` makes.
"""

from __future__ import annotations

import asyncio
import base64
from collections.abc import Callable
from dataclasses import dataclass, field
from http.client import HTTPException
import json
from pathlib import Path
import secrets
import time
from typing import Any
import urllib.error
import urllib.parse
import urllib.request

from .exceptions import ConnectionLostError, YarboError
from .transport import MqttTransport

DEFAULT_API_BASE = "https://data-api.yarbo.ai"
CONFIG_PATH = "/sdk/config"
# Fallbacks the vendor SDK ships, used when /sdk/config does not carry the new-broker keys.
NEW_MQTT_HOST = "c4750009.ala.dedicated.aws.emqxcloud.com"
NEW_MQTT_PORT = 8883
MIGRATION_QUERY_URL = (
    "https://gu7e5n9ef4.execute-api.us-east-1.amazonaws.com/Stage/yarbo/emqx-migration/query"
)
CLIENT_ID_PREFIX = "HA_"  # the brokers refuse client ids that do not follow the SDK's scheme
EXPIRY_MARGIN = 60.0
HTTP_TIMEOUT = 20.0

HttpGet = Callable[[str, dict[str, str] | None, float], dict[str, Any]]


class CloudSessionError(YarboError):
    """The cloud broker cannot be resolved: no usable session, or Yarbo's API said no."""


@dataclass(frozen=True, slots=True)
class CloudBroker:
    """Where to connect and as whom. Either credential can be the bearer token (the legacy
    broker takes it as the username), so neither is in ``repr``."""

    host: str
    port: int
    tls: bool
    username: str = field(repr=False)
    password: str = field(repr=False)
    client_id: str
    migrated: bool

    def transport(self, **kwargs: Any) -> MqttTransport:
        """A one-shot transport with these credentials. Prefer :class:`CloudTransport`, which
        re-resolves on every reconnect."""
        return MqttTransport(
            self.host,
            self.port,
            tls=self.tls,
            tls_verify=self.tls,
            username=self.username,
            password=self.password,
            identifier=self.client_id,
            **kwargs,
        )


def _http_get_json(url: str, headers: dict[str, str] | None, timeout: float) -> dict[str, Any]:
    request = urllib.request.Request(url, headers=headers or {})  # noqa: S310  (https URLs only)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310
            body = json.load(response)
    except urllib.error.HTTPError as err:
        if err.code == 401:
            raise CloudSessionError("Yarbo's API rejected the access token (HTTP 401)") from err
        raise CloudSessionError(
            f"GET {url.split('?', maxsplit=1)[0]} returned HTTP {err.code}"
        ) from err
    except (OSError, ValueError, HTTPException) as err:  # incl. a truncated (IncompleteRead) body
        raise CloudSessionError(f"GET {url.split('?', maxsplit=1)[0]} failed: {err}") from err
    if not isinstance(body, dict):
        raise CloudSessionError(f"GET {url.split('?', maxsplit=1)[0]} returned a non-object body")
    return body


def token_expiry(token: str) -> float | None:
    """The JWT's ``exp`` (Unix seconds), or None if it cannot be read. Not a signature check."""
    try:
        payload = token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        exp = json.loads(base64.urlsafe_b64decode(payload)).get("exp")
    except (IndexError, ValueError, AttributeError):
        return None
    return float(exp) if isinstance(exp, (int, float)) else None


def read_session(path: str | Path, *, now: float | None = None) -> tuple[str, str]:
    """``(username, token)`` from the session file, refusing a missing or expired token."""
    try:
        data = json.loads(Path(path).read_text())
        username, token = str(data["username"]), str(data["token"])
    except (OSError, ValueError, KeyError, TypeError) as err:
        raise CloudSessionError(f"cannot read a Yarbo session from {path}: {err!r}") from err
    if not username or not token:
        raise CloudSessionError(f"the session in {path} has an empty username or token")
    exp = token_expiry(token)
    if exp is not None and exp <= (time.time() if now is None else now) + EXPIRY_MARGIN:
        raise CloudSessionError(
            "the access token has expired; waiting for the session refresh job to rotate it"
        )
    return username, token


def _port(value: Any) -> int:
    """A port from Yarbo's config, or CloudSessionError (so the session retries, not crashes)."""
    try:
        port = int(value)
    except (TypeError, ValueError) as err:
        raise CloudSessionError(f"Yarbo's config has a non-numeric MQTT port: {value!r}") from err
    if not 0 < port < 65536:
        raise CloudSessionError(f"Yarbo's config has an out-of-range MQTT port: {port}")
    return port


def resolve_cloud_broker(
    session_file: str | Path,
    serial: str,
    *,
    api_base: str = DEFAULT_API_BASE,
    http_get: HttpGet | None = None,
    now: float | None = None,
    timeout: float = HTTP_TIMEOUT,
) -> CloudBroker:
    """Work out which cloud broker carries this robot and the credentials to use there.

    New (dedicated EMQX) broker: ``user=email, password=JWT``. Legacy broker: ``user=JWT,
    password=""``. If the routing query fails this raises rather than guessing legacy: a
    migrated robot publishes only on the new broker, so a wrong guess is silent data loss.
    """
    get = http_get or _http_get_json
    username, token = read_session(session_file, now=now)
    cfg = get(api_base.rstrip("/") + CONFIG_PATH, None, timeout)
    cfg = cfg["data"] if isinstance(cfg.get("data"), dict) else cfg
    query_url = str(cfg.get("emqx_migration_query_url") or MIGRATION_QUERY_URL)
    sep = "&" if "?" in query_url else "?"
    reply = get(
        f"{query_url}{sep}{urllib.parse.urlencode({'sn': serial})}",
        {"Authorization": f"Bearer {token}"},
        timeout,
    )
    if not reply.get("success"):
        raise CloudSessionError(
            f"the migration query was rejected: code={reply.get('code')} msg={reply.get('message')}"
        )
    data = reply.get("data")
    migrated = data.get("migrated") if isinstance(data, dict) else None
    if not isinstance(migrated, bool):
        # No routing answer is not "legacy": a migrated robot publishes only on the new
        # broker, so guessing would connect to the wrong one and hear nothing.
        raise CloudSessionError("the migration query returned no boolean 'migrated' decision")
    rand4 = f"{secrets.randbelow(10000):04d}"
    if migrated:
        return CloudBroker(
            host=str(cfg.get("new_mqtt_host") or NEW_MQTT_HOST),
            port=_port(cfg.get("new_mqtt_port") or NEW_MQTT_PORT),
            tls=bool(cfg.get("new_mqtt_use_tls", True)),
            username=username,
            password=token,
            client_id=f"{CLIENT_ID_PREFIX}{username.lower()}_{rand4}",
            migrated=True,
        )
    host = cfg.get("mqtt_host")
    port = cfg.get("mqtt_port")
    if not host or not port:
        raise CloudSessionError("Yarbo's config names no legacy broker (mqtt_host/mqtt_port)")
    use_tls = cfg.get("mqtt_use_tls")
    return CloudBroker(
        host=str(host),
        port=_port(port),
        tls=True if use_tls is None else bool(use_tls),
        username=token,
        password="",
        client_id=f"{CLIENT_ID_PREFIX}{username.lower()}_{int(time.time())}_{rand4}",
        migrated=False,
    )


class CloudTransport(MqttTransport):
    """:class:`MqttTransport` aimed at Yarbo's cloud broker.

    Every ``connect()`` re-reads the session file and re-asks Yarbo which broker carries the
    robot, so the session's reconnect loop picks up a rotated token and a moved robot with no
    restart. Resolution failures surface as :class:`ConnectionLostError`, which the session
    already backs off and retries.
    """

    def __init__(
        self,
        session_file: str | Path,
        serial: str,
        *,
        api_base: str = DEFAULT_API_BASE,
        http_get: HttpGet | None = None,
        keepalive: int = 60,
        will: tuple[str, bytes] | None = None,
    ) -> None:
        super().__init__("", 0, tls=True, tls_verify=True, keepalive=keepalive, will=will)
        self.session_file = Path(session_file)
        self.serial = serial
        self._api_base = api_base
        self._http_get = http_get
        self.broker: CloudBroker | None = None

    async def connect(self) -> None:
        try:
            broker = await asyncio.to_thread(
                resolve_cloud_broker,
                self.session_file,
                self.serial,
                api_base=self._api_base,
                http_get=self._http_get,
            )
        except CloudSessionError as err:
            raise ConnectionLostError(f"cannot resolve the cloud broker: {err}") from err
        self.broker = broker
        self.host, self.port = broker.host, broker.port
        self.tls, self.tls_verify = broker.tls, broker.tls
        self.username, self._password = broker.username, broker.password
        self.identifier = broker.client_id
        await super().connect()
