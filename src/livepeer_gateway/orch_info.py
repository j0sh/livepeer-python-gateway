from __future__ import annotations

import ipaddress
import logging
import os
import socket
import ssl
import tempfile
from dataclasses import dataclass
from functools import lru_cache
from typing import Optional, Tuple
from urllib.parse import urlparse

import grpc

from . import lp_rpc_pb2
from . import lp_rpc_pb2_grpc
from .errors import LivepeerGatewayError
from .remote_signer import get_orch_info_sig

_LOG = logging.getLogger(__name__)


@dataclass
class OrchestratorRpcError(LivepeerGatewayError):
    orch_url: str
    message: str
    cause: Optional[BaseException] = None

    def __str__(self) -> str:
        return f"Orchestrator RPC error: {self.message} (orch={self.orch_url})"


def create_orchestrator_stub(
    orch_url: str,
) -> Tuple[grpc.Channel, lp_rpc_pb2_grpc.OrchestratorStub]:
    # Always use TLS. "Ignore" invalid/self-signed certs by trusting the exact
    # certificate the server presents (trust-on-first-use) and overriding the
    # expected authority to match that cert.
    root_pem, authority, target = _trust_on_first_use_root_cert(orch_url)
    credentials = grpc.ssl_channel_credentials(root_certificates=root_pem)
    options = [
        ("grpc.ssl_target_name_override", authority),
        ("grpc.default_authority", authority),
    ]
    channel = grpc.secure_channel(target, credentials, options=options)
    stub = lp_rpc_pb2_grpc.OrchestratorStub(channel)
    return channel, stub


def call_get_orchestrator(
    stub: lp_rpc_pb2_grpc.OrchestratorStub,
    request: lp_rpc_pb2.OrchestratorRequest,
    orch_url: str,
) -> lp_rpc_pb2.OrchestratorInfo:
    try:
        return stub.GetOrchestrator(request, timeout=5.0)
    except grpc.RpcError as e:
        # e.details() may be None; be defensive
        details = ""
        try:
            details = e.details() or ""
        except Exception:
            details = ""

        code = ""
        try:
            code = str(e.code())
        except Exception:
            code = "UNKNOWN"

        msg = details or repr(e)
        raise OrchestratorRpcError(orch_url, f"{code}: {msg}", cause=e) from None


def get_orch_info(
    orch_url: str,
    *,
    signer_url: Optional[str] = None,
    capabilities: Optional[lp_rpc_pb2.Capabilities] = None,
) -> lp_rpc_pb2.OrchestratorInfo:
    """
    Fetch orchestrator info over gRPC.

    Public functional API:
        get_orch_info(orch_url, signer_url=...)
    Remote signer is called once per process (cached).
    Always uses secure channel (TLS) with certificate verification disabled.
    """
    _LOG.debug(
        "Fetching orchestrator info orch=%s",
        orch_url,
        signer_url or "",
        "set" if capabilities is not None else "none",
    )
    try:
        signer = get_orch_info_sig(signer_url)
    except Exception as e:
        # Ensure caller sees a clean library error (no raw traceback).
        raise OrchestratorRpcError(
            orch_url,
            f"{e.__class__.__name__}: {e}",
            cause=e,
        ) from None

    request = lp_rpc_pb2.OrchestratorRequest(
        address=signer.address,
        sig=signer.sig,
        ignoreCapacityCheck=True,
    )
    if capabilities is not None:
        request.capabilities.CopyFrom(capabilities)

    _, stub = create_orchestrator_stub(orch_url)
    return call_get_orchestrator(stub, request, orch_url)


def _split_host_port(target: str) -> Tuple[str, int]:
    """
    Parse a gRPC target in the form "host:port" or "[ipv6]:port".
    """
    target = target.strip()
    if target.startswith("["):
        # [::1]:8935
        host, rest = target[1:].split("]", 1)
        if not rest.startswith(":"):
            raise ValueError(f"Invalid gRPC target (missing port): {target!r}")
        return host, int(rest[1:])

    # host:port (we don't support bare host here)
    if target.count(":") != 1:
        raise ValueError(f"Invalid gRPC target (expected host:port): {target!r}")
    host, port_s = target.split(":", 1)
    return host, int(port_s)


def _parse_grpc_target(orch_url: str) -> str:
    """
    Normalize user input into a gRPC target string suitable for grpc.secure_channel().

    Accepts:
    - "host:port"
    - "[ipv6]:port"
    - "https://host:port" (scheme is stripped; TLS is still used by the channel)
    """
    orch_url = orch_url.strip()
    # If no scheme is provided, treat it as https://... implicitly.
    # Also avoids `urlparse("localhost:8935")` interpreting "localhost" as a scheme.
    url = orch_url if "://" in orch_url else f"https://{orch_url}"

    parsed = urlparse(url)
    if parsed.scheme != "https":
        raise ValueError(f"Only https:// orchestrator URLs are supported (got {parsed.scheme!r})")
    if not parsed.netloc:
        raise ValueError(f"Invalid orchestrator URL: {orch_url!r}")
    if parsed.path not in ("", "/") or parsed.params or parsed.query or parsed.fragment:
        # gRPC targets are host:port; ignore any path-like components by rejecting explicitly.
        raise ValueError(f"Orchestrator URL must not include a path/query/fragment: {orch_url!r}")
    return parsed.netloc


def _is_ip_address(host: str) -> bool:
    try:
        ipaddress.ip_address(host)
        return True
    except ValueError:
        return False


def _pick_cert_authority(cert: dict) -> Optional[str]:
    """
    Choose an authority value that will satisfy gRPC hostname verification.
    Prefers DNS SAN, then IP SAN, then Common Name.
    """
    san = cert.get("subjectAltName") or []
    for typ, val in san:
        if typ == "DNS" and val:
            return val
    for typ, val in san:
        if typ in ("IP Address", "IP") and val:
            return val

    # subject is a tuple of RDN tuples: ((('commonName','example.com'),), ...)
    for rdn in cert.get("subject") or []:
        for key, val in rdn:
            if key == "commonName" and val:
                return val
    return None


def _decode_pem_cert(pem: bytes) -> dict:
    """
    Decode a PEM cert into a dict containing subject / subjectAltName.

    Note: Python's `SSLSocket.getpeercert()` returns `{}` when verify_mode=CERT_NONE,
    so we decode the PEM ourselves via a CPython helper.
    """
    try:
        decode = ssl._ssl._test_decode_cert  # pyright: ignore[reportAttributeAccessIssue]
    except Exception:
        return {}

    tmp_path: Optional[str] = None
    try:
        with tempfile.NamedTemporaryFile(mode="wb", delete=False) as f:
            f.write(pem)
            tmp_path = f.name
        return decode(tmp_path) or {}
    finally:
        if tmp_path:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass


@lru_cache(maxsize=None)
def _trust_on_first_use_root_cert_target(target: str) -> Tuple[bytes, str]:
    """
    Fetch the server certificate via a TLS handshake with verification disabled,
    then "trust" that exact certificate by using it as the root cert for gRPC.

    Returns (root_cert_pem_bytes, authority_override).
    """
    host, port = _split_host_port(target)

    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    # gRPC requires HTTP/2; many gRPC-TLS servers expect ALPN "h2" in the ClientHello.
    try:
        ctx.set_alpn_protocols(["h2"])
    except NotImplementedError:
        # Some Python/OpenSSL builds may not support ALPN; best effort.
        pass

    # Only send SNI for DNS names; IP SNI can trigger "Invalid server name indication".
    server_hostname = None if _is_ip_address(host) else host

    with socket.create_connection((host, port), timeout=5.0) as sock:
        with ctx.wrap_socket(sock, server_hostname=server_hostname) as ssock:
            der = ssock.getpeercert(binary_form=True)

    pem = ssl.DER_cert_to_PEM_cert(der).encode("ascii")
    decoded = _decode_pem_cert(pem)
    authority = _pick_cert_authority(decoded) or host
    return pem, authority


def _trust_on_first_use_root_cert(orch_url: str) -> Tuple[bytes, str, str]:
    """
    Wrapper that:
    - accepts https://host:port or host:port
    - returns (root_pem, authority, target)
    - re-raises failures as OrchestratorRpcError with the *original* orch_url
    """
    try:
        target = _parse_grpc_target(orch_url)
        root_pem, authority = _trust_on_first_use_root_cert_target(target)
        return root_pem, authority, target
    except Exception as e:
        raise OrchestratorRpcError(
            orch_url,
            f"TLS trust-on-first-use probe failed: {e.__class__.__name__}: {e}",
            cause=e,
        ) from None
