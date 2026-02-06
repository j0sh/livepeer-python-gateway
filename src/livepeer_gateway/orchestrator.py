from __future__ import annotations

import json
import logging
import ssl
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache
from typing import Any, Optional, Sequence, Tuple
from urllib.parse import urlparse
from urllib.error import URLError, HTTPError
from urllib.request import Request, urlopen

from . import lp_rpc_pb2

from .errors import (
    LivepeerGatewayError,
    NoOrchestratorAvailableError,
    SignerRefreshRequired,
    SkipPaymentCycle,
)
from .orch_info import get_orch_info
from .remote_signer import RemoteSignerError

_LOG = logging.getLogger(__name__)

def _truncate(s: str, max_len: int = 2000) -> str:
    if len(s) <= max_len:
        return s
    return s[:max_len] + f"...(+{len(s) - max_len} chars)"

def _http_error_body(e: HTTPError) -> str:
    """
    Best-effort read of an HTTPError response body for debugging.
    """
    try:
        b = e.read()
        if not b:
            return ""
        if isinstance(b, bytes):
            return b.decode("utf-8", errors="replace")
        return str(b)
    except Exception:
        return ""

def _extract_error_message(e: HTTPError) -> str:
    """
    Best-effort extraction of a useful error message from an HTTPError body.

    If the body is JSON and matches {"error": {"message": "..."}}, return that message.
    Otherwise return the full body.

    Always truncates the returned value for readability.
    """
    body = _http_error_body(e)
    s = body.strip()
    if not s:
        return ""

    try:
        data = json.loads(s)
    except Exception:
        return _truncate(body)

    if isinstance(data, dict):
        err = data.get("error")
        if isinstance(err, dict):
            msg = err.get("message")
            if isinstance(msg, str) and msg:
                return _truncate(msg)

    return _truncate(body)


def request_json(
    url: str,
    *,
    method: Optional[str] = None,
    payload: Optional[dict[str, Any]] = None,
    headers: Optional[dict[str, str]] = None,
    timeout: float = 5.0,
) -> Any:
    """
    Make a JSON HTTP request and parse the JSON response.

    If method is None, defaults to POST when payload is provided, otherwise GET.

    Raises LivepeerGatewayError on HTTP/network/JSON parsing errors.
    """
    req_headers: dict[str, str] = {
        "Accept": "application/json",
        "User-Agent": "livepeer-python-gateway/0.1",
    }
    body: Optional[bytes] = None
    if payload is not None:
        req_headers["Content-Type"] = "application/json"
        body = json.dumps(payload).encode("utf-8")
    if headers:
        req_headers.update(headers)

    resolved_method = method.upper() if method else ("POST" if payload is not None else "GET")
    req = Request(url, data=body, headers=req_headers, method=resolved_method)

    # Always ignore HTTPS certificate validation (matches our gRPC behavior).
    ssl_ctx = ssl._create_unverified_context()

    try:
        with urlopen(req, timeout=timeout, context=ssl_ctx) as resp:
            raw = resp.read().decode("utf-8")
        data: Any = json.loads(raw)
    except HTTPError as e:
        body = _extract_error_message(e)
        body_part = f"; body={body!r}" if body else ""
        if e.code == 480:
            raise SignerRefreshRequired(
                f"Signer returned HTTP 480 (refresh session required) (url={url}){body_part}"
            ) from e
        if e.code == 482:
            raise SkipPaymentCycle(
                f"Signer returned HTTP 482 (skip payment cycle) (url={url}){body_part}"
            ) from e
        raise LivepeerGatewayError(
            f"HTTP JSON error: HTTP {e.code} from endpoint (url={url}){body_part}"
        ) from e
    except ConnectionRefusedError as e:
        raise LivepeerGatewayError(
            f"HTTP JSON error: connection refused (is the server running? is the host/port correct?) (url={url})"
        ) from e
    except URLError as e:
        raise LivepeerGatewayError(
            f"HTTP JSON error: failed to reach endpoint: {getattr(e, 'reason', e)} (url={url})"
        ) from e
    except json.JSONDecodeError as e:
        raise LivepeerGatewayError(f"HTTP JSON error: endpoint did not return valid JSON: {e} (url={url})") from e
    except Exception as e:
        raise LivepeerGatewayError(
            f"HTTP JSON error: unexpected error: {e.__class__.__name__}: {e} (url={url})"
        ) from e

    return data


def post_json(
    url: str,
    payload: dict[str, Any],
    *,
    headers: Optional[dict[str, str]] = None,
    timeout: float = 5.0,
) -> dict[str, Any]:
    """
    POST JSON to `url` and parse a JSON object response.
    """
    data = request_json(
        url,
        payload=payload,
        headers=headers,
        timeout=timeout,
    )
    if not isinstance(data, dict):
        raise LivepeerGatewayError(
            f"HTTP JSON error: expected JSON object, got {type(data).__name__} (url={url})"
        )
    return data


def get_json(
    url: str,
    *,
    headers: Optional[dict[str, str]] = None,
    timeout: float = 5.0,
) -> Any:
    """
    GET JSON from `url` and parse the response.
    """
    return request_json(url, headers=headers, timeout=timeout)

def _parse_http_url(url: str, *, context: str = "URL") -> ParseResult:
    """
    Normalize a URL for HTTP(S) endpoints.

    Accepts:
    - "host:port" (implicitly https://host:port)
    - "http://host:port[/...]"
    - "https://host:port[/...]"
    """
    url = url.strip()
    normalized = url if "://" in url else f"https://{url}"
    parsed = urlparse(normalized)
    if parsed.scheme not in ("http", "https"):
        raise ValueError(f"Only http:// or https:// {context}s are supported (got {parsed.scheme!r})")
    if not parsed.netloc:
        raise ValueError(f"Invalid {context}: {url!r}")
    return parsed


def _http_origin(url: str) -> str:
    """
    Normalize a URL (possibly with a path) into a scheme:// origin (scheme + host:port).

    Accepts:
    - "host:port" (implicitly https://host:port)
    - "http://host:port[/...]" (path/query/fragment are ignored)
    - "https://host:port[/...]" (path/query/fragment are ignored)
    """
    parsed = _parse_http_url(url)
    return f"{parsed.scheme}://{parsed.netloc}"


def DiscoverOrchestrators(
    orchestrators: Optional[Sequence[str] | str] = None,
    *,
    signer_url: Optional[str] = None,
    discovery_url: Optional[str] = None,
) -> list[str]:
    """
    Discover orchestrators and return a list of addresses.

    This discovery can happen via the following parameters in priority order (highest first):
    - orchestrators: list or comma-delimited string
      (empty/whitespace-only input falls through)
    - discovery_url: use this discovery endpoint
    - signer_url: use signer-provided discovery service
    """
    if orchestrators is not None:
        if isinstance(orchestrators, str):
            orch_list = [orch.strip() for orch in orchestrators.split(",")]
        else:
            try:
                orch_list = list(orchestrators)
            except TypeError as e:
                raise LivepeerGatewayError(
                    "DiscoverOrchestrators requires a list of orchestrator URLs or a comma-delimited string"
                ) from e
        orch_list = [orch.strip() for orch in orch_list if isinstance(orch, str) and orch.strip()]
        if orch_list:
            return orch_list

    if discovery_url:
        discovery_endpoint = _parse_http_url(discovery_url).geturl()
    elif signer_url:
        discovery_endpoint = f"{_http_origin(signer_url)}/discover-orchestrators"
    else:
        _LOG.debug("DiscoverOrchestrators failed: no discovery inputs")
        raise LivepeerGatewayError("DiscoverOrchestrators requires discovery_url or signer_url")

    try:
        _LOG.debug("DiscoverOrchestrators running discovery: %s", discovery_endpoint)
        data = get_json(discovery_endpoint)
    except LivepeerGatewayError as e:
        _LOG.debug("DiscoverOrchestrators discovery failed: %s", e)
        raise RemoteSignerError(
            discovery_endpoint,
            str(e),
            cause=e.__cause__ or e,
        ) from None

    if not isinstance(data, list):
        _LOG.debug(
            "DiscoverOrchestrators discovery response not list: type=%s",
            type(data).__name__,
        )
        raise RemoteSignerError(
            discovery_endpoint,
            f"Discovery response must be a JSON list, got {type(data).__name__}",
            cause=None,
        ) from None

    _LOG.debug("DiscoverOrchestrators discovery response: %s", data)

    orch_list = []
    for item in data:
        if not isinstance(item, dict):
            continue
        address = item.get("address")
        if isinstance(address, str) and address.strip():
            orch_list.append(address.strip())
    _LOG.debug("DiscoverOrchestrators discovered %d orchestrators", len(orch_list))

    return orch_list


def SelectOrchestrator(
    orchestrators: Optional[Sequence[str] | str] = None,
    *,
    signer_url: Optional[str] = None,
    discovery_url: Optional[str] = None,
    capabilities: Optional[lp_rpc_pb2.Capabilities] = None,
) -> Tuple[str, lp_rpc_pb2.OrchestratorInfo]:
    """
    Select an orchestrator by trying up to ~5 candidates in parallel.

    If orchestrators is empty/None, a discovery endpoint is used:
    - discovery_url, if provided
    - otherwise {signer_url}/discover-orchestrators
    """
    orch_list = DiscoverOrchestrators(
        orchestrators,
        signer_url=signer_url,
        discovery_url=discovery_url,
    )

    if not orch_list:
        _LOG.debug("SelectOrchestrator failed: empty orchestrator list")
        raise NoOrchestratorAvailableError("No orchestrators available to select")

    candidates = orch_list[:5]

    _LOG.debug("SelectOrchestrator trying candidates: %s", candidates)
    with ThreadPoolExecutor(max_workers=len(candidates)) as executor:
        futures = {
            executor.submit(
                get_orch_info,
                url,
                signer_url=signer_url,
                capabilities=capabilities,
            ): url
            for url in candidates
        }

        for future in as_completed(futures):
            url = futures[future]
            try:
                info = future.result()
            except LivepeerGatewayError as e:
                _LOG.debug("SelectOrchestrator candidate failed: %s (%s)", url, e)
                continue
            _LOG.debug("SelectOrchestrator selected: %s", url)
            return url, info

    _LOG.debug("SelectOrchestrator failed: all candidates errored")
    raise NoOrchestratorAvailableError("All orchestrators failed")


