from __future__ import annotations

import base64
import json
import logging
import re
import ssl
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from . import lp_rpc_pb2
from .errors import LivepeerGatewayError, PaymentError, SignerRefreshRequired, SkipPaymentCycle
_LOG = logging.getLogger(__name__)

@dataclass(frozen=True)
class GetPaymentResponse:
    payment: str
    seg_creds: Optional[str] = None


@dataclass(frozen=True)
class SignerMaterial:
    """
    Material returned by the remote signer.
    address: 20-byte broadcaster ETH address
    sig: signature bytes (length depends on scheme; commonly 65 bytes for ECDSA)
    """
    address: bytes
    sig: bytes


@dataclass
class RemoteSignerError(LivepeerGatewayError):
    signer_url: str
    message: str
    cause: Optional[BaseException] = None

    def __str__(self) -> str:
        return f"Remote signer error: {self.message} (url={self.signer_url})"


_HEX_RE = re.compile(r"^(0x)?[0-9a-fA-F]*$")

def _hex_to_bytes(s: str, *, expected_len: Optional[int] = None) -> bytes:
    s = s.strip()
    if not _HEX_RE.match(s):
        raise ValueError(f"Not a hex string: {s!r}")
    if s.startswith(("0x", "0X")):
        s = s[2:]
    if len(s) % 2 == 1:
        # allow odd-length hex (pad left)
        s = "0" + s
    b = bytes.fromhex(s)
    if expected_len is not None and len(b) != expected_len:
        raise ValueError(f"Expected {expected_len} bytes, got {len(b)} bytes")
    return b


@lru_cache(maxsize=None)
def get_orch_info_sig(signer_url: str) -> SignerMaterial:
    """
    Fetch signer material exactly once per signer_url for the lifetime of the process.
    Subsequent calls return cached data.
    """
    from .orchestrator import _extract_error_message, _http_origin, post_json

    # check for offchain mode
    if not signer_url:
        return SignerMaterial(address=None, sig=None)

    # Accept either a base URL or a full URL that includes /sign-orchestrator-info.
    # Normalize to an https:// origin and append the expected path.
    signer_url = f"{_http_origin(signer_url)}/sign-orchestrator-info"

    try:
        # Some signers accept/expect POST with an empty JSON object.
        data = post_json(signer_url, {}, timeout=5.0)

        # Expected response shape (example):
        # {
        #   "address": "0x0123...abcd",   # 20-byte ETH address hex
        #   "signature": "0x..."          # signature hex
        # }
        if "address" not in data or "signature" not in data:
            raise RemoteSignerError(
                signer_url,
                f"Remote signer JSON must contain 'address' and 'signature': {data!r}",
                cause=None,
            ) from None

        address = _hex_to_bytes(str(data["address"]), expected_len=20)
        sig = _hex_to_bytes(str(data["signature"]))  # signature length may vary

    except LivepeerGatewayError as e:
        # post_json wraps the underlying exception as __cause__; convert back into
        # a signer-specific error message.
        cause = e.__cause__ or e

        if isinstance(cause, HTTPError):
            body = _extract_error_message(cause)
            body_part = f"; body={body!r}" if body else ""
            raise RemoteSignerError(
                signer_url,
                f"HTTP {cause.code} from signer{body_part}",
                cause=cause,
            ) from None

        if isinstance(cause, ConnectionRefusedError):
            raise RemoteSignerError(
                signer_url,
                "connection refused (is the signer running? is the host/port correct?)",
                cause=cause,
            ) from None

        if isinstance(cause, URLError):
            raise RemoteSignerError(
                signer_url,
                f"failed to reach signer: {getattr(cause, 'reason', cause)}",
                cause=cause,
            ) from None

        if isinstance(cause, json.JSONDecodeError):
            raise RemoteSignerError(
                signer_url,
                f"signer did not return valid JSON: {cause}",
                cause=cause,
            ) from None

        raise RemoteSignerError(
            signer_url,
            f"unexpected error: {cause.__class__.__name__}: {cause}",
            cause=cause if isinstance(cause, BaseException) else e,
        ) from None

    return SignerMaterial(address=address, sig=sig)


class PaymentSession:
    def __init__(
        self,
        signer_url: Optional[str],
        info: lp_rpc_pb2.OrchestratorInfo,
        *,
        type: str,
        capabilities: Optional[lp_rpc_pb2.Capabilities] = None,
        max_refresh_retries: int = 3,
    ) -> None:
        self._signer_url = signer_url
        self._info = info
        self._type = type
        self._manifest_id: Optional[str] = None
        self._capabilities = capabilities
        self._max_refresh_retries = max(0, int(max_refresh_retries))
        self._state: Optional[dict[str, str]] = None

    def set_manifest_id(self, manifest_id: str) -> None:
        if not isinstance(manifest_id, str) or not manifest_id.strip():
            raise PaymentError("manifest_id must be a non-empty string")
        self._manifest_id = manifest_id.strip()

    def get_payment(self) -> GetPaymentResponse:
        """
        Generate a payment via the remote signer.

        Handles signer state round-tripping internally.
        On HTTP 480, refreshes OrchestratorInfo and retries
        (up to max_refresh_retries).
        Returns payment + seg_creds for use as HTTP headers.
        """

        # Offchain mode: still send the expected headers, but with empty content.
        if not self._signer_url:
            seg = lp_rpc_pb2.SegData()
            if not self._info.HasField("auth_token"):
                raise PaymentError(
                    "Orchestrator did not provide an auth token."
                )
            seg.auth_token.CopyFrom(self._info.auth_token)
            seg = base64.b64encode(seg.SerializeToString()).decode("ascii")
            return GetPaymentResponse(seg_creds=seg, payment="")

        def _payment_request() -> GetPaymentResponse:
            from .orchestrator import _http_origin, post_json

            base = _http_origin(self._signer_url)
            url = f"{base}/generate-live-payment"

            pb = self._info.SerializeToString()
            orch_b64 = base64.b64encode(pb).decode("ascii")
            payload: dict[str, Any] = {
                "orchestrator": orch_b64,
                "type": self._type,
            }
            if self._manifest_id is not None:
                payload["ManifestID"] = self._manifest_id
            if self._state is not None:
                payload["state"] = self._state

            data = post_json(url, payload)
            payment = data.get("payment")
            if not isinstance(payment, str) or not payment:
                raise PaymentError(
                    f"GetPayment error: missing/invalid 'payment' in response (url={url})"
                )

            seg_creds = data.get("segCreds")
            if seg_creds is not None and not isinstance(seg_creds, str):
                raise PaymentError(
                    f"GetPayment error: invalid 'segCreds' in response (url={url})"
                )

            state = data.get("state")
            if not isinstance(state, dict):
                raise PaymentError(
                    f"Remote signer response missing 'state' object (url={url})"
                )

            self._state = state
            return GetPaymentResponse(payment=payment, seg_creds=seg_creds)

        attempts = 0
        while True:
            try:
                return _payment_request()
            except SignerRefreshRequired as e:
                if attempts >= self._max_refresh_retries:
                    raise PaymentError(
                        f"Signer refresh required after {attempts} retries: {e}"
                    ) from e
                if not self._info.transcoder:
                    raise PaymentError(
                        "OrchestratorInfo missing transcoder URL for refresh"
                    )
                from .orch_info import get_orch_info

                self._info = get_orch_info(
                    self._info.transcoder,
                    signer_url=self._signer_url,
                    capabilities=self._capabilities,
                )
                attempts += 1

    def send_payment(self) -> None:
        """
        Generate a payment (via get_payment) and forward it
        to the orchestrator via POST {orch}/payment.
        """
        from .orchestrator import _extract_error_message, _http_origin

        p = self.get_payment()
        if not self._info.transcoder:
            raise PaymentError("OrchestratorInfo missing transcoder URL for payment")
        base = _http_origin(self._info.transcoder)
        url = f"{base}/payment"
        headers = {
            "Livepeer-Payment": p.payment,
            "Livepeer-Segment": p.seg_creds or "",
        }
        req = Request(url, data=b"", headers=headers, method="POST")
        ssl_ctx = ssl._create_unverified_context()
        try:
            with urlopen(req, timeout=5.0, context=ssl_ctx) as resp:
                resp.read()
        except HTTPError as e:
            body = _extract_error_message(e)
            body_part = f"; body={body!r}" if body else ""
            raise PaymentError(
                f"HTTP payment error: HTTP {e.code} from endpoint (url={url}){body_part}"
            ) from e
        except ConnectionRefusedError as e:
            raise PaymentError(
                f"HTTP payment error: connection refused (is the server running? is the host/port correct?) (url={url})"
            ) from e
        except URLError as e:
            raise PaymentError(
                f"HTTP payment error: failed to reach endpoint: {getattr(e, 'reason', e)} (url={url})"
            ) from e
        except Exception as e:
            raise PaymentError(
                f"HTTP payment error: unexpected error: {e.__class__.__name__}: {e} (url={url})"
            ) from e
