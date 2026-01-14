from __future__ import annotations

import json
from typing import Any

from .trickle_publisher import TricklePublisher


class Control:
    def __init__(self, control_url: str, mime_type: str = "application/json") -> None:
        self.control_url = control_url
        self._publisher = TricklePublisher(control_url, mime_type)

    async def write_control(self, msg: dict[str, Any]) -> None:
        """
        Publish an unstructured JSON message onto the trickle control channel.

        One `write_control()` call sends one message per trickle segment.
        """
        if not isinstance(msg, dict):
            raise TypeError(f"write_control expects dict, got {type(msg).__name__}")

        payload = json.dumps(msg).encode("utf-8")
        async with await self._publisher.next() as segment:
            await segment.write(payload)

    async def close_control(self) -> None:
        """
        Close the control-channel publisher (best-effort).
        """
        await self._publisher.close()

