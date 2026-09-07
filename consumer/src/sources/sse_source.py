"""
SSE (Server-Sent Events) source adapter.

Connects to an SSE endpoint and yields each event's data field.
Used by sources like Wikimedia EventStreams and Mastodon.

SSE is HTTP-based streaming: the server keeps the connection open
and pushes lines prefixed with "data:" as events arrive.
"""

import asyncio
import logging

import aiohttp

from .base import BaseSource

logger = logging.getLogger(__name__)


class SSESource(BaseSource):

    def __init__(self, url: str, headers: dict = None, **kwargs):
        self.url = url
        self.headers = headers or {}
        self._session = None
        self._reconnect_delay = 1
        self._max_reconnect_delay = 60
        self._last_event_id = None

    async def connect(self) -> None:
        """Create the HTTP session for SSE."""
        logger.info(f"SSE connecting to: {self.url}")
        self._session = aiohttp.ClientSession(
            headers={
                "Accept": "text/event-stream",
                **self.headers,
            },
            timeout=aiohttp.ClientTimeout(
                total=None,  # SSE connections are long-lived
                sock_read=90,  # but detect dead connections
            ),
        )
        self._reconnect_delay = 1

    async def consume(self):
        """Yield data payloads from the SSE stream with auto-reconnect."""
        if self._session is None:
            await self.connect()

        while True:
            try:
                request_headers = {}
                if self._last_event_id:
                    request_headers["Last-Event-ID"] = self._last_event_id

                async with self._session.get(
                    self.url, headers=request_headers
                ) as resp:
                    if resp.status != 200:
                        logger.warning(
                            f"SSE HTTP {resp.status}. "
                            f"Retrying in {self._reconnect_delay}s"
                        )
                        await asyncio.sleep(self._reconnect_delay)
                        self._reconnect_delay = min(
                            self._reconnect_delay * 2,
                            self._max_reconnect_delay,
                        )
                        continue

                    logger.info("SSE stream connected")
                    self._reconnect_delay = 1
                    data_buffer = []

                    async for line_bytes in resp.content:
                        line = line_bytes.decode("utf-8").rstrip("\n\r")

                        if line.startswith("data:"):
                            data_buffer.append(line[5:].strip())

                        elif line.startswith("id:"):
                            self._last_event_id = line[3:].strip()

                        elif line == "":
                            # Empty line = end of event
                            if data_buffer:
                                payload = "\n".join(data_buffer)
                                data_buffer = []
                                yield payload

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                logger.warning(
                    f"SSE connection lost: {e}. "
                    f"Reconnecting in {self._reconnect_delay}s..."
                )

            except Exception as e:
                logger.error(
                    f"SSE unexpected error: {e}. "
                    f"Reconnecting in {self._reconnect_delay}s..."
                )

            await asyncio.sleep(self._reconnect_delay)
            self._reconnect_delay = min(
                self._reconnect_delay * 2,
                self._max_reconnect_delay,
            )

    async def disconnect(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
            logger.info("SSE session closed")
