"""
REST polling source adapter.

Calls an HTTP endpoint on a fixed interval and yields the response body.
This is the fallback for data sources that don't support push (webhooks/WS).

Handles HTTP errors gracefully with exponential backoff.
"""

import asyncio
import logging

import aiohttp

from .base import BaseSource

logger = logging.getLogger(__name__)


class RestPollSource(BaseSource):

    def __init__(
        self,
        url: str,
        poll_interval: int = 60,
        headers: dict = None,
        **kwargs,
    ):
        self.url = url
        self.poll_interval = poll_interval
        self.headers = headers or {}
        self._session = None
        self._backoff = 1

    async def connect(self) -> None:
        """Create the HTTP session."""
        logger.info(
            f"REST poller configured: {self.url} "
            f"(every {self.poll_interval}s)"
        )
        self._session = aiohttp.ClientSession(
            headers=self.headers,
            timeout=aiohttp.ClientTimeout(total=30),
        )
        self._backoff = 1

    async def consume(self):
        """Poll the endpoint and yield response bodies."""
        if self._session is None:
            await self.connect()

        while True:
            try:
                async with self._session.get(self.url) as resp:
                    if resp.status == 200:
                        body = await resp.text()
                        yield body
                        self._backoff = 1
                    elif resp.status == 429:
                        retry_after = int(
                            resp.headers.get("Retry-After", self._backoff * 2)
                        )
                        logger.warning(
                            f"Rate limited (429). Waiting {retry_after}s"
                        )
                        await asyncio.sleep(retry_after)
                        continue
                    else:
                        logger.warning(
                            f"HTTP {resp.status} from {self.url}. "
                            f"Retrying in {self._backoff}s"
                        )
                        await asyncio.sleep(self._backoff)
                        self._backoff = min(self._backoff * 2, 300)
                        continue

            except aiohttp.ClientError as e:
                logger.error(
                    f"HTTP request failed: {e}. Retrying in {self._backoff}s"
                )
                await asyncio.sleep(self._backoff)
                self._backoff = min(self._backoff * 2, 300)
                continue

            await asyncio.sleep(self.poll_interval)

    async def disconnect(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
            logger.info("REST poller session closed")
