"""
WebSocket source adapter.

Connects to a WebSocket URL, optionally sends a subscribe message,
and yields every incoming message as a raw string.

Handles reconnection automatically on disconnect.
"""

import asyncio
import logging

import websockets
from websockets.exceptions import ConnectionClosed

from .base import BaseSource

logger = logging.getLogger(__name__)


class WebSocketSource(BaseSource):

    def __init__(self, url: str, subscribe_message: str = "", **kwargs):
        self.url = url
        self.subscribe_message = subscribe_message
        self._ws = None
        self._reconnect_delay = 1  # seconds, doubles on each failure
        self._max_reconnect_delay = 60

    async def connect(self) -> None:
        """Establish WebSocket connection."""
        logger.info(f"Connecting to WebSocket: {self.url}")
        self._ws = await websockets.connect(
            self.url,
            ping_interval=20,
            ping_timeout=10,
            close_timeout=5,
        )
        logger.info("WebSocket connected")

        if self.subscribe_message:
            logger.info(f"Sending subscribe message: {self.subscribe_message}")
            await self._ws.send(self.subscribe_message)

        # Reset delay on successful connection
        self._reconnect_delay = 1

    async def consume(self):
        """Yield raw messages from WebSocket with auto-reconnect."""
        while True:
            try:
                if self._ws is None or self._ws.closed:
                    await self.connect()

                async for message in self._ws:
                    yield message

            except ConnectionClosed as e:
                logger.warning(
                    f"WebSocket connection closed: code={e.code} "
                    f"reason={e.reason}. Reconnecting in "
                    f"{self._reconnect_delay}s..."
                )
            except Exception as e:
                logger.error(
                    f"WebSocket error: {e}. Reconnecting in "
                    f"{self._reconnect_delay}s..."
                )

            await asyncio.sleep(self._reconnect_delay)
            self._reconnect_delay = min(
                self._reconnect_delay * 2,
                self._max_reconnect_delay,
            )

    async def disconnect(self) -> None:
        if self._ws and not self._ws.closed:
            await self._ws.close()
            logger.info("WebSocket disconnected")
