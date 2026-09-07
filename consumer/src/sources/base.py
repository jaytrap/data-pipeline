"""
Base class for source adapters.

Every source adapter must implement two things:
  1. connect() - establish connection to the data source
  2. consume() - async generator that yields raw messages

The consumer loop doesn't care how data arrives. It just
iterates over whatever consume() yields.
"""

from abc import ABC, abstractmethod
from typing import AsyncIterator


class BaseSource(ABC):

    @abstractmethod
    async def connect(self) -> None:
        """Establish connection to the data source."""

    @abstractmethod
    async def consume(self) -> AsyncIterator[str]:
        """Yield raw message strings from the source."""

    async def disconnect(self) -> None:
        """Clean up connection. Override if needed."""
