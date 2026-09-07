from .base import BaseSource
from .websocket_source import WebSocketSource
from .rest_poll_source import RestPollSource
from .sse_source import SSESource


def create_source(source_type: str, **kwargs) -> BaseSource:
    """Factory: return the right source adapter based on config."""
    adapters = {
        "websocket": WebSocketSource,
        "rest_poll": RestPollSource,
        "sse": SSESource,
    }
    if source_type not in adapters:
        raise ValueError(
            f"Unknown source type '{source_type}'. "
            f"Valid types: {list(adapters.keys())}"
        )
    return adapters[source_type](**kwargs)
