"""
Self-contained integration test for the consumer.

Spins up a local HTTP server that serves fake data,
then verifies the REST poll source correctly reads from it.
No external APIs or MinIO needed.
"""

import asyncio
import json
import sys
from datetime import datetime, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading

sys.path.insert(0, "../src")

from sources import create_source


# ---- Fake data server ----

class FakeDataHandler(BaseHTTPRequestHandler):
    """Serves fake earthquake-like JSON on every request."""

    request_count = 0

    def do_GET(self):
        FakeDataHandler.request_count += 1
        payload = {
            "type": "FeatureCollection",
            "metadata": {"generated": datetime.now(timezone.utc).isoformat()},
            "features": [
                {
                    "type": "Feature",
                    "properties": {
                        "mag": 2.5 + FakeDataHandler.request_count * 0.1,
                        "place": f"Test Location {FakeDataHandler.request_count}",
                        "time": int(datetime.now(timezone.utc).timestamp() * 1000),
                    },
                    "geometry": {
                        "type": "Point",
                        "coordinates": [-122.0, 37.0, 10.0],
                    },
                }
            ],
        }
        body = json.dumps(payload).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass  # silence request logs


async def test_rest_poll():
    """Verify the REST poll source reads from a local server."""
    server = HTTPServer(("127.0.0.1", 18765), FakeDataHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    source = create_source(
        source_type="rest_poll",
        url="http://127.0.0.1:18765/data",
        poll_interval=1,
    )

    messages = []
    async for msg in source.consume():
        data = json.loads(msg)
        feat = data["features"][0]
        messages.append(feat)
        print(
            f"  Poll {len(messages)}: "
            f"mag={feat['properties']['mag']:.1f} "
            f"place={feat['properties']['place']}"
        )
        if len(messages) >= 3:
            break

    await source.disconnect()
    server.shutdown()

    # Verify
    assert len(messages) == 3, f"Expected 3 messages, got {len(messages)}"
    assert messages[0]["properties"]["place"] == "Test Location 1"
    assert messages[1]["properties"]["place"] == "Test Location 2"
    assert messages[2]["properties"]["place"] == "Test Location 3"
    print("  REST poll: PASSED")


async def test_source_factory():
    """Verify the factory creates the right adapter for each type."""
    from sources import WebSocketSource, RestPollSource, SSESource

    ws = create_source(source_type="websocket", url="wss://x.com")
    rest = create_source(source_type="rest_poll", url="https://x.com")
    sse = create_source(source_type="sse", url="https://x.com/stream")

    assert isinstance(ws, WebSocketSource)
    assert isinstance(rest, RestPollSource)
    assert isinstance(sse, SSESource)

    try:
        create_source(source_type="invalid", url="https://x.com")
        assert False, "Should have raised ValueError"
    except ValueError:
        pass

    print("  Factory: PASSED")


async def test_sink_envelope():
    """Verify the MinIO sink wraps messages in the correct envelope format."""
    from sinks.minio_sink import _safe_parse

    # Valid JSON gets parsed
    result = _safe_parse('{"price": 100}')
    assert isinstance(result, dict)
    assert result["price"] == 100

    # Invalid JSON stored as string
    result = _safe_parse("not json")
    assert result == "not json"

    print("  Envelope: PASSED")


async def main():
    print("Running consumer integration tests...\n")

    print("1. Source factory:")
    await test_source_factory()

    print("2. Sink envelope:")
    await test_sink_envelope()

    print("3. REST poll (live, local server):")
    await test_rest_poll()

    print("\nAll tests passed.")


if __name__ == "__main__":
    asyncio.run(main())
