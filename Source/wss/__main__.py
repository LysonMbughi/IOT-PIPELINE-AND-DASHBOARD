"""Entry point for the WebSocket live-feed server.

Run with:
    python -m wss

Starts a WebSocket server at ws://127.0.0.1:8000/live that accepts
client connections and pushes live sensor readings received from
the telemetry server's reading stream (port 9001).

This runs as a separate process from the main server.
"""
from __future__ import annotations

import asyncio
import logging
import websockets
from wss.broadcaster import Broadcaster
from wss import handler
from wss.reading_client import ReadingStreamClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Configuration
WS_HOST = "127.0.0.1"
WS_PORT = 8000

READING_STREAM_HOST = "127.0.0.1"
READING_STREAM_PORT = 9001


async def main() -> None:
    """Boot the WebSocket server.

    Responsibilities:
      - Create a broadcaster for WebSocket clients.
      - Start WebSocket server to accept client connections.
      - Connect to the server's reading stream (port 9001).
      - Forward readings from the stream to WebSocket clients.
    """
    logger.info("=" * 60)
    logger.info("WebSocket Live Feed Server Starting")
    logger.info("=" * 60)
    
    # Create the broadcaster for WebSocket clients
    broadcaster = Broadcaster()
    logger.info("Broadcaster initialized")
    
    # Set the broadcaster in the handler module
    handler.set_broadcaster(broadcaster)
    logger.info("Handler configured with broadcaster")
    
    # Define callback for when readings arrive from the server's reading stream
    async def on_reading(reading):
        """Called when a reading arrives from the reading stream."""
        await broadcaster.publish_reading(reading)
    
    # Create the reading stream client
    reading_client = ReadingStreamClient(
        READING_STREAM_HOST,
        READING_STREAM_PORT,
        on_reading
    )
    
    # Start the WebSocket server
    logger.info(f"Starting WebSocket server on ws://{WS_HOST}:{WS_PORT}/live")
    
    async with websockets.serve(
        handler.live,
        WS_HOST,
        WS_PORT,
        max_size=10 * 1024 * 1024,  # 10 MB
        max_queue=32,
    ):
        logger.info(f"WebSocket server listening at ws://{WS_HOST}:{WS_PORT}/live")
        logger.info("=" * 60)
        logger.info("Server ready. Press Ctrl+C to shut down.")
        logger.info("=" * 60)
        
        # Start the reading stream client in the background
        reading_task = asyncio.create_task(reading_client.run())
        
        try:
            # Wait forever (or until Ctrl+C)
            await asyncio.Future()
        except KeyboardInterrupt:
            logger.info("Shutdown signal received")
        finally:
            logger.info("Server shutting down...")
            reading_task.cancel()
            try:
                await reading_task
            except asyncio.CancelledError:
                pass
            logger.info("Server shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
