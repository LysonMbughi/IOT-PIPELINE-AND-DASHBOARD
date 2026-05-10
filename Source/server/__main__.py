"""Entry point for the telemetry server.

Run with:
    python -m server

Boots both:
  1. TCP ingest server (9000) — receives Protobuf from sensors
  2. REST API server (8080) — HTTP queries with content negotiation
  
Both share the Storage layer (SQLite).
"""
from __future__ import annotations

import asyncio
import logging
import websockets
from server.storage import Storage
from server.tcp_ingest import start_tcp_server
from server.rest_api import build_app
from server.reading_stream import ReadingStream

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Configuration (can be made configurable later)
TCP_INGEST_HOST = "127.0.0.1"
TCP_INGEST_PORT = 9000

REST_API_HOST = "127.0.0.1"
REST_API_PORT = 8080

READING_STREAM_HOST = "127.0.0.1"
READING_STREAM_PORT = 9001

DB_PATH = "telemetry.db"


async def main() -> None:
    """Boot the telemetry server.

    Responsibilities:
      - Initialise the storage layer (SQLite).
      - Create the reading stream for wss module to subscribe to.
      - Start the TCP ingest listener for sensor connections.
      - Start the aiohttp app hosting the REST API.
      - Wait until shutdown (Ctrl+C).
    """
    logger.info("=" * 60)
    logger.info("Telemetry Server Starting")
    logger.info("=" * 60)
    
    # 1. Initialize storage (SQLite)
    logger.info(f"Initializing storage: {DB_PATH}")
    storage = Storage(db_path=DB_PATH)
    logger.info("Storage initialized")
    
    # 2. Create reading stream for inter-process communication with wss module
    reading_stream = ReadingStream()
    logger.info("Reading stream initialized")
    
    # 3. Start reading stream server (for wss module to subscribe)
    logger.info(f"Starting reading stream server on {READING_STREAM_HOST}:{READING_STREAM_PORT}")
    reading_stream_server = await reading_stream.start_server(
        READING_STREAM_HOST,
        READING_STREAM_PORT
    )
    logger.info(f"Reading stream listening at {READING_STREAM_HOST}:{READING_STREAM_PORT}")
    
    # 4. Start TCP ingest server
    logger.info(f"Starting TCP ingest server on {TCP_INGEST_HOST}:{TCP_INGEST_PORT}")
    tcp_server = await start_tcp_server(
        TCP_INGEST_HOST,
        TCP_INGEST_PORT,
        storage,
        reading_stream
    )

    # 5. Build and start REST API server
    logger.info(f"Starting REST API server on {REST_API_HOST}:{REST_API_PORT}")
    rest_app = build_app(storage)
    
    runner = __import__("aiohttp").web.AppRunner(rest_app)
    await runner.setup()
    site = __import__("aiohttp").web.TCPSite(runner, REST_API_HOST, REST_API_PORT)
    await site.start()
    logger.info(f"REST API running at http://{REST_API_HOST}:{REST_API_PORT}")
    
    # 6. Run until shutdown
    logger.info("=" * 60)
    logger.info("Server ready. Press Ctrl+C to shut down.")
    logger.info("=" * 60)
    
    try:
        # Just wait forever (TCP server runs in background)
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        logger.info("Shutdown signal received")
    finally:
        logger.info("Shutting down...")
        
        # Close TCP server
        tcp_server.close()
        await tcp_server.wait_closed()
        logger.info("TCP server closed")
        
        # Close reading stream server
        reading_stream_server.close()
        await reading_stream_server.wait_closed()
        logger.info("Reading stream server closed")
        
        # Close REST API
        await runner.cleanup()
        logger.info("REST API closed")
        
        logger.info("Server shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
