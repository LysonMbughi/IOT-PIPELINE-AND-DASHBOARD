"""Inter-process communication layer for broadcasting readings.

The TCP ingest server publishes readings here, and the wss module
subscribes to receive them. Uses a simple TCP protocol where readings
are sent as JSON-encoded messages with a length prefix.

This enables the server and wss modules to run in separate processes.
"""
from __future__ import annotations

import asyncio
import json
import logging
import struct
from typing import Optional

logger = logging.getLogger(__name__)


class ReadingStream:
    """TCP-based pub/sub for readings between server and wss modules.
    
    Accepts connections from wss clients and broadcasts readings to them
    using a simple length-prefixed JSON protocol.
    """

    def __init__(self) -> None:
        """Initialize the reading stream."""
        self.clients: set = set()  # Connected wss subscribers
        self.lock = asyncio.Lock()

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Handle one connected wss client subscription.
        
        Args:
            reader: Stream reader for incoming data
            writer: Stream writer for outgoing readings
        """
        peer = writer.get_extra_info("peername")
        logger.info(f"ReadingStream: client connected from {peer}")
        
        async with self.lock:
            self.clients.add(writer)
        
        try:
            # Keep the connection open; readings come from publish()
            # The client doesn't send anything, just receives
            await asyncio.Event().wait()  # Wait forever (until disconnect)
        except Exception as e:
            logger.error(f"Error with client {peer}: {e}")
        finally:
            async with self.lock:
                self.clients.discard(writer)
            writer.close()
            await writer.wait_closed()
            logger.info(f"ReadingStream: client disconnected from {peer}")

    async def publish(self, reading) -> None:
        """Broadcast a reading to all connected clients.
        
        Args:
            reading: server.storage.Reading object
        """
        # Convert to dict for JSON serialization
        message = {
            "sensor_id": reading.sensor_id,
            "type": reading.sensor_type,
            "value": reading.value,
            "ts": reading.timestamp,
        }
        
        # Serialize as JSON with length prefix
        payload = json.dumps(message).encode("utf-8")
        length = struct.pack("!I", len(payload))
        frame = length + payload
        
        # Send to all connected clients
        async with self.lock:
            disconnected = []
            for client in self.clients:
                try:
                    client.write(frame)
                    await client.drain()
                except Exception as e:
                    logger.warning(f"Failed to send to client: {e}")
                    disconnected.append(client)
            
            # Remove disconnected clients
            for client in disconnected:
                self.clients.discard(client)

    async def start_server(self, host: str, port: int) -> asyncio.Server:
        """Start the reading stream TCP server.
        
        Args:
            host: Bind address (e.g. "127.0.0.1")
            port: TCP port (e.g. 9001)
        
        Returns:
            asyncio.Server instance
        """
        server = await asyncio.start_server(
            self.handle_client,
            host,
            port
        )
        
        logger.info(f"ReadingStream server listening on {host}:{port}")
        return server
