"""Client for subscribing to readings from the server's reading stream.

Connects to the server's reading stream (TCP port 9001) and receives
readings published by sensors. Broadcasts them to WebSocket clients.
"""
from __future__ import annotations

import asyncio
import json
import logging
import struct
from typing import Optional, Callable

logger = logging.getLogger(__name__)


class ReadingStreamClient:
    """Connects to the server's reading stream and forwards readings to a callback."""

    def __init__(self, host: str, port: int, on_reading: Callable):
        """Initialize the reading stream client.
        
        Args:
            host: Server host (e.g. "127.0.0.1")
            port: Server port (e.g. 9001)
            on_reading: Async callback function to call when a reading arrives
        """
        self.host = host
        self.port = port
        self.on_reading = on_reading
        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None

    async def run(self) -> None:
        """Connect to the reading stream and receive readings forever.
        
        Automatically reconnects on failure with exponential backoff.
        """
        backoff_seconds = 1
        max_backoff = 60
        
        while True:
            try:
                logger.info(f"Connecting to reading stream at {self.host}:{self.port}")
                self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
                logger.info("Connected to reading stream")
                
                # Reset backoff on successful connection
                backoff_seconds = 1
                
                # Read readings from the stream
                while True:
                    # Read 4-byte big-endian length prefix
                    length_bytes = await self.reader.readexactly(4)
                    if not length_bytes:
                        break  # Connection closed
                    
                    message_length = struct.unpack("!I", length_bytes)[0]
                    
                    # Sanity check
                    if message_length > 10000:
                        logger.warning(f"Message too large: {message_length} bytes")
                        break
                    
                    # Read the JSON payload
                    payload = await self.reader.readexactly(message_length)
                    
                    try:
                        reading = json.loads(payload.decode("utf-8"))
                        logger.debug(f"Received reading: {reading}")
                        
                        # Call the callback with the reading
                        await self.on_reading(reading)
                    
                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to decode reading JSON: {e}")
                        continue
            
            except asyncio.CancelledError:
                logger.info("Reading stream client cancelled")
                break
            except ConnectionRefusedError:
                logger.warning(f"Connection refused, retrying in {backoff_seconds}s")
            except ConnectionResetError:
                logger.warning(f"Connection reset, retrying in {backoff_seconds}s")
            except asyncio.IncompleteReadError:
                logger.info("Connection closed by server")
            except Exception as e:
                logger.error(f"Error: {e}, retrying in {backoff_seconds}s")
            finally:
                try:
                    if self.writer:
                        self.writer.close()
                        await self.writer.wait_closed()
                except:
                    pass
            
            # Exponential backoff
            await asyncio.sleep(backoff_seconds)
            backoff_seconds = min(backoff_seconds * 2, max_backoff)
