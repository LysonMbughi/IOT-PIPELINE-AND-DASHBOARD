"""Asynchronous TCP listener for sensor connections.

Sensors connect over TCP and stream Protobuf-encoded readings. This module:
  - Accepts connections concurrently with asyncio.start_server.
  - Frames and decodes each Protobuf message from the byte stream.
  - Hands decoded readings to the storage layer (and optionally to a
    broadcaster so the WebSocket /live feed can push them).
  - Tolerates disconnects and malformed messages without crashing the server.

Framing convention: 4-byte big-endian length prefix followed by the Protobuf
payload of that length. This allows the server to know exactly how many bytes
to read for each message.
"""
from __future__ import annotations

import asyncio
import logging
import struct
from typing import Optional, Any

# NOTE: Protobuf stubs must be generated first:
#   cd Source && protoc --python_out=. proto/telemetry.proto
try:
    import proto.telemetry_pb2 as telemetry_pb2
except ImportError as e:
    logging.warning(f"Could not import Protobuf stubs: {e}. Run protoc first.")
    telemetry_pb2 = None

logger = logging.getLogger(__name__)


async def handle_sensor(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    storage: Any,
    reading_stream: Optional[Any] = None,
) -> None:
    """Handle one sensor connection until it closes.
    
    Args:
        reader: asyncio stream reader for incoming data
        writer: asyncio stream writer for outgoing data
        storage: Storage instance for persisting readings
        reading_stream: Optional reading stream for publishing to wss module
    """
    peer = writer.get_extra_info("peername")
    logger.info(f"Sensor connected from {peer}")
    
    try:
        while True:
            # Read 4-byte big-endian length prefix
            length_bytes = await reader.readexactly(4)
            if not length_bytes:
                break  # Connection closed
            
            message_length = struct.unpack("!I", length_bytes)[0]
            
            # Sanity check: messages should be < 10 KB
            if message_length > 10000:
                logger.warning(f"Message too large: {message_length} bytes from {peer}")
                break
            
            # Read the Protobuf payload
            payload = await reader.readexactly(message_length)
            
            # Decode the Protobuf message
            try:
                if not telemetry_pb2:
                    logger.error("Protobuf stubs not available. Run protoc first.")
                    break
                
                # Strategy for message type discrimination:
                # 1. Parse as Reading (always succeeds for valid Protobuf)
                # 2. Check if sensor is already registered
                #    - If YES: treat as Reading (post-registration data)
                #    - If NO: check if location is set; if yes, it's a Registration
                
                reading = telemetry_pb2.Reading()
                reading.ParseFromString(payload)
                
                # Validate sensor_id exists in both message types
                if not reading.sensor_id:
                    logger.warning(f"Malformed message from {peer}: missing sensor_id")
                    continue
                
                # Check if sensor is already registered
                is_registered = await storage.sensor_exists(reading.sensor_id)
                
                if is_registered:
                    # Sensor is registered → this is a Reading, not a Registration
                    # Validate required fields for reading
                    if reading.timestamp == 0:
                        logger.warning(f"Malformed reading from {peer}: missing timestamp")
                        continue
                    
                    logger.debug(f"Received reading: {reading.sensor_id}, value={reading.value}, ts={reading.timestamp}")
                    
                    # Persist to storage
                    from server.storage import Reading as StorageReading
                    stored_reading = StorageReading(
                        sensor_id=reading.sensor_id,
                        sensor_type=telemetry_pb2.SensorType.Name(reading.sensor_type),
                        value=reading.value,
                        timestamp=reading.timestamp,
                    )
                    await storage.add_reading(stored_reading)
                    
                    # Publish to reading stream for wss module to receive
                    if reading_stream:
                        await reading_stream.publish(stored_reading)
                else:
                    # Sensor not registered yet → try to parse as Registration
                    registration = telemetry_pb2.SensorRegistration()
                    registration.ParseFromString(payload)
                    
                    # Validate registration has required fields
                    if registration.sensor_type == telemetry_pb2.UNKNOWN:
                        logger.warning(f"Malformed registration from {peer}: missing/invalid sensor_type")
                        continue
                    
                    logger.info(f"Sensor registered: {registration.sensor_id} ({telemetry_pb2.SensorType.Name(registration.sensor_type)})")
                    
                    # Add to storage
                    from server.storage import Sensor
                    sensor = Sensor(
                        id=registration.sensor_id,
                        type=telemetry_pb2.SensorType.Name(registration.sensor_type),
                        location=registration.location
                    )
                    await storage.add_sensor(sensor)
            
            except Exception as e:
                logger.error(f"Error processing message from {peer}: {e}")
                # Don't close connection, just skip this message
                continue
    
    except asyncio.IncompleteReadError:
        logger.info(f"Sensor {peer} disconnected (incomplete read)")
    except Exception as e:
        logger.error(f"Error handling sensor {peer}: {e}")
    finally:
        writer.close()
        await writer.wait_closed()
        logger.info(f"Sensor {peer} connection closed")


async def start_tcp_server(
    host: str,
    port: int,
    storage: Any,
    reading_stream: Optional[Any] = None,
) -> asyncio.AbstractServer:
    """Start the TCP ingest server listening on (host, port).
    
    Args:
        host: Bind address (e.g. "0.0.0.0" or "127.0.0.1")
        port: TCP port (e.g. 9000)
        storage: Storage instance
        reading_stream: Optional reading stream for publishing readings
    
    Returns:
        The asyncio server object (call server.close() to shut down)
    """
    # Create a handler that captures storage and reading_stream
    async def handler(reader, writer):
        await handle_sensor(reader, writer, storage, reading_stream)
    
    server = await asyncio.start_server(handler, host, port)
    addr = server.sockets[0].getsockname()
    logger.info(f"TCP ingest server listening on {addr[0]}:{addr[1]}")
    return server
