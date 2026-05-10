"""Single-sensor simulation logic.

Each simulated sensor:
  - Connects to the telemetry server over TCP.
  - Generates plausible readings on its configured interval.
  - Encodes each reading as a Protobuf message and writes a length-prefixed
    frame on the socket.
  - Reconnects with backoff after transient network failures.
"""
from __future__ import annotations

import asyncio
import logging
import struct
import time
import random
from typing import Optional

# NOTE: Protobuf stubs must be generated first:
#   cd Source && protoc --python_out=. proto/telemetry.proto
try:
    import proto.telemetry_pb2 as telemetry_pb2
except ImportError as e:
    logging.warning(f"Could not import Protobuf stubs: {e}. Run protoc first.")
    telemetry_pb2 = None

logger = logging.getLogger(__name__)


class SensorSimulator:
    """Simulates one sensor pushing readings to the telemetry server."""

    def __init__(
        self,
        sensor_id: str,
        sensor_type: str,
        interval_seconds: float,
        host: str,
        port: int,
    ) -> None:
        """Initialize a sensor simulator.
        
        Args:
            sensor_id: Unique identifier (e.g. "greenhouse-a-temp")
            sensor_type: One of "TEMPERATURE", "HUMIDITY", "SOIL_MOISTURE", "LIGHT"
            interval_seconds: Reporting cadence
            host: Server host (e.g. "127.0.0.1")
            port: Server TCP port (e.g. 9000)
        """
        self.sensor_id = sensor_id
        self.sensor_type = sensor_type
        self.interval_seconds = interval_seconds
        self.host = host
        self.port = port
        
        # Per-type state for realistic data generation
        if sensor_type == "TEMPERATURE":
            # Temperature: drift around 20°C ± 5°C
            self.state = {"base": 20.0, "drift": 0.0, "min": 15.0, "max": 30.0}
        elif sensor_type == "HUMIDITY":
            # Humidity: drift around 60% ± 20%
            self.state = {"base": 60.0, "drift": 0.0, "min": 20.0, "max": 100.0}
        elif sensor_type == "SOIL_MOISTURE":
            # Soil moisture: stable around 60% ± 10%
            self.state = {"base": 60.0, "drift": 0.0, "min": 30.0, "max": 80.0}
        else:  # LIGHT
            # Light: random 0-100%
            self.state = {"min": 0.0, "max": 100.0}

    async def run(self) -> None:
        """Connect, then push readings on the configured interval forever."""
        backoff_seconds = 1
        max_backoff = 60
        
        while True:
            try:
                logger.info(f"[{self.sensor_id}] Connecting to {self.host}:{self.port}")
                reader, writer = await asyncio.open_connection(self.host, self.port)
                logger.info(f"[{self.sensor_id}] Connected!")
                
                # Register this sensor first
                registration = self._create_registration()
                await self._send_reading(writer, registration)
                logger.info(f"[{self.sensor_id}] Registered with server")
                
                # Reset backoff on successful connection
                backoff_seconds = 1
                
                # Inner loop: generate and send readings
                while True:
                    await asyncio.sleep(self.interval_seconds)
                    
                    reading = self._generate_reading()
                    await self._send_reading(writer, reading)
            
            except asyncio.CancelledError:
                logger.info(f"[{self.sensor_id}] Task cancelled, shutting down")
                writer.close()
                await writer.wait_closed()
                break
            
            except ConnectionRefusedError:
                logger.warning(f"[{self.sensor_id}] Connection refused, retrying in {backoff_seconds}s")
            except ConnectionResetError:
                logger.warning(f"[{self.sensor_id}] Connection reset, retrying in {backoff_seconds}s")
            except Exception as e:
                logger.error(f"[{self.sensor_id}] Error: {e}, retrying in {backoff_seconds}s")
            finally:
                try:
                    writer.close()
                    await writer.wait_closed()
                except:
                    pass
            
            # Exponential backoff: 1s, 2s, 4s, 8s, ... up to 60s
            await asyncio.sleep(backoff_seconds)
            backoff_seconds = min(backoff_seconds * 2, max_backoff)

    def _generate_reading(self):
        """Produce a plausible next Reading for this sensor.
        
        Returns:
            telemetry_pb2.Reading message
        """
        if not telemetry_pb2:
            raise RuntimeError("Protobuf stubs not available")
        
        reading = telemetry_pb2.Reading()
        reading.sensor_id = self.sensor_id
        reading.timestamp = int(time.time())  # Unix epoch seconds
        
        # Map string type to Protobuf enum
        type_map = {
            "TEMPERATURE": telemetry_pb2.TEMPERATURE,
            "HUMIDITY": telemetry_pb2.HUMIDITY,
            "SOIL_MOISTURE": telemetry_pb2.SOIL_MOISTURE,
            "LIGHT": telemetry_pb2.LIGHT,
        }
        reading.sensor_type = type_map.get(self.sensor_type, telemetry_pb2.UNKNOWN)
        
        # Generate realistic value based on type
        reading.value = self._next_value()
        
        return reading

    def _create_registration(self):
        """Create a SensorRegistration message for initial handshake.
        
        Returns:
            telemetry_pb2.SensorRegistration message
        """
        if not telemetry_pb2:
            raise RuntimeError("Protobuf stubs not available")
        
        registration = telemetry_pb2.SensorRegistration()
        registration.sensor_id = self.sensor_id
        
        # Map string type to Protobuf enum
        type_map = {
            "TEMPERATURE": telemetry_pb2.TEMPERATURE,
            "HUMIDITY": telemetry_pb2.HUMIDITY,
            "SOIL_MOISTURE": telemetry_pb2.SOIL_MOISTURE,
            "LIGHT": telemetry_pb2.LIGHT,
        }
        registration.sensor_type = type_map.get(self.sensor_type, telemetry_pb2.UNKNOWN)
        registration.location = f"Sensor {self.sensor_id}"
        
        return registration

    def _next_value(self) -> float:
        """Generate the next plausible reading value."""
        state = self.state
        
        if self.sensor_type in ["TEMPERATURE", "HUMIDITY", "SOIL_MOISTURE"]:
            # Random walk (drift behavior)
            # Small step each reading (-0.5 to +0.5)
            state["drift"] += random.uniform(-0.5, 0.5)
            
            # Clamp drift to [-2, 2] so it doesn't explode
            state["drift"] = max(-2.0, min(2.0, state["drift"]))
            
            # Value = base + drift + small noise
            value = state["base"] + state["drift"] + random.uniform(-0.2, 0.2)
            
            # Clamp to valid range
            value = max(state["min"], min(state["max"], value))
        
        else:  # LIGHT
            # Light: just random 0-100
            value = random.uniform(state["min"], state["max"])
        
        return round(value, 2)

    async def _send_reading(self, writer: asyncio.StreamWriter, reading) -> None:
        """Encode and send a Protobuf reading with length prefix.
        
        Args:
            writer: asyncio stream writer
            reading: telemetry_pb2.Reading message
        """
        # Encode to binary
        payload = reading.SerializeToString()
        
        # Prepend 4-byte big-endian length
        length = struct.pack("!I", len(payload))
        frame = length + payload
        
        # Send
        writer.write(frame)
        
        # IMPORTANT: Drain the buffer to ensure data is actually sent.
        # Without this, data sits in the buffer indefinitely.
        await writer.drain()
