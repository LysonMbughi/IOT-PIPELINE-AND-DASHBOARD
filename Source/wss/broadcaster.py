"""Tracks connected WebSocket clients and dispatches readings to them.

Owns the set of live clients, their subscription filters, and a way for
producers (the telemetry server) to publish a new reading.

Handles slow consumers by buffering with a limit. When a client's buffer
exceeds the limit, we disconnect it.
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Optional, Set
from collections import defaultdict, deque

logger = logging.getLogger(__name__)

# Tuning parameters for slow consumer handling
MAX_BUFFER_PER_CLIENT = 100  # Start dropping messages after this many queued
HARD_DISCONNECT_THRESHOLD = 500  # Disconnect if queue gets this big


class Broadcaster:
    """Fan-out of readings to the set of connected WebSocket clients.
    
    Each client has:
      - A websocket (the connection)
      - A subscription filter (list of sensor IDs to receive, or None for all)
      - A buffer queue (for messages waiting to be sent)
    """

    def __init__(self) -> None:
        """Initialize the broadcaster."""
        # Map: websocket -> {"subscription": set of sensor IDs, "queue": deque}
        self.clients: dict = {}
        # Lock for thread-safe access (even though we're asyncio, be safe)
        self.lock = asyncio.Lock()

    async def register(self, websocket) -> None:
        """Add a newly connected client.
        
        Initially, the client receives all sensor readings (subscription = None).
        """
        async with self.lock:
            self.clients[websocket] = {
                "subscription": None,  # None = all sensors
                "queue": deque(maxlen=MAX_BUFFER_PER_CLIENT),
            }
        logger.info(f"Broadcaster: client registered. Total: {len(self.clients)}")

    async def unregister(self, websocket) -> None:
        """Remove a disconnected client."""
        async with self.lock:
            if websocket in self.clients:
                del self.clients[websocket]
        logger.info(f"Broadcaster: client unregistered. Total: {len(self.clients)}")

    async def set_subscription(self, websocket, sensor_ids: Optional[list[str]]) -> None:
        """Replace the per-client sensor-id filter.
        
        Args:
            websocket: The client connection
            sensor_ids: List of sensor IDs to subscribe to (None = all)
        """
        async with self.lock:
            if websocket in self.clients:
                # Convert list to set for fast lookup
                self.clients[websocket]["subscription"] = set(sensor_ids) if sensor_ids else None
                logger.debug(f"Broadcaster: set subscription for client to {sensor_ids}")

    async def publish(self, reading) -> None:
        """Push a reading (storage.Reading object) to every interested client.
        
        For each client:
          1. Check if subscription matches
          2. Try to queue the message
          3. If buffer is full (maxlen hit), warn
          4. If queue is too full, disconnect the client
        """
        # Serialize the reading to JSON (WebSocket standard)
        payload = {
            "sensor_id": reading.sensor_id,
            "type": reading.sensor_type,
            "value": reading.value,
            "ts": reading.timestamp,
        }
        message_json = json.dumps(payload)
        
        await self._publish_json(payload, message_json, reading.sensor_id)

    async def publish_reading(self, reading_dict: dict) -> None:
        """Push a reading (dict from reading stream) to every interested client.
        
        Args:
            reading_dict: Dict with keys: sensor_id, type, value, ts
        """
        message_json = json.dumps(reading_dict)
        await self._publish_json(reading_dict, message_json, reading_dict.get("sensor_id"))

    async def _publish_json(self, payload: dict, message_json: str, sensor_id: str) -> None:
        """Internal method to publish a JSON message to all interested clients.
        
        Args:
            payload: Dict payload (for subscription checking)
            message_json: JSON-serialized message string
            sensor_id: Sensor ID for subscription filtering
        """
        
        # Get snapshot of clients to avoid holding lock during sends
        async with self.lock:
            clients_snapshot = list(self.clients.items())
        
        # Send to each interested client
        disconnected = []
        
        for websocket, client_info in clients_snapshot:
            # Check subscription filter
            subscription = client_info["subscription"]
            if subscription is not None and sensor_id not in subscription:
                # Client not interested in this sensor
                continue
            
            queue = client_info["queue"]
            
            # Check if queue is critically full
            if len(queue) > HARD_DISCONNECT_THRESHOLD:
                logger.warning(
                    f"Broadcaster: queue too full for client (size {len(queue)}), disconnecting"
                )
                disconnected.append(websocket)
                continue
            
            # Add message to queue
            try:
                queue.append(message_json)
            except Exception as e:
                logger.error(f"Broadcaster: error queueing message: {e}")
        
        # Disconnect slow clients
        for ws in disconnected:
            try:
                await ws.close(code=1008, message="Buffer overflow")
            except Exception as e:
                logger.error(f"Broadcaster: error closing websocket: {e}")
            await self.unregister(ws)
        
        # Drain queues (send buffered messages to clients)
        # This runs concurrently for all clients
        await self._drain_queues(clients_snapshot)

    async def _drain_queues(self, clients_snapshot: list) -> None:
        """Send all queued messages to clients that are ready.
        
        This is a helper that runs sends concurrently so we don't block
        if one client is slow.
        """
        tasks = []
        
        for websocket, client_info in clients_snapshot:
            if websocket not in self.clients:
                # Client was unregistered, skip
                continue
            
            queue = client_info["queue"]
            
            async def send_queued():
                """Send all messages in this client's queue."""
                try:
                    while queue:
                        message_json = queue.popleft()
                        # Send with a timeout so we don't wait forever
                        try:
                            await asyncio.wait_for(
                                websocket.send(message_json),
                                timeout=2.0
                            )
                        except asyncio.TimeoutError:
                            logger.warning(f"Broadcaster: send timeout for client, queueing message back")
                            queue.appendleft(message_json)  # Re-queue if timeout
                            break
                except Exception as e:
                    logger.debug(f"Broadcaster: error sending to client: {e}")
                    # On error, unregister the client
                    await self.unregister(websocket)
            
            tasks.append(send_queued())
        
        # Run all sends concurrently
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
