"""WebSocket connection handler at /live.

One coroutine per connected client. Reads optional subscription messages
from the client and otherwise just forwards readings published by the
broadcaster.
"""
from __future__ import annotations

import json
import logging

logger = logging.getLogger(__name__)

# Broadcaster instance (set by wss/__main__.py)
_broadcaster = None


async def live(websocket) -> None:
    """Handle one WebSocket client connection at /live.

    Protocol on this socket (JSON frames):
      Client -> Server (optional, after upgrade):
          {"action": "subscribe", "sensors": ["sensor-a", "sensor-b"]}
      Server -> Client (continuous):
          {"sensor_id": "...", "type": "...", "value": ..., "ts": ...}
    """
    if not _broadcaster:
        await websocket.close(code=1011, reason="Broadcaster not initialized")
        return
    
    # Register this client with the broadcaster
    await _broadcaster.register(websocket)
    logger.info("WebSocket client connected to /live")
    
    try:
        # Read incoming subscription messages
        async for message in websocket:
            try:
                # Parse the subscription command
                data = json.loads(message)
                action = data.get("action")
                
                if action == "subscribe":
                    # Client wants to filter on specific sensors
                    sensor_ids = data.get("sensors")
                    if sensor_ids and isinstance(sensor_ids, list):
                        await _broadcaster.set_subscription(websocket, sensor_ids)
                        logger.debug(f"Updated subscription to: {sensor_ids}")
                    else:
                        logger.warning(f"Invalid subscribe message: {data}")
                else:
                    logger.warning(f"Unknown action: {action}")
            
            except json.JSONDecodeError as e:
                logger.warning(f"Malformed JSON from WebSocket client: {e}")
                # Don't close, just skip this message
                continue
            except Exception as e:
                logger.error(f"Error processing subscription message: {e}")
                continue
    
    except Exception as e:
        logger.error(f"WebSocket connection error: {e}")
    
    finally:
        # Client disconnected, unregister from broadcaster
        await _broadcaster.unregister(websocket)
        logger.info("WebSocket client disconnected from /live")


def set_broadcaster(broadcaster) -> None:
    """Set the broadcaster instance (called by wss/__main__.py)."""
    global _broadcaster
    _broadcaster = broadcaster
