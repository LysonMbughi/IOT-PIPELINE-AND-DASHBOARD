"""REST API for the telemetry server.

Endpoints:
    GET    /                              serve the dashboard HTML
    GET    /sensors                       list registered sensors
    GET    /sensors/{id}/readings         historical readings  (?from=&to=)
    POST   /sensors                       register a new sensor
    DELETE /sensors/{id}                  remove a sensor

Content negotiation:
    Server-driven via the `Accept` header. Supported media types:
      application/json, application/xml, application/yaml.
    Delegates to server.serialization.

Sessions:
    A cookie identifies the client session — set on first response, read
    on subsequent requests.
"""
from __future__ import annotations

import uuid
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional
from aiohttp import web
from server.serialization import negotiate, serialize
from server.storage import Sensor, Reading

logger = logging.getLogger(__name__)

# Storage instance (set by build_app)
_storage = None


async def serve_dashboard(request: web.Request) -> web.Response:
    """GET / — serve the dashboard HTML."""
    dashboard_path = Path(__file__).parent / "dashboard.html"
    
    if not dashboard_path.exists():
        return web.Response(
            text="Dashboard not found. Create server/dashboard.html",
            status=404
        )
    
    with open(dashboard_path, "r") as f:
        html_content = f.read()
    
    return web.Response(
        text=html_content,
        content_type="text/html",
        status=200
    )


async def list_sensors(request: web.Request) -> web.Response:
    """GET /sensors — list all registered sensors."""
    sensors = await _storage.list_sensors()
    
    # Convert to dicts for serialization
    payload = [
        {"id": s.id, "type": s.type, "location": s.location}
        for s in sensors
    ]
    
    media_type = negotiate(request)
    body = serialize(payload, media_type)
    
    response = web.Response(body=body, content_type=media_type, status=200)
    return response


async def get_readings(request: web.Request) -> web.Response:
    """GET /sensors/{id}/readings — historical readings for a sensor.
    
    Query parameters:
      - from: Unix timestamp (inclusive)
      - to: Unix timestamp (inclusive)
      - limit: Max number of readings to return (default: 10000)
    """
    sensor_id = request.match_info["id"]
    
    # Parse query parameters: from and to (Unix timestamps)
    from_ts = None
    to_ts = None
    limit = 10000  # Default limit to prevent memory overload
    
    if "from" in request.rel_url.query:
        try:
            from_ts = int(request.rel_url.query["from"])
        except ValueError:
            return web.Response(
                text='Query param "from" must be a Unix timestamp (integer)',
                status=400
            )
    
    if "to" in request.rel_url.query:
        try:
            to_ts = int(request.rel_url.query["to"])
        except ValueError:
            return web.Response(
                text='Query param "to" must be a Unix timestamp (integer)',
                status=400
            )
    
    if "limit" in request.rel_url.query:
        try:
            limit = int(request.rel_url.query["limit"])
            if limit <= 0:
                raise ValueError("limit must be > 0")
        except ValueError:
            return web.Response(
                text='Query param "limit" must be a positive integer',
                status=400
            )
    
    # Query storage
    readings = await _storage.get_readings(sensor_id, from_ts, to_ts, limit)
    
    if not readings:
        return web.Response(
            text=f"No readings found for sensor {sensor_id}",
            status=404
        )
    
    # Convert to dicts with ISO 8601 formatted timestamps
    payload = [
        {
            "sensor_id": r.sensor_id, 
            "type": r.sensor_type, 
            "value": r.value, 
            "timestamp": datetime.utcfromtimestamp(r.timestamp).isoformat() + "Z"
        }
        for r in readings
    ]
    
    media_type = negotiate(request)
    body = serialize(payload, media_type)
    
    response = web.Response(body=body, content_type=media_type, status=200)
    return response


async def register_sensor(request: web.Request) -> web.Response:
    """POST /sensors — register a new sensor.
    
    Accepts JSON body: {"id": "...", "type": "...", "location": "..."}
    """
    try:
        data = await request.json()
    except Exception as e:
        return web.Response(
            text=f"Invalid JSON body: {e}",
            status=400
        )
    
    # Validate required fields
    if not data.get("id") or not data.get("type"):
        return web.Response(
            text='Request body must include "id" and "type"',
            status=400
        )
    
    sensor = Sensor(
        id=data["id"],
        type=data["type"],
        location=data.get("location", "")
    )
    
    await _storage.add_sensor(sensor)
    
    # Return 201 Created with Location header
    response = web.Response(
        text=f'Sensor {sensor.id} registered',
        status=201
    )
    response.headers["Location"] = f"/sensors/{sensor.id}"
    return response


async def delete_sensor(request: web.Request) -> web.Response:
    """DELETE /sensors/{id} — remove a sensor."""
    sensor_id = request.match_info["id"]
    
    await _storage.remove_sensor(sensor_id)
    
    # Return 204 No Content
    return web.Response(status=204)


@web.middleware
async def session_cookie_middleware(request: web.Request, handler) -> web.Response:
    """Middleware that sets/reads the session cookie on every request.
    
    The cookie value is a UUID that identifies the client session.
    On first request, a cookie is created. On subsequent requests, the
    existing cookie is read.
    """
    # Try to read existing session cookie
    session_id = request.cookies.get("session_id")
    
    if not session_id:
        # No session yet, create one
        session_id = str(uuid.uuid4())
        logger.debug(f"Created new session: {session_id}")
    
    # Attach to request for handler to use if needed
    request["session_id"] = session_id
    
    # Call the handler
    response = await handler(request)
    
    # Set the cookie on the response (even if it existed, to refresh TTL)
    response.set_cookie(
        "session_id",
        session_id,
        max_age=3600,  # 1 hour TTL
        httponly=True,  # Don't allow JS to read it
        samesite="Lax"  # CSRF protection
    )
    
    return response


def build_app(storage) -> web.Application:
    """Construct and return the aiohttp Application for the REST API.
    
    Args:
        storage: Storage instance to query
    
    Returns:
        aiohttp Application ready to run
    """
    global _storage
    _storage = storage
    
    app = web.Application(middlewares=[session_cookie_middleware])
    
    # Register routes
    app.router.add_get("/", serve_dashboard)
    app.router.add_get("/sensors", list_sensors)
    app.router.add_get("/sensors/{id}/readings", get_readings)
    app.router.add_post("/sensors", register_sensor)
    app.router.add_delete("/sensors/{id}", delete_sensor)
    
    return app
