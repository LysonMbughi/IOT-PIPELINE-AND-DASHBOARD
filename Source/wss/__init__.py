"""WebSocket server for the live telemetry feed.

Connected clients receive readings as they arrive, encoded as JSON
(browser-friendly). Clients may subscribe to specific sensor IDs by
sending a JSON subscription message after the WebSocket upgrade.
"""
