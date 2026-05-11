# IoT Telemetry Pipeline — Source

A distributed system for real-time monitoring of greenhouse conditions. Sensors push readings over TCP using Protobuf; the server stores them and exposes both REST (with content negotiation) and WebSocket APIs.

## Architecture Overview

```
Sensors (TCP)         →  Telemetry Server      →  SQLite DB
                         - TCP Ingest (9000)
                         - REST API (8080)
                         - Broadcaster
                                  ↓
WebSocket Clients (WS) →  WS Server (8000)
REST Clients (HTTP)    →  REST API (8080)
```

**Components:**
- **Sensors** (`client/`): Generate plausible readings, send via TCP with length-prefixed Protobuf.
- **Server** (`server/`): Receives TCP connections, deserializes, stores in SQLite.
- **REST API** (`server/rest_api.py`): Content-negotiated responses (JSON, XML, YAML).
- **WebSocket** (`wss/`): Live subscription feed with client-side buffering.
- **Storage** (`server/storage.py`): SQLite with asyncio integration via thread pool executor.

## Setup

### Prerequisites
- Python 3.9+
- `protobuf` compiler (for `.proto` → Python stubs)
- Dependencies in `requirements.txt`

### Installation

```bash
# 1. Install dependencies
pip install -r requirements.txt

pip install --upgrade protobuf

# 2. Compile Protobuf schema to Python stubs
cd Source
protoc --python_out=. proto/telemetry.proto

# 3. Verify (should create proto/telemetry_pb2.py)
ls -la proto/telemetry_pb2.py
```

## Running the System

Open **three separate terminals** from the project directory:

### Terminal 1: Telemetry Server
```bash
python -m server
```
Output:
```
2026-05-10 10:15:00 - server - INFO - ============================================================
2026-05-10 10:15:00 - server - INFO - Telemetry Server Starting
2026-05-10 10:15:00 - server - INFO - Initializing storage: telemetry.db
2026-05-10 10:15:00 - server - INFO - Starting TCP ingest server on 127.0.0.1:9000
2026-05-10 10:15:00 - server - INFO - Starting reading stream server on 127.0.0.1:9001
2026-05-10 10:15:00 - server - INFO - Reading stream listening at 127.0.0.1:9001
2026-05-10 10:15:00 - server - INFO - Starting REST API server on 127.0.0.1:8080
2026-05-10 10:15:00 - server - INFO - REST API running at http://127.0.0.1:8080
2026-05-10 10:15:00 - server - INFO - Server ready. Press Ctrl+C to shut down.
```

**Listens on:**
- TCP port 9000 (sensor ingest)
- TCP port 9001 (reading stream for wss module)
- HTTP port 8080 (REST API + dashboard)

### Terminal 2: WebSocket Live Feed Server
```bash
python -m wss
```
Output:
```
2026-05-10 10:15:01 - wss - INFO - WebSocket Live Feed Server Starting
2026-05-10 10:15:01 - wss - INFO - Broadcaster initialized
2026-05-10 10:15:01 - wss - INFO - Handler configured with broadcaster
2026-05-10 10:15:01 - wss - INFO - Starting WebSocket server on ws://127.0.0.1:8000/live
2026-05-10 10:15:01 - wss - INFO - WebSocket server listening at ws://127.0.0.1:8000/live
2026-05-10 10:15:01 - wss - INFO - Connecting to reading stream at 127.0.0.1:9001
2026-05-10 10:15:01 - wss - INFO - Connected to reading stream
```

**Listens on:**
- WebSocket port 8000 (at `/live` endpoint)

**Connects to:**
- Server's reading stream on port 9001

### Terminal 3: Sensor Simulator
```bash
python -m client --config config/sensors.yaml
```
Output:
```
2026-05-10 10:15:02 - client - INFO - Loaded config: 4 sensors
2026-05-10 10:15:02 - client - INFO - Server: 127.0.0.1:9000
2026-05-10 10:15:02 - client.simulator - INFO - [greenhouse-a-temp] Connecting to 127.0.0.1:9000
2026-05-10 10:15:02 - client.simulator - INFO - [greenhouse-a-temp] Connected!
2026-05-10 10:15:02 - client.simulator - INFO - [greenhouse-a-humidity] Connecting to 127.0.0.1:9000
```

All sensors will connect and start pushing readings every N seconds (as configured).

## REST API

All endpoints support **content negotiation** via the `Accept` header. Default: JSON.

### Dashboard (Web UI)
Open your browser and navigate to:
```
http://localhost:8080/
```

The dashboard displays:
- **Real-time sensor values** with latest readings
- **Connection status** (connected/disconnected)
- **Sensor count** and last update timestamp
- **Temperature trend chart** (last 20 readings)
- **Auto-reconnection** if WebSocket drops

The dashboard connects to the WebSocket feed and displays all sensors automatically.

### List Sensors
```bash
# JSON (default)
curl http://localhost:8080/sensors

# XML
curl -H "Accept: application/xml" http://localhost:8080/sensors

# YAML
curl -H "Accept: application/xml" http://localhost:8080/sensors

```

**Response (JSON):**
```json
[
  {"id": "greenhouse-a-temp", "type": "TEMPERATURE", "location": "Greenhouse A, Temperature Sensor"},
  {"id": "greenhouse-a-humidity", "type": "HUMIDITY", "location": "Greenhouse A, Humidity Sensor"},
  ...
]
```

### Query Historical Readings
```bash
# All readings (up to 10,000 by default)
curl "http://localhost:8080/sensors/greenhouse-a-temp/readings"

# Last 24 hours of readings
curl "http://localhost:8080/sensors/greenhouse-a-temp/readings?from=1620000000&to=1620086400"

# Get only 100 most recent readings
curl "http://localhost:8080/sensors/greenhouse-a-temp/readings?limit=100"

# XML format
curl -H "Accept: application/xml" \
  "http://localhost:8080/sensors/greenhouse-a-temp/readings?from=1620000000&to=1620086400"

# YAML format
curl -H "Accept: application/yaml" \
  "http://localhost:8080/sensors/greenhouse-a-temp/readings?from=1620000000&to=1620086400"
```

**Query parameters:**
- `from`: Unix timestamp (inclusive start time)
- `to`: Unix timestamp (inclusive end time)
- `limit`: Max readings to return (default: 10000, max recommended: 50000)

**Response (JSON):**
```json
[
  {"sensor_id": "greenhouse-a-temp", "type": "TEMPERATURE", "value": 20.5, "timestamp": "2021-05-03T10:01:40Z"},
  {"sensor_id": "greenhouse-a-temp", "type": "TEMPERATURE", "value": 20.3, "timestamp": "2021-05-03T10:01:45Z"},
  ...
]
```

### Register a New Sensor
```bash
curl -X POST http://localhost:8080/sensors \
  -H "Content-Type: application/json" \
  -d '{
    "id": "greenhouse-d-light",
    "type": "LIGHT",
    "location": "Greenhouse D, Main Light"
  }'
```

**Response:** `201 Created`
```
Location: /sensors/greenhouse-d-light
Sensor greenhouse-d-light registered
```

### Delete a Sensor
```bash
curl -X DELETE http://localhost:8080/sensors/greenhouse-d-light
```

**Response:** `204 No Content`

### Sessions
Every request returns a `session_id` cookie. Clients automatically send it on subsequent requests:
```bash
# First request: server sets the cookie
curl -v http://localhost:8080/sensors

# Cookie is automatically stored and sent on next request
curl http://localhost:8080/sensors
```

## WebSocket Live Feed

Connect to `ws://127.0.0.1:8000/live` and receive readings in real-time.

### JavaScript Example
```javascript
const ws = new WebSocket("ws://127.0.0.1:8000/live");

ws.onopen = () => {
    console.log("Connected to live feed");
    
    // After 2 seconds, subscribe to temperature only
    setTimeout(() => {
        ws.send(JSON.stringify({
            action: "subscribe",
            sensors: ["greenhouse-a-temp"]
        }));
    }, 2000);
};

ws.onmessage = (event) => {
    const reading = JSON.parse(event.data);
    console.log(`${reading.sensor_id}: ${reading.value}°C at ${reading.ts}`);
};

ws.onerror = (error) => {
    console.error("WebSocket error:", error);
};
```

### Python Example (with `websockets`)
```python
import asyncio
import json
import websockets

async def main():
    async with websockets.connect("ws://127.0.0.1:8000/live") as ws:
        # Subscribe to specific sensors
        await ws.send(json.dumps({
            "action": "subscribe",
            "sensors": ["greenhouse-a-temp", "greenhouse-a-humidity"]
        }))
        
        # Receive readings
        async for message in ws:
            reading = json.loads(message)
            print(f"{reading['sensor_id']}: {reading['value']}")

asyncio.run(main())
```

### cURL Example (simpler, just one message)
```bash
# Can't truly stream with curl, but can test connection:
curl -i -N -H "Connection: Upgrade" -H "Upgrade: websocket" \
  http://localhost:8000/live
# (Will upgrade to WebSocket, but curl won't display frames)
```

## Protocol Details

### TCP Ingest (Sensor → Server)
Sensors send **length-prefixed Protobuf** frames:
```
[4 bytes: big-endian message length] [Protobuf Reading message]
```

**Example (hex):**
```
00 00 00 28  [40 bytes of Protobuf payload]
```

### Protobuf Messages
- **`Reading`**: sensor_id, sensor_type (enum), value (float), timestamp (int64)
- **`SensorType`**: enum with TEMPERATURE, HUMIDITY, SOIL_MOISTURE, LIGHT

See `proto/telemetry.proto` for the schema.

### REST API Status Codes
- `200 OK`: Successful query
- `201 Created`: Sensor registered
- `204 No Content`: Sensor deleted
- `400 Bad Request`: Invalid query parameters
- `404 Not Found`: Sensor or readings not found

### WebSocket Protocol
**Client → Server** (optional subscription):
```json
{"action": "subscribe", "sensors": ["sensor-id-1", "sensor-id-2"]}
```
If no subscription sent, client receives all readings.

**Server → Client** (continuous):
```json
{"sensor_id": "greenhouse-a-temp", "type": "TEMPERATURE", "value": 20.5, "ts": 1620000100}
```

## Configuration

### Sensor Config (`config/sensors.yaml`)
Edit to add, remove, or modify sensors:
```yaml
server:
  host: 127.0.0.1
  port: 9000

sensors:
  - id: greenhouse-a-temp
    type: TEMPERATURE        # One of: TEMPERATURE, HUMIDITY, SOIL_MOISTURE, LIGHT
    interval_seconds: 5
    location: "Greenhouse A, Temperature Sensor"

  - id: greenhouse-b-soil
    type: SOIL_MOISTURE
    interval_seconds: 10
    location: "Greenhouse B, Soil Moisture"
```

### Server Ports
Edit `server/__main__.py` and `wss/__main__.py` to change:
- TCP ingest: 9000 → change `TCP_INGEST_PORT`
- REST API: 8080 → change `REST_API_PORT`
- WebSocket: 8000 → change `WS_PORT`

## Storage

**Database:** `telemetry.db` (SQLite, created automatically)

**Tables:**
- `sensors`: sensor metadata (id, type, location)
- `readings`: measurements (sensor_id, value, timestamp, type)
- Index: (sensor_id, timestamp) for fast time-range queries

**Data Retention:**
- **All readings are stored permanently** — no automatic cleanup
- Database grows continuously as sensors produce data
- ~1KB per reading (varies by sensor ID/type length)
- Example: 4 sensors at 1 reading/sec = ~345 MB per day

**Checking stored data:**
```bash
# Count total readings
sqlite3 telemetry.db "SELECT COUNT(*) FROM readings;"

# Count readings per sensor
sqlite3 telemetry.db "SELECT sensor_id, COUNT(*) FROM readings GROUP BY sensor_id;"

# Check database file size
ls -lh telemetry.db
```

To reset: `rm telemetry.db` (it'll be recreated on next server start)

## Design Notes

### Concurrency Model
- **Asyncio, no threads**: All I/O (network, database) is async. Database calls use a thread pool executor to avoid blocking the event loop.
- **Multiple sensors**: Each sensor has its own coroutine. 100 sensors = 100 concurrent coroutines, not 100 threads.
- **Slow consumers**: WebSocket clients are buffered (up to 100 messages). If buffer fills, oldest messages are dropped. If queue exceeds 500, the client is disconnected.

### Failure Handling
- **Sensor disconnects**: Server logs the disconnect, closes the connection gracefully.
- **Malformed Protobuf**: Server logs a warning, skips the message, keeps the connection open.
- **Slow WebSocket client**: Messages are buffered. If client can't keep up, it's eventually disconnected.
- **Database errors**: Logged. REST API returns 500 Internal Server Error.

### Serialization Trade-offs
- **Protobuf (TCP)**: Compact, fast, binary. Ideal for high-frequency sensor data.
- **JSON (REST)**: Human-readable, browser-friendly, standard.
- **XML/YAML (REST)**: For legacy systems and config files.

## Testing Checklist

- [ ] All sensors connect and send readings
- [ ] REST API returns 200 for `GET /sensors`
- [ ] Dashboard displays at `http://localhost:8080/`
- [ ] Content negotiation: request with `Accept: application/xml` gets XML
- [ ] Register a sensor with `POST /sensors`
- [ ] Query readings with time range: `?from=...&to=...`
- [ ] Session cookies: request twice, second request includes `Cookie` header
- [ ] WebSocket: connect to `/live`, receive readings in real-time
- [ ] WebSocket subscription: send `{"action": "subscribe", "sensors": [...]}`, filter works
- [ ] Slow consumer: hold WebSocket connection without reading, eventually disconnect

## Troubleshooting

### Protobuf Version Mismatch
If you get: `Detected incompatible Protobuf Gencode/Runtime versions`

**Solution:** Upgrade the protobuf package:
```bash
pip install --upgrade protobuf
```

Then recompile the schema:
```bash
protoc --python_out=. proto/telemetry.proto
```

### Sensors Not Registering
If `GET /sensors` returns `[]` after sensors connect:

1. Check the **client logs** — do you see `[sensor-id] Connected!`?
2. Check the **server logs** — do you see `Sensor registered: ...`?
3. Restart the client to force re-registration

### WebSocket Connection Refused
If the dashboard says "Disconnected":

1. Make sure `python -m wss` is running
2. Check that it's listening on `127.0.0.1:8000`
3. Make sure `python -m server` is running (wss needs to connect to reading stream on port 9001)
4. Check your firewall/network settings

### Empty Dashboard
If sensors are registered but dashboard shows no readings:

1. Ensure the WebSocket server (`python -m wss`) is running
2. Ensure the main server (`python -m server`) is running
3. Open browser console (F12) and check for JavaScript errors
4. Verify that sensors are actively publishing (check `python -m client` logs)

### Only seeing recent data on dashboard
The **dashboard live chart only shows the last 30 readings** per sensor (for performance). This is normal and doesn't mean older data is lost.

To verify all data is stored:
```bash
# Query the REST API for all readings
curl "http://localhost:8080/sensors/greenhouse-a-temp/readings?limit=100"

# Count total stored readings
sqlite3 telemetry.db "SELECT COUNT(*) FROM readings WHERE sensor_id='greenhouse-a-temp';"
```

### Database grows too large
If `telemetry.db` becomes too large, implement a cleanup policy:

```bash
# Delete readings older than 30 days
sqlite3 telemetry.db "DELETE FROM readings WHERE timestamp < strftime('%s', 'now') - 2592000;"

# Vacuum to reclaim space
sqlite3 telemetry.db "VACUUM;"
```

## Authors

- TODO: Name 1, Student ID
- TODO: Name 2, Student ID
