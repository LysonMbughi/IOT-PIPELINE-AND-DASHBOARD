"""Storage layer for sensors and readings.

The backing store is SQLite. The interface below is what the rest of the server uses.
All blocking I/O is run in a thread pool to avoid blocking the asyncio event loop.
"""
from __future__ import annotations

import asyncio
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from typing import Iterable, Optional
from dataclasses import dataclass


@dataclass
class Sensor:
    """Metadata about a registered sensor."""
    id: str
    type: str  # e.g. "TEMPERATURE"
    location: str


@dataclass
class Reading:
    """A single sensor measurement."""
    sensor_id: str
    sensor_type: str
    value: float
    timestamp: int  # Unix epoch seconds


class Storage:
    """SQLite-backed storage with asyncio integration."""

    def __init__(self, db_path: str = "telemetry.db"):
        """Initialize and create schema if needed."""
        self.db_path = db_path
        self.executor = ThreadPoolExecutor(max_workers=2)
        
        # Run schema creation synchronously on init
        self._init_db()

    def _init_db(self) -> None:
        """Create tables if they don't exist."""
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sensors (
                id TEXT PRIMARY KEY,
                type TEXT NOT NULL,
                location TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS readings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sensor_id TEXT NOT NULL,
                sensor_type TEXT NOT NULL,
                value REAL NOT NULL,
                timestamp INTEGER NOT NULL,
                FOREIGN KEY (sensor_id) REFERENCES sensors(id)
            )
        """)
        # Index for fast time-range queries
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_readings_sensor_time
            ON readings (sensor_id, timestamp)
        """)
        conn.commit()
        conn.close()

    async def add_sensor(self, sensor: Sensor) -> None:
        """Register a new sensor."""
        def _insert():
            conn = sqlite3.connect(self.db_path)
            conn.execute(
                "INSERT OR IGNORE INTO sensors (id, type, location) VALUES (?, ?, ?)",
                (sensor.id, sensor.type, sensor.location)
            )
            conn.commit()
            conn.close()
        
        # Run blocking DB call in executor, await it
        await asyncio.get_event_loop().run_in_executor(self.executor, _insert)

    async def remove_sensor(self, sensor_id: str) -> None:
        """Remove a sensor and its readings."""
        def _delete():
            conn = sqlite3.connect(self.db_path)
            conn.execute("DELETE FROM readings WHERE sensor_id = ?", (sensor_id,))
            conn.execute("DELETE FROM sensors WHERE id = ?", (sensor_id,))
            conn.commit()
            conn.close()
        
        await asyncio.get_event_loop().run_in_executor(self.executor, _delete)

    async def sensor_exists(self, sensor_id: str) -> bool:
        """Check if a sensor is already registered."""
        def _query():
            conn = sqlite3.connect(self.db_path)
            cursor = conn.execute("SELECT 1 FROM sensors WHERE id = ?", (sensor_id,))
            result = cursor.fetchone() is not None
            conn.close()
            return result
        
        return await asyncio.get_event_loop().run_in_executor(self.executor, _query)

    async def list_sensors(self) -> list[Sensor]:
        """Return all registered sensors."""
        def _query():
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("SELECT id, type, location FROM sensors ORDER BY id")
            rows = cursor.fetchall()
            conn.close()
            return [Sensor(row['id'], row['type'], row['location']) for row in rows]
        
        return await asyncio.get_event_loop().run_in_executor(self.executor, _query)

    async def add_reading(self, reading: Reading) -> None:
        """Persist a single reading."""
        def _insert():
            conn = sqlite3.connect(self.db_path)
            conn.execute(
                "INSERT INTO readings (sensor_id, sensor_type, value, timestamp) VALUES (?, ?, ?, ?)",
                (reading.sensor_id, reading.sensor_type, reading.value, reading.timestamp)
            )
            conn.commit()
            conn.close()
        
        await asyncio.get_event_loop().run_in_executor(self.executor, _insert)

    async def get_readings(
        self,
        sensor_id: str,
        from_ts: Optional[int] = None,
        to_ts: Optional[int] = None,
    ) -> list[Reading]:
        """Return readings for a sensor within an optional time window (Unix timestamps)."""
        def _query():
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            
            query = "SELECT sensor_id, sensor_type, value, timestamp FROM readings WHERE sensor_id = ?"
            params: list[str | int] = [sensor_id]
            
            if from_ts is not None:
                query += " AND timestamp >= ?"
                params.append(from_ts)
            if to_ts is not None:
                query += " AND timestamp <= ?"
                params.append(to_ts)
            
            query += " ORDER BY timestamp"
            
            cursor = conn.execute(query, params)
            rows = cursor.fetchall()
            conn.close()
            return [
                Reading(row['sensor_id'], row['sensor_type'], row['value'], row['timestamp'])
                for row in rows
            ]
        
        return await asyncio.get_event_loop().run_in_executor(self.executor, _query)
