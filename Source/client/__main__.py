"""Entry point for the sensor simulator.

Run with:
    python -m client --config config/sensors.yaml

Loads a YAML configuration file, spawns one coroutine per sensor,
and runs them all concurrently.
"""
from __future__ import annotations

import asyncio
import argparse
import logging
import yaml
from pathlib import Path
from client.simulator import SensorSimulator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def load_config(config_path: str) -> dict:
    """Load and validate the YAML configuration file.
    
    Args:
        config_path: Path to sensors.yaml
    
    Returns:
        Parsed YAML dict with keys: server, sensors
    
    Raises:
        FileNotFoundError: if config file doesn't exist
        ValueError: if config structure is invalid
    """
    config_file = Path(config_path)
    
    if not config_file.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with open(config_file, "r") as f:
        config = yaml.safe_load(f)
    
    # Validate structure
    if not isinstance(config, dict):
        raise ValueError("Config must be a YAML dict")
    
    if "server" not in config:
        raise ValueError("Config must have a 'server' section")
    
    server = config["server"]
    if "host" not in server or "port" not in server:
        raise ValueError("'server' section must have 'host' and 'port'")
    
    if "sensors" not in config:
        raise ValueError("Config must have a 'sensors' section")
    
    sensors = config["sensors"]
    if not isinstance(sensors, list) or len(sensors) == 0:
        raise ValueError("'sensors' must be a non-empty list")
    
    # Validate each sensor
    for i, sensor in enumerate(sensors):
        if not isinstance(sensor, dict):
            raise ValueError(f"Sensor {i} must be a dict")
        if "id" not in sensor or "type" not in sensor or "interval_seconds" not in sensor:
            raise ValueError(
                f"Sensor {i} must have 'id', 'type', and 'interval_seconds'"
            )
    
    logger.info(f"Loaded config: {len(sensors)} sensors")
    return config


async def main() -> None:
    """Load the YAML config, spawn one task per sensor, run them all."""
    parser = argparse.ArgumentParser(
        description="Sensor simulator for IoT telemetry pipeline"
    )
    parser.add_argument(
        "--config",
        required=True,
        help="Path to YAML configuration file (e.g. config/sensors.yaml)"
    )
    
    args = parser.parse_args()
    
    try:
        config = load_config(args.config)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        return
    
    # Extract server details
    server_host = config["server"]["host"]
    server_port = config["server"]["port"]
    
    logger.info(f"Server: {server_host}:{server_port}")
    
    # Spawn one simulator per sensor
    tasks = []
    for sensor_config in config["sensors"]:
        simulator = SensorSimulator(
            sensor_id=sensor_config["id"],
            sensor_type=sensor_config["type"],
            interval_seconds=sensor_config["interval_seconds"],
            host=server_host,
            port=server_port,
        )
        logger.info(
            f"Starting sensor: {sensor_config['id']} "
            f"({sensor_config['type']}, interval={sensor_config['interval_seconds']}s)"
        )
        tasks.append(simulator.run())
    
    # Run all sensors concurrently
    try:
        logger.info(f"Running {len(tasks)} sensors...")
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        logger.info("Shutting down sensors...")


if __name__ == "__main__":
    asyncio.run(main())
