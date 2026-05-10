"""Sensor simulator package.

Spawns one async task per simulated sensor. Each task connects to the
telemetry server over TCP and pushes Protobuf-encoded readings on its
configured interval.
"""
