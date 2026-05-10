"""Content negotiation for the REST API.

Maps the `Accept` header on a request to a serializer for the response.
Supported media types:
    application/json
    application/xml
    application/yaml (also accepts text/yaml)

Falls back to JSON when no supported type matches.
"""
from __future__ import annotations

import json
from typing import Any
from xml.etree import ElementTree as ET
from aiohttp import web
import yaml


def negotiate(request: web.Request) -> str:
    """Return the chosen response media type for `request`.
    
    Parses the Accept header (with quality values) and returns the best
    supported media type. Defaults to application/json.
    """
    accept_header = request.headers.get("Accept", "application/json")
    
    # Parse Accept header with quality values
    # Format: "type1;q=0.9, type2;q=1.0"
    preferences = []
    for part in accept_header.split(","):
        media_type = part.split(";")[0].strip()
        
        # Extract quality value (default 1.0)
        quality = 1.0
        if "q=" in part:
            try:
                quality = float(part.split("q=")[1])
            except (ValueError, IndexError):
                pass
        
        preferences.append((media_type, quality))
    
    # Sort by quality (descending)
    preferences.sort(key=lambda x: x[1], reverse=True)
    
    # Return first match (or json if none match)
    supported = {
        "application/json": "application/json",
        "application/xml": "application/xml",
        "application/yaml": "application/yaml",
        "text/yaml": "application/yaml",
        "*/*": "application/json",  # wildcard defaults to json
    }
    
    for media_type, _ in preferences:
        if media_type in supported:
            return supported[media_type]
    
    # Fallback
    return "application/json"


def serialize(payload: Any, media_type: str) -> bytes:
    """Serialize `payload` (a dict or list of dicts) into bytes.
    
    Args:
        payload: A dict or list to serialize
        media_type: One of "application/json", "application/xml", "application/yaml"
    
    Returns:
        Bytes ready to send as HTTP response body
    """
    if media_type == "application/json":
        return json.dumps(payload, indent=2).encode("utf-8")
    
    elif media_type == "application/xml":
        # Convert payload to XML
        # Assume it's a dict or list of dicts
        root = ET.Element("response")
        
        if isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict):
                    item_elem = ET.SubElement(root, "item")
                    for key, value in item.items():
                        sub = ET.SubElement(item_elem, key)
                        sub.text = str(value)
                else:
                    elem = ET.SubElement(root, "item")
                    elem.text = str(item)
        elif isinstance(payload, dict):
            for key, value in payload.items():
                elem = ET.SubElement(root, key)
                if isinstance(value, (list, dict)):
                    elem.text = json.dumps(value)
                else:
                    elem.text = str(value)
        else:
            root.text = str(payload)
        
        return ET.tostring(root, encoding="utf-8")
    
    elif media_type == "application/yaml":
        return yaml.dump(payload, default_flow_style=False).encode("utf-8")
    
    else:
        # Default to JSON for unknown types
        return json.dumps(payload, indent=2).encode("utf-8")
