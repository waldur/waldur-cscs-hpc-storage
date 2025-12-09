from typing import Any, Dict


class JsonSerializer:
    """Helper class to serialize Waldur resource objects for JSON output."""

    def serialize(self, resource: object) -> Dict[str, Any]:
        """Serialize a Waldur resource object for JSON output."""
        result = self._serialize_value(resource)
        return result if isinstance(result, dict) else {"serialized": result}

    def _serialize_value(self, value: object) -> Any:
        """Convert various types to JSON-serializable format."""
        if hasattr(value, "__dict__"):
            return {k: self._serialize_value(v) for k, v in value.__dict__.items()}
        if isinstance(value, (list, tuple)):
            return [self._serialize_value(item) for item in value]
        if isinstance(value, dict):
            return {k: self._serialize_value(v) for k, v in value.items()}
        return str(value) if value is not None else None
