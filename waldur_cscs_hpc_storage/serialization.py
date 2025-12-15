import json
from enum import Enum
from typing import Any
from uuid import UUID

from starlette.responses import JSONResponse as StarletteJSONResponse


class UUIDEncoder(json.JSONEncoder):
    def default(self, obj: Any) -> Any:
        if isinstance(obj, UUID):
            return obj.hex
        if isinstance(obj, Enum):
            return obj.value
        return super().default(obj)


def dump_json(obj: Any, **kwargs) -> str:
    return json.dumps(obj, cls=UUIDEncoder, **kwargs)


class JSONResponse(StarletteJSONResponse):
    def render(self, content: Any) -> bytes:
        return json.dumps(
            content,
            ensure_ascii=False,
            allow_nan=False,
            indent=None,
            separators=(",", ":"),
            cls=UUIDEncoder,
        ).encode("utf-8")
