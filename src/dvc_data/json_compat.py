import json
from typing import Any

try:
    import orjson  # type: ignore[import-not-found]
except ImportError:

    def loads(data: str) -> Any:
        return json.loads(data)

    def dumps(data: Any) -> str:
        return json.dumps(data)
else:

    def loads(data: str) -> Any:
        return orjson.loads(data)

    def dumps(data: Any) -> str:
        return orjson.dumps(data).decode("utf8")


__all__ = ["loads", "dumps"]
