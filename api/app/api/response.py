from typing import Any


def ok(data: Any = None, **meta: Any) -> dict[str, Any]:
    # Standard DaaS envelope: every response returns `data` plus request metadata.
    return {"data": data, "meta": meta}
