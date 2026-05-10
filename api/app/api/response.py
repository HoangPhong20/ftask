from typing import Any


def ok(data: Any = None, **meta: Any) -> dict[str, Any]:
    # Standard DaaS envelope: every response returns `data` plus request metadata.
    meta = {key: value for key, value in meta.items() if value is not None}
    return {"data": data, "meta": meta}
