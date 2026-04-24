import threading
import time
from collections import OrderedDict
from typing import Any


class TTLCache:
    def __init__(self, ttl_seconds: int = 60, max_entries: int = 500) -> None:
        self.ttl_seconds = max(ttl_seconds, 1)
        self.max_entries = max(max_entries, 1)
        self._data: OrderedDict[str, tuple[float, Any]] = OrderedDict()
        self._lock = threading.Lock()

    def _now(self) -> float:
        return time.time()

    def _evict_expired(self) -> None:
        now = self._now()
        stale_keys = [k for k, (exp, _) in self._data.items() if exp <= now]
        for key in stale_keys:
            self._data.pop(key, None)

    def get(self, key: str) -> Any | None:
        with self._lock:
            self._evict_expired()
            item = self._data.get(key)
            if item is None:
                return None
            exp, value = item
            if exp <= self._now():
                self._data.pop(key, None)
                return None
            self._data.move_to_end(key)
            return value

    def set(self, key: str, value: Any) -> None:
        with self._lock:
            self._evict_expired()
            self._data[key] = (self._now() + self.ttl_seconds, value)
            self._data.move_to_end(key)
            while len(self._data) > self.max_entries:
                self._data.popitem(last=False)
