"""
src/dashboard/result_cache.py
=============================
Thread-safe, single-flight LRU result cache for dashboard analytical queries.

Key characteristics:
1. Thread-safe LRU eviction capped at maxsize (default 128).
2. Cache key incorporates ActiveDatasetStore version, stringified filters,
   the analysis key, and sorted query parameter pairs:
   (dataset_version, repr(filters), analysis_key, repr(sorted(params.items()))).
3. Single-flight execution: When multiple threads (such as the UI worker and
   background prefetch daemon) request the same key simultaneously, only ONE
   thread executes the query against Apache Spark SQL; all other callers
   await and share the identical result without launching redundant Spark jobs.
4. Process-wide singleton instance accessible via ResultCache.get_instance().
"""

from __future__ import annotations

from collections import OrderedDict
from concurrent.futures import Future
import threading
from typing import Any, Callable, TypeVar

T = TypeVar("T")


class ResultCache:
    """
    Thread-safe in-memory LRU cache with single-flight concurrency deduplication.
    """

    _instance: ResultCache | None = None
    _singleton_lock = threading.Lock()

    def __init__(self, maxsize: int = 128):
        self._maxsize: int = maxsize
        self._cache: OrderedDict[tuple, Any] = OrderedDict()
        self._in_flight: dict[tuple, Future] = {}
        self._lock = threading.RLock()
        self._hits: int = 0
        self._misses: int = 0
        self._evictions: int = 0

    @classmethod
    def get_instance(cls, maxsize: int = 128) -> ResultCache:
        """Return the process-wide ResultCache singleton."""
        with cls._singleton_lock:
            if cls._instance is None:
                cls._instance = cls(maxsize=maxsize)
            return cls._instance

    @staticmethod
    def build_key(
        version: int,
        filters: Any,
        analysis_key: str,
        params: dict[str, Any] | None = None,
        view: str = "",
    ) -> tuple:
        """
        Build a deterministic hashable cache key tuple.
        """
        f_repr = repr(filters) if filters is not None else ""
        p_repr = repr(sorted(params.items())) if params else ""
        return (version, view, f_repr, analysis_key, p_repr)

    def contains(self, key: tuple) -> bool:
        """Check if key is currently in the cache without triggering computation."""
        with self._lock:
            return key in self._cache

    def get_or_compute(self, key: tuple, compute_fn: Callable[[], T]) -> T:
        """
        Retrieve cached value or compute it with single-flight execution.

        If another thread is already computing the result for this key,
        the calling thread waits for that computation to finish rather
        than submitting duplicate Spark SQL jobs.
        """
        with self._lock:
            if key in self._cache:
                self._cache.move_to_end(key)
                self._hits += 1
                return self._cache[key]

            if key in self._in_flight:
                future = self._in_flight[key]
                is_leader = False
            else:
                future = Future()
                self._in_flight[key] = future
                is_leader = True
                self._misses += 1

        if not is_leader:
            # Await the leader thread's computation
            res = future.result()
            with self._lock:
                self._hits += 1
            return res

        # Leader thread computes the result
        try:
            val = compute_fn()
            with self._lock:
                self._cache[key] = val
                self._cache.move_to_end(key)
                if len(self._cache) > self._maxsize:
                    self._cache.popitem(last=False)
                    self._evictions += 1
                future.set_result(val)
            return val
        except Exception as exc:
            with self._lock:
                future.set_exception(exc)
            raise
        finally:
            with self._lock:
                self._in_flight.pop(key, None)

    def clear(self) -> None:
        """Invalidate all cached items and cancel pending in-flight futures."""
        with self._lock:
            self._cache.clear()
            self._in_flight.clear()

    def stats(self) -> dict[str, int]:
        """Return cache hit, miss, eviction, and current size statistics."""
        with self._lock:
            return {
                "hits": self._hits,
                "misses": self._misses,
                "evictions": self._evictions,
                "size": len(self._cache),
                "maxsize": self._maxsize,
            }

