from typing import Any, Dict, Optional, Tuple
import logging
import time
from pathlib import Path
import json


class URLCache:
    """Efficient URL resolution result cache class"""

    def __init__(
        self,
        max_size: int = 1000,
        ttl: int = 24 * 60 * 60,
        cleanup_threshold: int = 100,
    ):
        """
        Initialize cache

        Args:
            max_size: Maximum number of cache items
            ttl: Cache item lifetime (seconds)
            cleanup_threshold: How many operations before triggering cleanup
        """
        self.cache: Dict[str, Tuple[str, float]] = {}  # (url, timestamp)
        self.max_size = max_size
        self.ttl = ttl
        self.cleanup_threshold = cleanup_threshold
        self.hits = 0
        self.misses = 0
        self.operations_count = 0
        self.logger = logging.getLogger("URLCache")

    def stats(self) -> Dict[str, Any]:
        """Return cache statistics"""
        total_reqs = self.hits + self.misses
        hit_rate = self.hits / total_reqs if total_reqs > 0 else 0

        return {
            "size": len(self.cache),
            "max_size": self.max_size,
            "ttl": self.ttl,
            "hits": self.hits,
            "misses": self.misses,
            "hit_rate": hit_rate,
        }

    def clean_expired(self) -> None:
        """Clean expired cache items"""
        now = time.time()
        expired_keys = []

        for k, cache_entry in self.cache.items():
            try:
                if isinstance(cache_entry, tuple) and len(cache_entry) == 2:
                    _, timestamp = cache_entry
                    if now - timestamp > self.ttl:
                        expired_keys.append(k)
                else:
                    # For malformatted items, treat as expired
                    if self.logger:
                        self.logger.debug(
                            f"Found malformatted cache item: {k} -> {cache_entry}, marking as expired"
                        )
                    expired_keys.append(k)
            except Exception as e:
                if self.logger:
                    self.logger.warning(f"Error processing cache item: {e}, marking as expired: {k}")
                expired_keys.append(k)

        for key in expired_keys:
            del self.cache[key]

        if expired_keys and self.logger:
            self.logger.debug(f"Cleaned {len(expired_keys)} expired cache items")

    def maybe_clean_expired(self) -> None:
        """Clean expired cache items as needed"""
        # Clean once every cleanup_threshold operations
        if self.operations_count >= self.cleanup_threshold:
            self.clean_expired()
            self.operations_count = 0

    def evict_entries(self, max_percent=20) -> None:
        """Delete some cache items to free up space"""
        if not self.cache:
            return

        items = list(self.cache.items())
        items.sort(key=lambda x: x[1][1])  # Sort by timestamp

        to_remove = items[: max(1, len(items) * max_percent // 100)]

        for key, _ in to_remove:
            del self.cache[key]

        if self.logger:
            self.logger.debug(f"Cache space insufficient, removed {len(to_remove)} oldest cache items")

    def set(self, org_url: str, real_url: str) -> None:
        """Set cache"""
        self.operations_count += 1
        self.maybe_clean_expired()

        # Check if cache is full
        if len(self.cache) >= self.max_size:
            self.evict_entries()

        self.cache[org_url] = (real_url, time.time())
        if self.logger:
            self.logger.debug(f"【URL_CACHE】Set cache for URL: {org_url}")

    def get(self, url: str) -> Optional[str]:
        """Get cached URL"""
        self.operations_count += 1
        self.maybe_clean_expired()

        if url in self.cache:
            cached_url, timestamp = self.cache[url]
            # Check if expired
            if time.time() - timestamp <= self.ttl:
                self.hits += 1
                return cached_url
            else:
                # Expired, delete and return None
                del self.cache[url]

        self.misses += 1
        return None

    def clear(self) -> None:
        """Clear cache"""
        self.cache.clear()
        self.hits = 0
        self.misses = 0
        if self.logger:
            self.logger.debug("【URL_CACHE】Cache cleared")

    def save_to_file(self, file_path: Path) -> bool:
        """Save cache to file"""
        try:
            # Only save non-expired cache items
            self.clean_expired()

            # Convert cache to serializable format
            serializable_cache = {}
            for url, cache_entry in self.cache.items():
                # Handle cases where cache item might be a tuple or directly a URL string
                if isinstance(cache_entry, tuple) and len(cache_entry) == 2:
                    real_url, timestamp = cache_entry
                    serializable_cache[url] = {
                        "real_url": real_url,
                        "timestamp": timestamp,
                    }
                elif isinstance(cache_entry, str):
                    # If cache item is a string, use it directly as real_url and use current time as timestamp
                    serializable_cache[url] = {
                        "real_url": cache_entry,
                        "timestamp": time.time(),
                    }
                else:
                    # Skip unrecognized formats
                    if self.logger:
                        self.logger.warning(
                            f"Skipping unrecognized cache item format: {url} -> {cache_entry}"
                        )
                    continue

            with file_path.open("w", encoding="utf-8") as f:
                json.dump(serializable_cache, f, ensure_ascii=False, indent=2)

            if self.logger:
                self.logger.info(
                    f"Saved {len(serializable_cache)} cache items to {file_path}"
                )
            return True

        except Exception as e:
            if self.logger:
                self.logger.error(f"Failed to save cache to file: {e}")
            return False

    def load_from_file(self, file_path: Path) -> bool:
        """Load cache from file"""
        try:
            if not file_path.exists():
                if self.logger:
                    self.logger.warning(f"Cache file does not exist: {file_path}")
                return False

            with file_path.open("r", encoding="utf-8") as f:
                data = json.load(f)

            # Convert back to internal format and filter expired items
            now = time.time()
            loaded_count = 0

            for url, item in data.items():
                real_url = item["real_url"]
                timestamp = item["timestamp"]

                # Only load non-expired items
                if now - timestamp <= self.ttl:
                    self.cache[url] = (real_url, timestamp)
                    loaded_count += 1

            if self.logger:
                self.logger.info(
                    f"Loaded {loaded_count} cache items from {file_path} (skipped {len(data) - loaded_count} expired items)"
                )
            return True

        except json.JSONDecodeError as e:
            if self.logger:
                self.logger.error(f"Failed to parse cache file: {e}")
            return False
        except Exception as e:
            if self.logger:
                self.logger.error(f"Failed to load cache file: {e}")
            return False
