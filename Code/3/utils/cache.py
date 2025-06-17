"""
In-memory URL cache with TTL expiration and file persistence.
Provides efficient caching for URL resolution with automatic cleanup.
"""

import json
import logging
import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple


class URLCache:
    """
    In-memory cache for URL resolution with TTL and size limits.
    Supports persistence to JSON files for cache recovery.
    """

    def __init__(
        self,
        max_size: int = 1000,
        ttl: int = 24 * 60 * 60,
        cleanup_threshold: int = 100,
    ):
        """
        Initialize URL cache with size and time limits.

        Args:
            max_size: Maximum number of cache entries
            ttl: Time-to-live for entries in seconds
            cleanup_threshold: Operations before cleanup check
        """
        self.cache: Dict[str, Tuple[str, float]] = {}
        self.max_size = max_size
        self.ttl = ttl
        self.cleanup_threshold = cleanup_threshold
        self.hits = 0
        self.misses = 0
        self.operations_count = 0

        self.logger = logging.getLogger("URLCache")
        self._upgrade_logger()

    def _upgrade_logger(self):
        """Upgrade to module-specific logger if available."""
        try:
            from config import get_logger
            # 使用自动检测，不传递字符串参数
            self.logger = get_logger()
        except ImportError:
            # 保持原有的简单logger
            pass

    def stats(self) -> Dict[str, Any]:
        """Return current cache statistics."""
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
        """Remove all expired items from the cache based on their timestamp and TTL."""
        now = time.time()
        # Identify keys of expired items
        expired_keys = []
        for k, cache_entry in self.cache.items():
            _, timestamp = cache_entry
            if now - timestamp > self.ttl:
                expired_keys.append(k)

        # Remove expired items
        for key in expired_keys:
            del self.cache[key]

        if expired_keys and self.logger:
            self.logger.debug(f"Cleaned {len(expired_keys)} expired cache entries")

    def maybe_clean_expired(self) -> None:
        """Trigger cleaning of expired items periodically based on operations count."""
        # Check and clean only after a certain number of operations
        if self.operations_count >= self.cleanup_threshold:
            self.clean_expired()
            self.operations_count = 0  # Reset the counter

    def evict_entries(self, max_percent=10) -> None:
        """
        Evict (remove) a portion of the oldest entries to make space when the cache is full.
        Evicts a percentage of the cache size or at least 1 item.
        """
        if not self.cache:
            return  # Nothing to evict

        # Sort items by timestamp (oldest first)
        items = []
        for key, entry in self.cache.items():
            _, timestamp = entry
            items.append((key, timestamp))

        # Sort by timestamp (oldest first)
        items.sort(key=lambda item: item[1])

        # Calculate number of items to remove (at least 1)
        num_to_remove = max(1, len(items) * max_percent // 100)
        keys_to_remove = [key for key, _ in items[:num_to_remove]]

        # Remove the selected items
        for key in keys_to_remove:
            del self.cache[key]

        if keys_to_remove and self.logger:
            self.logger.debug(f"Evicted {len(keys_to_remove)} oldest cache entries")

    def set(self, org_url: str, real_url: str) -> None:
        """
        Add or update an entry in the cache.

        Args:
            org_url: The original URL (key).
            real_url: The resolved final URL (value).
        """
        if not org_url:
            return  # Skip empty URLs

        self.operations_count += 1
        self.maybe_clean_expired()  # Perform cleanup if threshold reached

        # Check if cache is full *before* adding the new item
        if len(self.cache) >= self.max_size and org_url not in self.cache:
            self.evict_entries()  # Make space by removing oldest entries

        # Add the new entry with current timestamp
        self.cache[org_url] = (real_url, time.time())

    def get(self, url: str) -> Optional[str]:
        """
        Retrieve a cached URL mapping.

        Args:
            url: The original URL to look up.

        Returns:
            The cached resolved URL if found and not expired, None otherwise.
        """
        if not url or url not in self.cache:
            self.misses += 1
            return None

        self.operations_count += 1
        self.maybe_clean_expired()  # Perform cleanup if threshold reached

        # Extract the cached data
        real_url, timestamp = self.cache[url]

        # Check if the entry has expired
        if time.time() - timestamp > self.ttl:
            # Remove the expired entry
            del self.cache[url]
            self.misses += 1
            return None

        # Update the entry's timestamp to keep frequently accessed items fresh
        self.cache[url] = (real_url, time.time())
        self.hits += 1
        return real_url

    def clear(self) -> None:
        """Remove all items from the cache."""
        self.cache.clear()
        if self.logger:
            self.logger.debug("Cache cleared")

    def save_to_file(self, file_path: Path) -> bool:
        """
        Save the current cache to a JSON file.

        Args:
            file_path: Path to the output file.

        Returns:
            True if saving was successful, False otherwise.
        """
        try:
            # Convert cache to a serializable format
            # {url: (real_url, timestamp)} -> {url: {"url": real_url, "timestamp": timestamp}}
            serializable_cache = {}
            for url, (real_url, timestamp) in self.cache.items():
                serializable_cache[url] = {"url": real_url, "timestamp": timestamp}

            # Add metadata for easy verification when loading
            data_to_save = {
                "version": 1,
                "created": time.time(),
                "ttl": self.ttl,
                "max_size": self.max_size,
                "stats": self.stats(),
                "cache": serializable_cache,
            }

            # Ensure the directory exists
            file_path.parent.mkdir(parents=True, exist_ok=True)

            # Write to file
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(data_to_save, f, indent=2)

            if self.logger:
                self.logger.debug(f"Cache saved to {file_path}")
            return True

        except Exception as e:
            if self.logger:
                self.logger.error(f"Failed to save cache to {file_path}: {e}")
            return False

    def load_from_file(self, file_path: Path) -> bool:
        """
        Load cache data from a JSON file.

        Args:
            file_path: Path to the input file.

        Returns:
            True if loading was successful, False otherwise.
        """
        try:
            if not file_path.exists():
                if self.logger:
                    self.logger.warning(f"Cache file not found: {file_path}")
                return False

            # Read from file
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            # Check if the loaded data has the expected structure
            if not all(key in data for key in ["version", "cache"]):
                if self.logger:
                    self.logger.error(f"Invalid cache file format: {file_path}")
                return False

            # Use the saved TTL and max_size if they exist
            if "ttl" in data:
                self.ttl = data["ttl"]
            if "max_size" in data:
                self.max_size = data["max_size"]

            # Load the cache entries
            self.cache = {}
            for url, entry in data["cache"].items():
                if isinstance(entry, dict) and "url" in entry and "timestamp" in entry:
                    self.cache[url] = (entry["url"], entry["timestamp"])

            if self.logger:
                self.logger.debug(
                    f"Loaded {len(self.cache)} entries from cache file {file_path}"
                )
            return True

        except Exception as e:
            if self.logger:
                self.logger.error(f"Failed to load cache from {file_path}: {e}")
            return False
