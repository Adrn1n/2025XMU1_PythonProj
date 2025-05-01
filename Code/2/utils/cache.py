from typing import Any, Dict, Optional, Tuple
import logging
import time
from pathlib import Path
import json


class URLCache:
    """
    In-memory cache for storing resolved URLs (original URL -> final URL).
    Includes Time-To-Live (TTL) expiration and size limits with basic eviction.
    Supports saving to and loading from a JSON file.
    """

    def __init__(
        self,
        max_size: int = 1000,
        ttl: int = 24 * 60 * 60,  # Default TTL: 24 hours in seconds
        cleanup_threshold: int = 100,  # Trigger cleanup every N operations
    ):
        """
        Initialize the URL cache.

        Args:
            max_size: Maximum number of entries the cache can hold.
            ttl: Default time-to-live for cache entries in seconds.
            cleanup_threshold: Number of get/set operations before checking for expired items.
        """
        # Cache format: { original_url: (resolved_url, timestamp) }
        self.cache: Dict[str, Tuple[str, float]] = {}
        self.max_size = max_size
        self.ttl = ttl
        self.cleanup_threshold = cleanup_threshold
        # Statistics tracking
        self.hits = 0
        self.misses = 0
        self.operations_count = 0  # Counter to trigger periodic cleanup
        self.logger = logging.getLogger("URLCache")  # Logger for cache operations

    def stats(self) -> Dict[str, Any]:
        """Return current statistics about the cache."""
        total_reqs = self.hits + self.misses
        hit_rate = self.hits / total_reqs if total_reqs > 0 else 0

        return {
            "size": len(self.cache),  # Current number of items
            "max_size": self.max_size,  # Configured maximum size
            "ttl": self.ttl,  # Configured TTL
            "hits": self.hits,  # Number of times an item was found in cache
            "misses": self.misses,  # Number of times an item was not found (or expired)
            "hit_rate": hit_rate,  # Proportion of hits vs total requests
        }

    def clean_expired(self) -> None:
        """Remove all expired items from the cache based on their timestamp and TTL."""
        now = time.time()
        # Identify keys of expired items
        expired_keys = [
            k for k, (_, timestamp) in self.cache.items() if now - timestamp > self.ttl
        ]

        # Remove expired items
        for key in expired_keys:
            try:
                del self.cache[key]
            except KeyError:
                pass  # Should not happen if key was just retrieved, but handle defensively

        if expired_keys and self.logger:
            self.logger.debug(f"Cleaned {len(expired_keys)} expired cache items.")

    def maybe_clean_expired(self) -> None:
        """Trigger cleaning of expired items periodically based on operations count."""
        # Check and clean only after a certain number of operations
        if self.operations_count >= self.cleanup_threshold:
            self.clean_expired()
            self.operations_count = 0  # Reset counter after cleaning

    def evict_entries(self, max_percent=10) -> None:
        """
        Evict (remove) a portion of the oldest entries to make space when the cache is full.
        Evicts a percentage of the cache size or at least 1 item.
        """
        if not self.cache:
            return  # Nothing to evict

        # Sort items by timestamp (oldest first)
        items = list(self.cache.items())
        items.sort(key=lambda item: item[1][1])

        # Calculate number of items to remove (at least 1)
        num_to_remove = max(1, len(items) * max_percent // 100)
        keys_to_remove = [key for key, _ in items[:num_to_remove]]

        # Remove the selected items
        for key in keys_to_remove:
            try:
                del self.cache[key]
            except KeyError:
                pass  # Ignore if key somehow already removed

        if self.logger:
            self.logger.warning(
                f"Cache full (size {len(self.cache)} >= {self.max_size}). Evicted {len(keys_to_remove)} oldest items."
            )

    def set(self, org_url: str, real_url: str) -> None:
        """
        Add or update an entry in the cache.

        Args:
            org_url: The original URL (key).
            real_url: The resolved final URL (value).
        """
        if not org_url:
            return  # Do not cache empty keys

        self.operations_count += 1
        self.maybe_clean_expired()  # Perform cleanup if threshold reached

        # Check if cache is full *before* adding the new item
        if len(self.cache) >= self.max_size and org_url not in self.cache:
            self.evict_entries()  # Make space if needed

        # Add/update the entry with the current timestamp
        self.cache[org_url] = (real_url, time.time())
        if self.logger:
            self.logger.debug(f"[URL_CACHE]: Set cache: {org_url} -> {real_url}")

    def get(self, url: str) -> Optional[str]:
        """
        Retrieve a resolved URL from the cache. Returns None if not found or expired.

        Args:
            url: The original URL to look up.

        Returns:
            The cached resolved URL string, or None if not found or expired.
        """
        if not url:
            return None  # Cannot get empty key

        self.operations_count += 1
        self.maybe_clean_expired()  # Perform cleanup if threshold reached

        cache_entry = self.cache.get(url)

        if cache_entry:
            try:
                cached_url, timestamp = cache_entry
                # Check if the entry is still within its TTL
                if time.time() - timestamp <= self.ttl:
                    self.hits += 1  # Increment hit counter
                    if self.logger:
                        self.logger.debug(f"[URL_CACHE]: Cache hit for {url}")
                    return cached_url
                else:
                    # Entry has expired, remove it from cache
                    if self.logger:
                        self.logger.debug(f"[URL_CACHE]: Cache expired for {url}")
                    del self.cache[url]
                    self.misses += 1  # Count expired entry as a miss
                    return None
            except (TypeError, ValueError, KeyError) as e:
                # Handle potential malformed entries or errors during unpacking/deletion
                if self.logger:
                    self.logger.warning(
                        f"[URL_CACHE]: Error processing cache entry for {url}: {e}. Removing entry."
                    )
                try:
                    del self.cache[url]
                except KeyError:
                    pass  # Already removed or never existed properly
                self.misses += 1
                return None

        # URL not found in cache
        self.misses += 1
        if self.logger:
            self.logger.debug(f"[URL_CACHE]: Cache miss for {url}")
        return None

    def clear(self) -> None:
        """Remove all items from the cache and reset statistics."""
        self.cache.clear()
        self.hits = 0
        self.misses = 0
        self.operations_count = 0  # Reset operations counter as well
        if self.logger:
            self.logger.info("[URL_CACHE]: Cache cleared.")

    def save_to_file(self, file_path: Path) -> bool:
        """
        Save the current state of the cache (non-expired items) to a JSON file.

        Args:
            file_path: The Path object representing the file to save to.

        Returns:
            True if saving was successful, False otherwise.
        """
        try:
            # Clean expired items before saving to avoid saving stale data
            self.clean_expired()

            # Prepare cache data for JSON serialization
            serializable_cache = {
                url: {"real_url": real_url, "timestamp": timestamp}
                for url, (real_url, timestamp) in self.cache.items()
            }

            # Ensure parent directory exists
            file_path.parent.mkdir(parents=True, exist_ok=True)

            # Write the serializable data to the file
            with file_path.open("w", encoding="utf-8") as f:
                json.dump(serializable_cache, f, ensure_ascii=False, indent=2)

            if self.logger:
                self.logger.info(
                    f"Saved {len(serializable_cache)} cache items to {file_path}"
                )
            return True

        except Exception as e:
            if self.logger:
                self.logger.error(
                    f"Failed to save cache to file {file_path}: {e}", exc_info=True
                )
            return False

    def load_from_file(self, file_path: Path) -> bool:
        """
        Load cache state from a JSON file, replacing current cache content.
        Only loads non-expired items based on the current time and stored timestamps.

        Args:
            file_path: The Path object representing the file to load from.

        Returns:
            True if loading was successful, False otherwise.
        """
        try:
            if not file_path.exists():
                if self.logger:
                    self.logger.warning(
                        f"Cache file not found, cannot load: {file_path}"
                    )
                return False  # Indicate file not found, but not necessarily an error

            # Read the JSON data from the file
            with file_path.open("r", encoding="utf-8") as f:
                data = json.load(f)

            # Clear the current cache before loading
            self.cache.clear()
            loaded_count = 0
            expired_count = 0
            malformed_count = 0
            now = time.time()

            # Load data, converting back to internal format and checking TTL
            for url, item in data.items():
                try:
                    # Validate item structure
                    if (
                        isinstance(item, dict)
                        and "real_url" in item
                        and "timestamp" in item
                    ):
                        real_url = item["real_url"]
                        timestamp = item["timestamp"]

                        # Only load if the item is not expired
                        if now - timestamp <= self.ttl:
                            # Check if cache is full before adding
                            if len(self.cache) < self.max_size:
                                self.cache[url] = (real_url, timestamp)
                                loaded_count += 1
                            else:
                                # Stop loading if cache becomes full during load
                                if self.logger:
                                    self.logger.warning(
                                        f"Cache full while loading from {file_path}. Stopped loading."
                                    )
                                break  # Exit the loop early
                        else:
                            expired_count += 1  # Count expired items not loaded
                    else:
                        malformed_count += 1
                        if self.logger:
                            self.logger.warning(
                                f"Skipping malformed item in cache file for URL: {url}"
                            )

                except Exception as item_error:
                    malformed_count += 1
                    if self.logger:
                        self.logger.warning(
                            f"Error processing item for URL {url} from cache file: {item_error}"
                        )

            if self.logger:
                log_msg = f"Loaded {loaded_count} items from {file_path}."
                if expired_count > 0:
                    log_msg += f" Skipped {expired_count} expired items."
                if malformed_count > 0:
                    log_msg += f" Skipped {malformed_count} malformed items."
                self.logger.info(log_msg)
            return True

        except json.JSONDecodeError as e:
            if self.logger:
                self.logger.error(f"Failed to parse cache file {file_path}: {e}")
            return False
        except Exception as e:
            if self.logger:
                self.logger.error(
                    f"Failed to load cache from file {file_path}: {e}", exc_info=True
                )
            return False
