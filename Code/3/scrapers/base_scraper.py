"""
Base web scraper providing common functionality for web scraping tasks.
Includes request handling, proxy support, caching, and rate limiting.
"""

import asyncio
import aiohttp
import logging
import random
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from utils.cache import URLCache
from utils.logging_utils import get_logger


class BaseScraper:
    """Base scraper providing common web scraping functionality."""

    def __init__(
        self,
        headers: Dict[str, str],
        proxies: List[str] = None,
        use_proxy: bool = False,
        max_concurrent_pages: int = 5,
        max_semaphore: int = 25,
        batch_size: int = 25,
        timeout: int = 3,
        retries: int = 0,
        min_sleep: float = 0.1,
        max_sleep: float = 0.3,
        max_redirects: int = 5,
        cache_size: int = 1000,
        cache_ttl: int = 24 * 60 * 60,
        enable_logging: bool = False,
        log_to_console: bool = True,
        log_level: int = logging.INFO,
        log_file: Optional[Union[str, Path]] = None,
    ):
        """
        Initialize base scraper with configuration options.

        Args:
            headers: Default HTTP headers for requests
            proxies: List of proxy server URLs
            use_proxy: Whether to use proxies for requests
            max_concurrent_pages: Maximum pages to scrape concurrently
            max_semaphore: Global limit for concurrent network requests
            batch_size: Default batch size for processing operations
            timeout: Default request timeout in seconds
            retries: Default number of retry attempts
            max_sleep: Maximum random delay before requests
            max_redirects: Maximum number of redirects to follow
            cache_size: Maximum entries in URL cache
            cache_ttl: Cache entry lifetime in seconds
            enable_logging: Whether to set up logger for this instance
            log_to_console: Whether to output logs to console
            log_level: Logging level
            log_file: Path to log file if desired
        """
        self.headers = headers
        self.proxies = proxies or []
        self.use_proxy = use_proxy
        self.max_concurrent_pages = max_concurrent_pages
        self.max_semaphore = max_semaphore
        self.batch_size = batch_size
        self.timeout = timeout
        self.retries = retries
        self.min_sleep = min_sleep
        self.max_sleep = max_sleep
        self.max_redirects = max_redirects
        self.url_cache = URLCache(max_size=cache_size, ttl=cache_ttl)
        self.logger = None

        self.semaphore = asyncio.Semaphore(max_semaphore)

        self.stats: Dict[str, Any] = {
            "total": 0,
            "success": 0,
            "failed": 0,
            "start": None,
            "end": None,
        }

        if enable_logging:
            from config import get_class_logger
            # 使用简化的类日志器，自动检测类名，无需传递任何字符串
            self.logger = get_class_logger(self)
            self.logger.info(f"Initializing {self.__class__.__name__}")

    def get_stats(self) -> Dict[str, Any]:
        """Calculate and return current scraper statistics."""
        stats = self.stats.copy()  # Work on a copy

        # Calculate total duration if scraping has started and ended
        if stats["start"] and stats["end"]:
            stats["duration"] = stats["end"] - stats["start"]
        elif stats["start"]:
            stats["duration"] = time.time() - stats["start"]  # Ongoing duration
        else:
            stats["duration"] = 0

        # Calculate success rate
        if stats["total"] > 0:
            stats["success_rate"] = stats["success"] / stats["total"]
        else:
            stats["success_rate"] = 0

        # Include statistics from the URL cache
        if hasattr(self, "url_cache"):
            stats["cache"] = self.url_cache.stats()

        return stats

    async def get_page(
        self,
        url: str,
        params: Dict[str, str] = None,
        use_proxy: bool = None,
        headers: Dict[str, str] = None,
        timeout: int = None,
        retries: int = None,
        session: Optional[aiohttp.ClientSession] = None,
    ) -> Optional[str]:
        """
        Asynchronously fetches the content of a single webpage.
        Handles proxies, retries, delays, and statistics updates.

        Args:
            url: The URL of the page to fetch.
            params: Optional dictionary of URL parameters.
            use_proxy: Override the instance's use_proxy setting for this request.
            headers: Override the instance's default headers for this request.
            timeout: Override the instance's default timeout for this request.
            retries: Override the instance's default retries for this request.
            session: An optional existing aiohttp.ClientSession to use. If None, a new one is created and closed.

        Returns:
            The HTML content of the page as a string, or None if the request failed after all retries.
        """
        # Determine effective settings, prioritizing arguments over instance defaults
        effective_use_proxy = use_proxy if use_proxy is not None else self.use_proxy
        effective_headers = headers or self.headers
        effective_timeout = timeout or self.timeout
        effective_retries = retries if retries is not None else self.retries

        # Update statistics: increment total requests and set start time if first request
        self.stats["total"] += 1
        if self.stats["start"] is None:
            self.stats["start"] = time.time()

        # Manage the aiohttp session: use provided or create/close a temporary one
        session_provided = session is not None
        if not session_provided:
            session = aiohttp.ClientSession()

        try:
            # Introduce a random delay before making the request to be polite
            await asyncio.sleep(random.uniform(self.min_sleep, self.max_sleep))

            # Select a proxy randomly if proxy usage is enabled and proxies are available
            proxy_url = (
                random.choice(self.proxies)
                if self.proxies and effective_use_proxy
                else None
            )

            if self.logger:
                log_msg = f"[BASE]: Sending request to: {url}"
                if params:
                    log_msg += f" with params: {params}"
                if proxy_url:
                    log_msg += f" via proxy: {proxy_url}"
                self.logger.debug(log_msg)

            # Retry loop: attempt the request up to 'effective_retries' + 1 times
            for attempt in range(effective_retries + 1):
                try:
                    # Make the GET request using the session
                    async with session.get(
                        url,
                        headers=effective_headers,
                        params=params,
                        proxy=proxy_url,  # Pass proxy URL directly to aiohttp
                        timeout=aiohttp.ClientTimeout(total=effective_timeout),
                        allow_redirects=True,  # Allow aiohttp to handle redirects by default
                        max_redirects=self.max_redirects,  # Limit redirects
                    ) as response:
                        # Check if the request was successful (HTTP 200 OK)
                        if response.status != 200:
                            if self.logger:
                                self.logger.warning(  # Use warning for non-200 status
                                    f"[BASE]: Request failed for {url} (Attempt {attempt+1}/{effective_retries+1}) - Status: {response.status}"
                                )

                            # If this was the last attempt, mark as failed
                            if attempt == effective_retries:
                                self.stats["failed"] += 1
                                self.stats["end"] = (
                                    time.time()
                                )  # Update end time even on failure

                            # Decide whether to retry based on status code and remaining attempts
                            # Don't retry for common 'not found' or 'forbidden' errors
                            if (
                                response.status in (404, 403)
                                or attempt == effective_retries
                            ):
                                return None  # Stop retrying

                            # Wait before retrying for other error statuses
                            sleep_time = random.uniform(
                                self.min_sleep, self.max_sleep
                            ) * (
                                attempt + 1
                            )  # Exponential backoff factor
                            if self.logger:
                                self.logger.debug(
                                    f"[BASE]: Retrying ({attempt+1}/{effective_retries}) after {sleep_time:.2f}s due to status {response.status}"
                                )
                            await asyncio.sleep(sleep_time)
                            continue  # Go to the next attempt

                        # Request was successful (status 200)
                        text = await response.text()  # Read the response body as text
                        if self.logger:
                            self.logger.debug(
                                f"[BASE]: Successfully fetched {url} ({len(text)} bytes)"
                            )

                        # Update success statistics
                        self.stats["success"] += 1
                        self.stats["end"] = time.time()  # Update end time on success

                        return text  # Return the page content

                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    # Handle network errors or timeouts
                    if self.logger:
                        self.logger.warning(  # Use warning for potentially transient errors
                            f"[BASE]: Request error for {url} (Attempt {attempt+1}/{effective_retries+1}): {type(e).__name__} - {str(e)}"
                        )

                    # If this was the last attempt, mark as failed
                    if attempt == effective_retries:
                        self.stats["failed"] += 1
                        self.stats["end"] = time.time()
                        return None  # Stop retrying

                    # Wait before retrying
                    sleep_time = random.uniform(self.min_sleep, self.max_sleep) * (
                        attempt + 1
                    )
                    if self.logger:
                        self.logger.debug(
                            f"[BASE]: Retrying ({attempt+1}/{effective_retries}) after {sleep_time:.2f}s due to error: {str(e)}"
                        )
                    await asyncio.sleep(sleep_time)
                    # Continue to the next attempt in the loop

            # Should not be reached if loop completes correctly, but acts as a fallback
            return None
        finally:
            # Close the session only if it was created within this method
            if not session_provided and session:
                await session.close()
