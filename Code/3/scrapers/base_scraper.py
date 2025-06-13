from typing import Any, Dict, List, Optional, Union
import logging
from pathlib import Path
import aiohttp
import asyncio
import random
import time

from utils.cache import URLCache
from utils.logging_utils import setup_logger, setup_module_logger


class BaseScraper:
    """Provides common functionalities for web scraping tasks."""

    def __init__(
        self,
        headers: Dict[str, str],
        proxies: List[str] = None,
        use_proxy: bool = False,
        max_concurrent_pages: int = 5,  # Note: Primarily used by subclasses like BaiduScraper
        max_semaphore: int = 25,
        batch_size: int = 25,
        timeout: int = 3,
        retries: int = 0,
        min_sleep: float = 0.1,
        max_sleep: float = 0.3,
        max_redirects: int = 5,
        cache_size: int = 1000,
        cache_ttl: int = 24
        * 60
        * 60,  # Cache Time-To-Live in seconds (default: 24 hours)
        enable_logging: bool = False,
        log_to_console: bool = True,
        log_level: int = logging.INFO,
        log_file: Optional[Union[str, Path]] = None,
    ):
        """
        Initialize the base scraper.

        Args:
            headers: Default HTTP headers for requests.
            proxies: List of proxy server URLs (e.g., 'http://user:pass@host:port').
            use_proxy: Whether to use proxies from the list for requests.
            max_concurrent_pages: Max pages to scrape concurrently (informational for subclasses).
            max_semaphore: Global limit for concurrent network requests managed by this instance.
            batch_size: Default size for batch processing operations (e.g., URL resolution).
            timeout: Default request timeout in seconds.
            retries: Default number of retry attempts for failed requests.
            min_sleep: Minimum random delay (seconds) before making a request.
            max_sleep: Maximum random delay (seconds) before making a request.
            max_redirects: Default maximum number of redirects to follow.
            cache_size: Maximum number of entries in the URL cache.
            cache_ttl: Lifetime of entries in the URL cache (seconds).
            enable_logging: If True, set up a logger for this scraper instance.
            log_to_console: If logging is enabled, whether to output logs to the console.
            log_level: Logging level (e.g., logging.INFO, logging.DEBUG).
            log_file: Path to the log file if logging to file is desired.
        """
        self.headers = headers
        self.proxies = proxies or []
        self.use_proxy = use_proxy
        self.max_concurrent_pages = (
            max_concurrent_pages  # Stored but primarily used by subclasses
        )
        self.max_semaphore = max_semaphore
        self.batch_size = batch_size
        self.timeout = timeout
        self.retries = retries
        self.min_sleep = min_sleep
        self.max_sleep = max_sleep
        self.max_redirects = max_redirects
        # Initialize the URL cache
        self.url_cache = URLCache(max_size=cache_size, ttl=cache_ttl)
        self.logger = None

        # Initialize an asyncio Semaphore to limit concurrent requests
        self.semaphore = asyncio.Semaphore(max_semaphore)

        # Initialize dictionary to store request statistics
        self.stats: Dict[str, Any] = {
            "total": 0,  # Total requests initiated
            "success": 0,  # Successful requests (status 200)
            "failed": 0,  # Failed requests (non-200 status or exceptions after retries)
            "start": None,  # Timestamp of the first request
            "end": None,  # Timestamp of the last successful request
        }

        # Set up logging if enabled
        if enable_logging:
            # Try to get config files for module-specific logging
            try:
                from config import files

                self.logger = setup_module_logger(
                    self.__class__.__name__,  # Use the class name as the logger name
                    log_level,
                    files,
                    log_to_console,
                    propagate=False,  # Prevent cross-contamination between modules
                )
            except ImportError:
                # Fallback to standard setup if config is not available
                self.logger = setup_logger(
                    self.__class__.__name__,
                    log_level,
                    log_file,
                    log_to_console,
                )
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
