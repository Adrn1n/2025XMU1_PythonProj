from typing import Any, Dict, List, Optional, Union
import logging
from pathlib import Path
import aiohttp
import asyncio
import random
import time

from utils.cache import URLCache
from utils.logging_utils import setup_logger


class BaseScraper:
    """Base Web Scraper class"""

    def __init__(
        self,
        headers: Dict[str, str],
        proxies: List[str] = None,
        use_proxy: bool = False,
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
        Initialize base scraper

        Args:
            headers: Complete request headers
            proxies: List of proxy servers
            use_proxy: Whether to use proxies for requests
            max_semaphore: Concurrent request limit
            batch_size: URL batch processing size
            timeout: Request timeout in seconds
            retries: Number of retry attempts for failed requests
            min_sleep: Minimum delay between requests
            max_sleep: Maximum delay between requests
            max_redirects: Maximum number of redirects to follow
            cache_size: Size of URL cache
            cache_ttl: Cache entry lifetime in seconds
            enable_logging: Whether to enable logging
            log_to_console: Whether to output logs to console
            log_level: Logging level
            log_file: Path to log file
        """
        self.headers = headers
        self.proxies = proxies or []
        self.use_proxy = use_proxy
        self.max_semaphore = max_semaphore
        self.batch_size = batch_size
        self.timeout = timeout
        self.retries = retries
        self.min_sleep = min_sleep
        self.max_sleep = max_sleep
        self.max_redirects = max_redirects
        self.url_cache = URLCache(max_size=cache_size, ttl=cache_ttl)
        self.logger = None

        # Initialize semaphore
        self.semaphore = asyncio.Semaphore(max_semaphore)

        # Initialize statistics
        self.stats = {
            "total": 0,
            "success": 0,
            "failed": 0,
            "start": None,
            "end": None,
        }

        if enable_logging:
            self.logger = setup_logger(
                self.__class__.__name__, log_level, log_file, log_to_console
            )
            self.logger.info(f"Initializing {self.__class__.__name__}")

    def get_stats(self) -> Dict[str, Any]:
        """Get scraper statistics"""
        stats = self.stats.copy()

        # Calculate runtime
        if stats["start"] and stats["end"]:
            stats["duration"] = stats["end"] - stats["start"]
        else:
            stats["duration"] = 0

        # Calculate success rate
        if stats["total"] > 0:
            stats["success_rate"] = stats["success"] / stats["total"]
        else:
            stats["success_rate"] = 0

        # Add cache statistics
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
        Get webpage content

        Args:
            url: Request URL
            params: URL parameters
            use_proxy: Whether to use proxy
            headers: Custom request headers
            timeout: Custom timeout
            retries: Custom retry attempts
            session: Optional client session

        Returns:
            Webpage content or None (if request failed)
        """
        # Use provided parameters or instance defaults
        use_proxy = use_proxy if use_proxy is not None else self.use_proxy
        headers = headers or self.headers
        timeout = timeout or self.timeout
        retries = retries if retries is not None else self.retries

        # Update statistics
        self.stats["total"] += 1
        if self.stats["start"] is None:
            self.stats["start"] = time.time()

        session_provided = session is not None
        if not session_provided:
            session = aiohttp.ClientSession()
        try:
            # Add random delay before request
            await asyncio.sleep(random.uniform(self.min_sleep, self.max_sleep))

            # Randomly select a proxy if needed and available
            proxy = random.choice(self.proxies) if self.proxies and use_proxy else None

            if self.logger:
                self.logger.debug(
                    f"【BASE】Sending request: {url}"
                    + (f" with params: {params}" if params else "")
                    + (f" via proxy: {proxy}" if proxy else "")
                )

            for attempt in range(retries + 1):
                try:
                    async with session.get(
                        url,
                        headers=headers,
                        params=params,
                        proxy=proxy,
                        timeout=aiohttp.ClientTimeout(total=timeout),
                    ) as response:
                        if response.status != 200:
                            if self.logger:
                                self.logger.error(
                                    f"Request failed, status code: {response.status}"
                                )

                            # Update failure statistics on final attempt
                            if attempt == retries:
                                self.stats["failed"] += 1

                            # Some status codes don't warrant retries
                            if response.status in (404, 403):
                                return None

                            # For other status codes, retry if attempts remain
                            if attempt < retries:
                                sleep_time = random.uniform(
                                    self.min_sleep, self.max_sleep
                                )
                                if self.logger:
                                    self.logger.debug(
                                        f"【BASE】Retrying ({attempt+1}/{retries}) waiting {sleep_time:.2f}s"
                                    )
                                await asyncio.sleep(sleep_time)
                                continue

                            return None

                        text = await response.text()
                        if self.logger:
                            self.logger.debug(f"Response length: {len(text)} bytes")

                        # Update success statistics
                        self.stats["success"] += 1
                        self.stats["end"] = time.time()

                        return text

                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    if attempt == retries:
                        if self.logger:
                            self.logger.error(f"Error during request: {str(e)}")
                        self.stats["failed"] += 1
                        return None

                    sleep = random.uniform(self.min_sleep, self.max_sleep)
                    if self.logger:
                        self.logger.debug(
                            f"Retrying ({attempt+1}/{retries}) waiting {sleep:.2f}s, error: {str(e)}"
                        )
                    await asyncio.sleep(sleep)

            return None  # If all retries fail
        finally:
            if not session_provided:
                await session.close()
