"""
Baidu search results scraper with advertisement filtering and URL resolution.
Provides specialized extraction methods for Baidu search result pages.
"""

import asyncio
import aiohttp
import logging
import random
import time
from bs4 import BeautifulSoup
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from scrapers.base_scraper import BaseScraper
from utils.url_utils import batch_fetch_real_urls


class BaiduScraper(BaseScraper):
    """Scraper specifically designed for Baidu search result pages."""

    # CSS selectors for extracting elements from Baidu's HTML structure
    TITLE_SELECTORS = [
        "h3[class*='title']",
        "h3[class*='t']",
    ]
    CONTENT_SELECTORS = [
        "div[class*='desc']",
        "div[class*='text']",
        "span[class*='content-right']",
        "span[class*='text']",
    ]
    SOURCE_SELECTORS = [
        "div[class*='showurl'], div[class*='source-text']",
        "span[class*='showurl'], span[class*='source-text'], span.c-color-gray",
    ]
    TIME_SELECTORS = [
        "span[class*='time']",
        "span.c-color-gray2",
        "span.n2n9e2q",
    ]
    RELATED_CONTENT_SELECTORS = [
        "div[class*=text], div[class*=abs], div[class*=desc], div[class*=content]",
        "p[class*=text], p[class*=desc], p[class*=content]",
        "span[class*=text], span[class*=desc], span[class*=content], span[class*=clamp]",
    ]
    RELATED_SOURCE_SELECTORS = [
        "span[class*=small], span[class*=showurl], span[class*=source-text], span[class*=site-name]",
        "div[class*=source-text], div[class*=showurl], div[class*=small]",
    ]

    # Advertisement detection patterns
    AD_STYLE_KEYWORDS = ["!important"]
    AD_CLASS_KEYWORDS = ["tuiguang"]
    AD_TAG_SELECTORS = ["[class*='tuiguang']"]

    def __init__(
        self,
        headers: Dict[str, str],
        proxies: List[str] = None,
        filter_ads: bool = True,
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
        Initialize BaiduScraper with configuration options.

        Args:
            headers: HTTP headers for requests
            proxies: List of proxy servers
            filter_ads: Whether to filter advertisement results
            use_proxy: Whether to use proxy servers
            max_concurrent_pages: Maximum pages to scrape concurrently
            max_semaphore: Global request semaphore limit
            batch_size: Batch size for URL processing
            timeout: Request timeout in seconds
            retries: Number of retry attempts
            min_sleep: Minimum delay between requests
            max_sleep: Maximum delay between requests
            max_redirects: Maximum redirects to follow
            cache_size: Maximum cache entries
            cache_ttl: Cache time-to-live in seconds
            enable_logging: Whether to enable logging
            log_to_console: Whether to log to console
            log_level: Logging level
            log_file: Optional log file path
        """
        super().__init__(
            headers=headers,
            proxies=proxies,
            use_proxy=use_proxy,
            max_semaphore=max_semaphore,
            batch_size=batch_size,
            timeout=timeout,
            retries=retries,
            min_sleep=min_sleep,
            max_sleep=max_sleep,
            max_redirects=max_redirects,
            cache_size=cache_size,
            cache_ttl=cache_ttl,
            enable_logging=enable_logging,
            log_to_console=log_to_console,
            log_level=log_level,
            log_file=log_file,
        )
        self.filter_ads = filter_ads
        self.max_concurrent_pages = max_concurrent_pages

    def extract_main_title_and_link(self, result) -> Tuple[str, str]:
        """Extract the main title text and link URL from a single search result block."""
        for selector in self.TITLE_SELECTORS:
            title_tag = result.select_one(selector)
            if title_tag:
                a_tag = title_tag.find(
                    "a"
                )  # The link is usually within an <a> tag inside the title <h3>
                if a_tag:
                    title = a_tag.get_text(strip=True)
                    url = a_tag.get("href", "")  # Get the raw URL (might be a redirect)
                    return title, url
        return "", ""  # Return empty strings if not found

    def extract_main_content(self, result) -> str:
        """Extract the content summary/description text from a single search result block."""
        for selector in self.CONTENT_SELECTORS:
            element = result.select_one(selector)
            if element:
                return element.get_text(strip=True)
        return ""

    def extract_main_source(self, result) -> str:
        """
        Extract the source information (e.g., display URL or site name).
        Returns an empty string if multiple potential sources are found to avoid ambiguity.
        """
        for selector in self.SOURCE_SELECTORS:
            elements = result.select(selector)
            if elements:
                # Only return if exactly one match is found
                return elements[0].get_text(strip=True) if len(elements) == 1 else ""
        return ""

    def extract_time(self, result) -> str:
        """
        Extract the timestamp information (e.g., date).
        Returns an empty string if multiple potential timestamps are found.
        Includes special handling for ambiguous selectors like 'span.c-color-gray2'.
        """
        for selector in self.TIME_SELECTORS:
            elements = result.select(selector)
            if not elements:
                continue

            # Special handling for 'span.c-color-gray2' which can be ambiguous
            if selector == "span.c-color-gray2":
                # Filter elements that *only* have the class 'c-color-gray2'
                # This helps distinguish it from other uses of the class (e.g., in source)
                filtered_elements = [
                    el
                    for el in elements
                    if len(el.attrs) == 1 and el.get("class") == ["c-color-gray2"]
                ]
                if len(filtered_elements) == 1:
                    return filtered_elements[0].get_text(strip=True)
                elif len(filtered_elements) > 1:
                    return ""  # Ambiguous, return empty
            # Standard logic for other time selectors
            else:
                if len(elements) == 1:
                    return elements[0].get_text(strip=True)
                elif len(elements) > 1:
                    return ""  # Ambiguous, return empty

        return ""

    @staticmethod
    def find_link_container(link_tag, result):
        """
        Attempt to find the logical container element for a related/sub-link within a main result block.
        This helps associate content/source/time with the correct sub-link.
        Traverses up the DOM tree from the link tag.
        """
        container = None
        current = link_tag.parent
        # Traverse upwards until the main result block is reached
        while current and current != result:
            # Prioritize divs with common container-like class names
            if current.name == "div" and current.get("class"):
                class_str = " ".join(current.get("class", []))
                if any(
                    kw in class_str.lower()
                    for kw in ["item", "container", "result", "sitelink"]
                ):
                    container = current
                    break  # Found a likely container
            current = current.parent

        # Fallback: If no specific container is found, use the nearest parent div
        if not container:
            current = link_tag.parent
            while current and current != result:
                if current.name == "div":
                    container = current
                    break
                current = current.parent

        return container

    @staticmethod
    def extract_from_container(container, selectors):
        """Extract the first matching text using a list of selectors within a given container."""
        if not container:
            return ""

        for selector in selectors:
            elements = container.select(selector)
            if elements:
                # Return the text of the first element found
                return elements[0].get_text(strip=True)
        return ""

    def extract_related_links(
        self, result, main_links: List[str]
    ) -> List[Dict[str, Any]]:
        """
        Extract related/sub-links found within a main search result block.
        Excludes links already identified as the main link for this result.
        Attempts to find associated content, source, and time for each related link.
        """
        related_links = []

        # Find all <a> tags within the current result block
        for link_tag in result.find_all("a"):
            href = link_tag.get("href", "")
            title = link_tag.get_text(strip=True)

            # Skip if it's one of the main links or if it lacks a URL or title text
            if not href or not title or href in main_links:
                continue

            # Try to find the container element for this specific related link
            container = self.find_link_container(link_tag, result)

            # Extract content, source, and time from the identified container (if any)
            content = (
                self.extract_from_container(container, self.RELATED_CONTENT_SELECTORS)
                if container
                else ""
            )
            source = (
                self.extract_from_container(container, self.RELATED_SOURCE_SELECTORS)
                if container
                else ""
            )
            # Use the general time extraction logic on the container
            time_str = self.extract_time(container) if container else ""

            related_links.append(
                {
                    "title": title,
                    "url": href,  # Raw URL
                    "content": content,
                    "source": source,
                    "time": time_str,
                    "more": [],  # Placeholder for potential future merging
                }
            )

        return related_links

    @staticmethod
    def merge_entries(target, source):
        """
        Merge data from a 'source' entry into a 'target' entry.
        Used during deduplication to combine information from duplicate URLs.
        Fills missing fields in the target and aggregates additional title/content pairs into 'more'.
        """
        # Fill missing fields in the target entry with data from the source
        if not target.get("title") and source.get("title"):
            target["title"] = source["title"]
        if not target.get("content") and source.get("content"):
            target["content"] = source["content"]
        if not target.get("source") and source.get("source"):
            target["source"] = source["source"]
        if not target.get("time") and source.get("time"):
            target["time"] = source["time"]

        # Add the source's title/content pair to the target's 'more' list if both exist
        if source.get("title") and source.get("content"):
            # Ensure 'more' exists and is a list
            target.setdefault("more", []).append({source["title"]: source["content"]})

        # Merge related links from source into target
        if "related_links" in source:
            target.setdefault("related_links", []).extend(source["related_links"])

    def deduplicate_results(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Deduplicate search results based on URL. Merges entries with the same URL.
        Also deduplicates related links within each entry.
        """
        # --- Deduplicate Main Entries ---
        main_map = {}
        for entry in data:
            key = entry.get("url", "")  # Use URL as the key for deduplication
            if not key:
                continue  # Skip entries without a URL

            if key in main_map:
                # If URL already exists, merge the current entry into the existing one
                existing = main_map[key]
                self.merge_entries(existing, entry)
            else:
                # If URL is new, add the entry to the map
                main_map[key] = entry

        # Convert the map back to a list of unique main entries
        results = list(main_map.values())

        # --- Deduplicate Related Links within each Main Entry ---
        for entry in results:
            rl_map = {}
            new_more = entry.get("more", []).copy()

            for rl in entry["related_links"]:
                rl_key = rl.get("url", "")
                if not rl_key:
                    continue  # Skip related links without a URL

                # If a related link's URL matches the main entry's URL, merge it into the main entry
                if rl_key == entry.get("url", ""):
                    self.merge_entries(entry, rl)
                    continue  # Don't add it to the related links map

                # Deduplicate among other related links
                if rl_key in rl_map:
                    # Merge into the existing related link entry
                    existing_rl = rl_map[rl_key]
                    self.merge_entries(existing_rl, rl)
                else:
                    # Add the new related link to the map
                    rl_map[rl_key] = rl

            # Update the entry with the deduplicated related links and merged 'more' data
            entry["related_links"] = list(rl_map.values())
            entry["more"] = new_more

        return results

    def initial_deduplicate_results(
        self, data: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Perform preliminary deduplication based on the raw URLs extracted."""
        return self.deduplicate_results(data)

    def final_deduplicate_results(
        self, data: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Perform final deduplication after resolving redirect URLs."""
        return self.deduplicate_results(data)

    async def process_real_urls(
        self, session: aiohttp.ClientSession, data: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Resolve Baidu's redirect links to get the final target URLs for main and related links.
        Uses batch processing for efficiency.
        """
        if not data:
            return []

        # Collect all URLs (main and related) that need resolution
        all_links = []
        link_map = {}

        for i, entry in enumerate(data):
            if entry.get("url"):
                all_links.append(entry["url"])
                link_map[(i, "main")] = len(all_links) - 1
            for j, link_data in enumerate(entry.get("related_links", [])):
                if link_data.get("url"):
                    all_links.append(link_data["url"])
                    link_map[(i, j)] = len(all_links) - 1

        if not all_links:
            return data  # No URLs to resolve

        # Use a semaphore to limit concurrent URL resolution requests
        semaphore = asyncio.Semaphore(self.max_semaphore)
        # Prepare headers for URL resolution (often good to remove cookies)
        resolve_headers = {
            k: v for k, v in self.headers.items() if k.lower() != "cookie"
        }

        # Fetch real URLs in batches using the utility function
        real_urls = await batch_fetch_real_urls(
            session=session,
            urls=all_links,
            headers=resolve_headers,
            proxy_list=self.proxies,
            base="https://www.baidu.com",  # Base URL for resolving relative redirects
            max_semaphore=semaphore,
            timeout=self.timeout,
            retries=self.retries,
            min_sleep=self.min_sleep,
            max_sleep=self.max_sleep,
            max_redirects=self.max_redirects,
            logger=self.logger,
            cache=self.url_cache.cache,  # Pass the cache dictionary
            batch_size=self.batch_size,
        )

        # Update the original data structure with the resolved URLs
        for (entry_idx, position), resolved_url_idx in link_map.items():
            if resolved_url_idx < len(real_urls):
                resolved_url = real_urls[resolved_url_idx]
                if position == "main":
                    data[entry_idx]["url"] = resolved_url
                else:
                    if "related_links" in data[entry_idx] and position < len(
                        data[entry_idx]["related_links"]
                    ):
                        data[entry_idx]["related_links"][position]["url"] = resolved_url

        # Perform a final deduplication pass now that real URLs are available
        return self.final_deduplicate_results(data)

    def is_advertisement(self, result) -> bool:
        """Check if a given search result element is likely an advertisement."""
        # Check for specific keywords in the inline style attribute
        style_attr = result.get("style", "")
        if any(keyword in style_attr for keyword in self.AD_STYLE_KEYWORDS):
            return True

        # Check for specific keywords in the class attribute
        classes = result.get("class", [])
        class_str = " ".join(classes) if classes else ""
        if any(keyword in class_str for keyword in self.AD_CLASS_KEYWORDS):
            return True

        # Check if specific ad-indicating tags exist within the result
        if any(result.select_one(selector) for selector in self.AD_TAG_SELECTORS):
            return True

        return False  # Not identified as an ad

    def parse_results(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Parse the HTML of the search results area (`#content_left`) to extract structured data."""
        if self.logger:
            self.logger.debug("[BAIDU]: Start parsing search results page content")

        # Select all main result container divs
        results = soup.select("div[class*='c-container'][class*='result']")
        if not results:
            if self.logger:
                self.logger.warning("[BAIDU]: No main result containers found on page.")
            return []
        if self.logger:
            self.logger.info(
                f"[BAIDU]: Found {len(results)} potential result containers"
            )

        parsed_data = []
        for result_container in results:
            # Skip if identified as an advertisement and filtering is enabled
            if self.filter_ads and self.is_advertisement(result_container):
                if self.logger:
                    self.logger.debug("[BAIDU]: Skipping advertisement result.")
                continue

            # Extract main title and link
            title, url = self.extract_main_title_and_link(result_container)
            # Keep track of the main link to avoid re-extracting it as a related link
            main_links_in_result = [url] if url else []

            # Extract other main data points
            content = self.extract_main_content(result_container)
            source = self.extract_main_source(result_container)
            time_info = self.extract_time(result_container)
            # Extract related links found within this result container
            related_links = self.extract_related_links(
                result_container, main_links_in_result
            )

            # Only add the result if it has at least a title or a URL
            if title or url:
                parsed_data.append(
                    {
                        "title": title,
                        "url": url,  # Raw URL
                        "content": content,
                        "source": source,
                        "time": time_info,
                        "more": [],  # Initialize 'more' list
                        "related_links": related_links,
                    }
                )

        # Perform initial deduplication based on raw URLs before resolving redirects
        return self.initial_deduplicate_results(parsed_data)

    async def scrape_single_page(
        self, session: aiohttp.ClientSession, query: str, page: int
    ) -> List[Dict[str, Any]]:
        """Scrape a single page of Baidu search results."""
        # Baidu uses 'pn' parameter for pagination, starting from 0 for page 1, 10 for page 2, etc.
        page_start = page * 10
        if self.logger:
            self.logger.info(
                f"Scraping page {page+1} (pn={page_start}) for query '{query}'"
            )

        # Construct URL parameters for the Baidu search request
        params = {
            "wd": query,  # Search keyword
            "pn": str(page_start),  # Page number offset
            "ie": "utf-8",  # Input encoding
            "usm": "1",
            "rsv_pq": str(int(time.time() * 1000)),
            "rsv_t": str(int(time.time() * 1000)),
        }

        # Use the semaphore from BaseScraper to limit concurrent HTTP requests
        async with self.semaphore:
            # Fetch the HTML content using the base class method
            html_content = await self.get_page(
                url="https://www.baidu.com/s",  # Baidu search URL
                params=params,
                use_proxy=self.use_proxy,
                headers=self.headers,
                session=session,  # Pass the shared session
            )

        if not html_content:
            if self.logger:
                self.logger.error(
                    f"Failed to retrieve HTML for page {page+1}, skipping."
                )
            return []  # Return empty list on failure

        try:
            # Parse the HTML using BeautifulSoup with lxml parser for speed
            soup = BeautifulSoup(html_content, "lxml")
            # Find the main content area containing search results
            content_left = soup.find("div", id="content_left")
            if not content_left:
                # This often indicates a CAPTCHA page or an error page from Baidu
                if self.logger:
                    self.logger.error(
                        f"Could not find '#content_left' div on page {page+1}. Possible anti-scraping measure or empty results. Skipping."
                    )
                return []

            # Parse the results from the content area
            return self.parse_results(content_left)
        except Exception as e:
            if self.logger:
                self.logger.error(
                    f"Error parsing HTML for page {page+1}: {e}", exc_info=True
                )
            return []  # Return empty list on parsing error

    async def scrape(
        self,
        query: str,
        pages: int = 1,
        filter_ads: bool = None,
        max_concurrent_pages: int = None,
        cache_file: Optional[Path] = None,
        use_cache: bool = True,
        clear_cache: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Main method to search Baidu and scrape multiple pages of search results for a given query.
        Handles concurrent page fetching, result parsing, URL resolution, and deduplication.

        Args:
            query: Search query string
            pages: Number of pages to scrape
            filter_ads: Whether to filter advertisements (uses instance setting if None)
            max_concurrent_pages: Override for concurrent pages limit (uses instance setting if None)
            cache_file: Cache file path for URL caching
            use_cache: Whether to use URL caching
            clear_cache: Whether to clear existing cache before starting

        Returns:
            List of search result dictionaries
        """
        # Store original settings for restoration
        original_filter_ads = self.filter_ads
        original_max_concurrent_pages = self.max_concurrent_pages

        # Apply temporary settings if provided
        if filter_ads is not None:
            self.filter_ads = filter_ads
        if max_concurrent_pages is not None:
            self.max_concurrent_pages = max_concurrent_pages

        # Clear cache if requested
        if clear_cache and hasattr(self, "url_cache"):
            self.url_cache.clear()
            if self.logger:
                self.logger.info("URL cache cleared")

        try:
            # Main scraping logic starts here
            start_time = time.time()
            if self.logger:
                self.logger.info(
                    f"[BAIDU]: Scraping started for query: '{query}', pages: {pages}, concurrent pages: {self.max_concurrent_pages}"
                )

            all_results = []

            # Use a single aiohttp session for all requests in this scrape operation
            async with aiohttp.ClientSession() as session:
                # Process pages in batches based on the max_concurrent_pages setting
                for i in range(0, pages, self.max_concurrent_pages):
                    # Determine the range of pages for the current batch
                    batch_pages = range(i, min(i + self.max_concurrent_pages, pages))
                    if self.logger:
                        self.logger.info(
                            f"Processing page batch: {batch_pages.start + 1} to {batch_pages.stop}"
                        )

                    # Create tasks for scraping each page in the current batch concurrently
                    tasks = [
                        self.scrape_single_page(session, query, page)
                        for page in batch_pages
                    ]
                    # Run tasks concurrently and gather results
                    page_batch_results = await asyncio.gather(
                        *tasks,
                        return_exceptions=True,  # Capture exceptions instead of raising them immediately
                    )

                    # Process results from the batch
                    for result in page_batch_results:
                        if isinstance(result, list):
                            # Extend the main list with successfully parsed results
                            all_results.extend(result)
                        elif isinstance(result, Exception):
                            # Log errors that occurred during scraping of individual pages
                            if self.logger:
                                self.logger.error(
                                    f"Error during page scraping batch: {result}"
                                )

                    # Introduce a delay between batches if more pages are remaining
                    if batch_pages.stop < pages:
                        delay = random.uniform(self.min_sleep, self.max_sleep)
                        if self.logger:
                            self.logger.debug(
                                f"Sleeping for {delay:.2f} seconds between page batches"
                            )
                        await asyncio.sleep(delay)

                # Perform initial deduplication on raw results
                if self.logger:
                    self.logger.info(
                        f"[BAIDU]: Performing initial deduplication on {len(all_results)} raw results..."
                    )
                all_results = self.initial_deduplicate_results(all_results)

                # Resolve real URLs for all results after deduplication
                if self.logger:
                    self.logger.info(
                        f"[BAIDU]: Resolving real URLs for {len(all_results)} results after initial deduplication..."
                    )
                all_results = await self.process_real_urls(session, all_results)

            # Perform final deduplication after URL resolution
            final_results = self.final_deduplicate_results(all_results)

            # Save the URL cache to file if enabled
            if use_cache and cache_file:
                if self.logger:
                    self.logger.info(f"[BAIDU]: Saving URL cache to file: {cache_file}")
                self.url_cache.save_to_file(cache_file)
            elif self.logger:
                self.logger.debug(
                    f"[BAIDU]: Skipping URL cache save, use_cache={use_cache}, cache_file={cache_file}"
                )

            elapsed = time.time() - start_time
            if self.logger:
                self.logger.info(
                    f"[BAIDU]: Search completed for '{query}'. Retrieved {len(final_results)} final results. Elapsed time: {elapsed:.2f}s"
                )
            return final_results

        finally:
            # Restore original settings
            self.filter_ads = original_filter_ads
            self.max_concurrent_pages = original_max_concurrent_pages
