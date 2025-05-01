from typing import Any, Dict, List, Optional, Tuple, Union
import asyncio
import aiohttp
from bs4 import BeautifulSoup
from pathlib import Path
import time
import random
import logging

from scrapers.base_scraper import BaseScraper
from utils.url_utils import batch_fetch_real_urls


class BaiduScraper(BaseScraper):
    """Baidu Search Results Scraper"""

    # CSS selectors for extracting content
    TITLE_SELECTORS = ["h3[class*='title']", "h3[class*='t']"]
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
    TIME_SELECTORS = ["span[class*='time']", "span.c-color-gray2", "span.n2n9e2q"]
    RELATED_CONTENT_SELECTORS = [
        "div[class*=text], div[class*=abs], div[class*=desc], div[class*=content]",
        "p[class*=text], p[class*=desc], p[class*=content]",
        "span[class*=text], span[class*=desc], span[class*=content], span[class*=clamp]",
    ]
    RELATED_SOURCE_SELECTORS = [
        "span[class*=small], span[class*=showurl], span[class*=source-text], span[class*=site-name]",
        "div[class*=source-text], div[class*=showurl], div[class*=small]",
    ]

    # Constants for advertisement detection
    AD_STYLE_KEYWORDS = ["!important"]
    AD_CLASS_KEYWORDS = ["tuiguang"]
    AD_TAG_SELECTORS = ["span.ec-tuiguang"]

    def __init__(
        self,
        headers: Dict[str, str],
        proxies: List[str] = None,
        filter_ads: bool = True,
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

    def extract_main_title_and_link(self, result) -> Tuple[str, str]:
        """
        Extract title and link from search result

        Args:
            result: BeautifulSoup element representing a search result

        Returns:
            Tuple of (title, link URL)
        """
        for selector in self.TITLE_SELECTORS:
            title_tag = result.select_one(selector)
            if title_tag:
                a_tag = title_tag.find("a")
                if a_tag:
                    return a_tag.get_text(strip=True), a_tag.get("href", "")
        return "", ""

    def extract_main_content(self, result) -> str:
        """
        Extract content summary from search result

        Args:
            result: BeautifulSoup element representing a search result

        Returns:
            Content text
        """
        for selector in self.CONTENT_SELECTORS:
            element = result.select_one(selector)
            if element:
                return element.get_text(strip=True)
        return ""

    def extract_main_source(self, result) -> str:
        """
        Extract source information; returns empty if multiple matches

        Args:
            result: BeautifulSoup element representing a search result

        Returns:
            Source text or empty string
        """
        for selector in self.SOURCE_SELECTORS:
            elements = result.select(selector)
            if elements:
                return elements[0].get_text(strip=True) if len(elements) == 1 else ""
        return ""

    def extract_time(self, result) -> str:
        """
        Extract time information; returns empty if multiple matches

        Args:
            result: BeautifulSoup element representing a search result

        Returns:
            Time string or empty string
        """
        for selector in self.TIME_SELECTORS:
            elements = result.select(selector)
            if not elements:
                continue

            # Special handling for c-color-gray2 selector
            if selector == "span.c-color-gray2":
                # Filter elements that have only class attribute with value c-color-gray2
                filtered_elements = [
                    el
                    for el in elements
                    if len(el.attrs) == 1 and el.get("class") == ["c-color-gray2"]
                ]
                if len(filtered_elements) == 1:
                    return filtered_elements[0].get_text(strip=True)
                elif len(filtered_elements) > 1:
                    return ""
            # Logic for other selectors
            else:
                if len(elements) == 1:
                    return elements[0].get_text(strip=True)
                elif len(elements) > 1:
                    return ""

        return ""

    @staticmethod
    def find_link_container(link_tag, result):
        """
        Find the container element for a link

        Args:
            link_tag: The link tag to find container for
            result: The parent search result element

        Returns:
            Container element or None
        """
        # Look for container with specific class types first
        container = None
        current = link_tag.parent
        while current and current != result:
            if current.name == "div" and current.get("class"):
                class_str = " ".join(current.get("class", []))
                # Match common container class keywords
                if any(
                    kw in class_str.lower()
                    for kw in ["item", "container", "result", "sitelink"]
                ):
                    container = current
                    break
            current = current.parent

        # If no specific container found, use nearest parent div
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
        """
        Extract text from specified elements within a container

        Args:
            container: Container element to search within
            selectors: List of CSS selectors to try

        Returns:
            Extracted text or empty string
        """
        if not container:
            return ""

        for selector in selectors:
            elements = container.select(selector)
            if elements:
                return elements[0].get_text(strip=True)
        return ""

    def extract_related_links(
        self, result, main_links: List[str]
    ) -> List[Dict[str, Any]]:
        """
        Extract related links from a result, excluding provided main links

        Args:
            result: BeautifulSoup object representing a search result
            main_links: List of main link URLs to exclude

        Returns:
            List of dictionaries with related link details
        """
        related_links = []

        # Traverse all <a> tags in the result
        for link_tag in result.find_all("a"):
            href = link_tag.get("href", "")

            # Skip if the link is in main_links or lacks title/href
            if href in main_links or not (href and link_tag.get_text(strip=True)):
                continue

            title = link_tag.get_text(strip=True)
            container = self.find_link_container(link_tag, result)

            # Extract additional data from the container
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
            time_str = self.extract_time(container) if container else ""

            related_links.append(
                {
                    "title": title,
                    "url": href,
                    "content": content,
                    "source": source,
                    "time": time_str,
                    "more": [],
                }
            )

        return related_links

    @staticmethod
    def merge_entries(target, source):
        """
        Merge two entries, combining source content into target

        Args:
            target: Target entry to merge into
            source: Source entry to merge from
        """
        # Fill missing fields
        if not target["title"] and source["title"]:
            target["title"] = source["title"]
        if not target["content"] and source["content"]:
            target["content"] = source["content"]
        if not target["source"] and source["source"]:
            target["source"] = source["source"]
        if not target["time"] and source["time"]:
            target["time"] = source["time"]

        # Merge more field
        if source["title"] and source["content"]:
            target["more"].append({source["title"]: source["content"]})

        # Merge related links
        if "related_links" in source:
            target.setdefault("related_links", []).extend(source["related_links"])

    def deduplicate_results(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Deduplicate processing: merge duplicate main links and their related links

        Args:
            data: List of search result entries

        Returns:
            Deduplicated list of entries
        """
        main_map = {}
        # Handle main link deduplication
        for entry in data:
            key = entry.get("url", "")
            if not key:
                continue

            if key in main_map:
                # Merge into existing entry
                existing = main_map[key]
                self.merge_entries(existing, entry)
            else:
                main_map[key] = entry

        results = list(main_map.values())

        # Handle related links deduplication
        for entry in results:
            rl_map = {}
            new_more = entry["more"].copy()

            for rl in entry["related_links"]:
                rl_key = rl.get("url", "")

                # If related link matches main link, merge into main
                if rl_key == entry.get("url", ""):
                    self.merge_entries(entry, rl)
                    continue

                # Deduplicate between related links
                if rl_key in rl_map:
                    existing_rl = rl_map[rl_key]
                    self.merge_entries(existing_rl, rl)
                else:
                    rl_map[rl_key] = rl

            entry["related_links"] = list(rl_map.values())
            entry["more"] = new_more

        return results

    def initial_deduplicate_results(
        self, data: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Preliminary deduplication

        Args:
            data: List of search result entries

        Returns:
            Initially deduplicated results
        """
        return self.deduplicate_results(data)

    def final_deduplicate_results(
        self, data: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Final deduplication based on real URLs

        Args:
            data: List of search result entries

        Returns:
            Finally deduplicated results
        """
        return self.deduplicate_results(data)

    async def process_real_urls(
        self, session: aiohttp.ClientSession, data: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Process Baidu redirect links to get real URLs

        Args:
            session: Active client session
            data: List of search result entries

        Returns:
            List of entries with resolved URLs
        """
        if not data:
            return []

        all_links = []
        link_map = {}
        for i, entry in enumerate(data):
            if entry["url"]:
                all_links.append(entry["url"])
                link_map[(i, "main")] = len(all_links) - 1
            for j, link in enumerate(entry["related_links"]):
                if link["url"]:
                    all_links.append(link["url"])
                    link_map[(i, j)] = len(all_links) - 1

        if not all_links:
            return data

        semaphore = asyncio.Semaphore(self.max_semaphore)
        headers = {k: v for k, v in self.headers.items() if k != "Cookie"}
        real_urls = await batch_fetch_real_urls(
            session,
            all_links,
            headers,
            self.proxies,
            "https://www.baidu.com",
            semaphore,
            self.timeout,
            self.retries,
            self.min_sleep,
            self.max_sleep,
            self.max_redirects,
            self.logger,
            self.url_cache.cache,
            self.batch_size,
        )

        for (i, pos), url_idx in link_map.items():
            if url_idx < len(real_urls):
                if pos == "main":
                    data[i]["url"] = real_urls[url_idx]
                else:
                    data[i]["related_links"][pos]["url"] = real_urls[url_idx]

        return self.final_deduplicate_results(data)

    def is_advertisement(self, result) -> bool:
        """
        Check if a search result is an advertisement using predefined constants.

        Args:
            result: BeautifulSoup element representing a search result

        Returns:
            True if the element is an advertisement, False otherwise
        """
        # Check style attribute for keywords
        style_attr = result.get("style", "")
        if any(keyword in style_attr for keyword in self.AD_STYLE_KEYWORDS):
            return True

        # Check for ad-specific class names
        classes = result.get("class", [])
        class_str = " ".join(classes) if classes else ""
        if any(keyword in class_str for keyword in self.AD_CLASS_KEYWORDS):
            return True

        # Check for ad tags inside the result
        if any(result.select_one(selector) for selector in self.AD_TAG_SELECTORS):
            return True

        return False

    def parse_results(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """
        Parse Baidu search results
        """
        if self.logger:
            self.logger.debug("[BAIDU]: Start parsing search results page")
        results = soup.select("div[class*='c-container'][class*='result']")
        if not results:
            if self.logger:
                self.logger.error("[BAIDU]: No search results found")
            return []
        if self.logger:
            self.logger.info(f"[BAIDU]: Found {len(results)} search results")

        data = []
        for result in results:
            # Skip Advertisements
            if self.filter_ads and self.is_advertisement(result):
                if self.logger:
                    self.logger.debug("[BAIDU]: Skipping advertisement result.")
                continue

            # Extract main title and link
            title, url = self.extract_main_title_and_link(result)
            main_links = [url] if url else []  # List of main links to exclude

            # Extract other data
            content = self.extract_main_content(result)
            source = self.extract_main_source(result)
            time_info = self.extract_time(result)
            related_links = self.extract_related_links(result, main_links)

            if title or url:
                data.append(
                    {
                        "title": title,
                        "url": url,
                        "content": content,
                        "source": source,
                        "time": time_info,
                        "more": [],
                        "related_links": related_links,
                    }
                )

        return self.initial_deduplicate_results(data)

    async def scrape(
        self,
        query: str,
        num_pages: int = 1,
        cache_to_file: bool = True,
        cache_file: Optional[Path] = None,
    ) -> List[Dict[str, Any]]:
        start_time = time.time()
        if self.logger:
            self.logger.info(
                f"[BAIDU]: Scraping started for query: '{query}', pages: {num_pages}"
            )
        all_results = []

        async with aiohttp.ClientSession() as session:
            for page in range(num_pages):
                page_start = page * 10
                if self.logger:
                    self.logger.info(f"Scraping page {page+1}/{num_pages}")

                params = {
                    "wd": query,
                    "pn": str(page_start),
                    "ie": "utf-8",
                    "usm": "1",
                    "rsv_pq": str(int(time.time() * 1000)),
                    "rsv_t": str(int(time.time() * 1000)),
                }

                html_content = await self.get_page(
                    url="https://www.baidu.com/s",
                    params=params,
                    use_proxy=self.use_proxy,
                    headers=self.headers,
                )
                if not html_content:
                    if self.logger:
                        self.logger.error(f"Failed to get page {page+1}, skipping")
                    continue

                # soup = BeautifulSoup(html_content, "html.parser")
                soup = BeautifulSoup(
                    html_content, "lxml"
                )  # Use lxml parser for better performance
                content_left = soup.find("div", id="content_left")
                if not content_left:
                    if self.logger:
                        self.logger.error(
                            f"No content found, possibly due to Baidu's anti-crawling mechanism, skipping page {page+1}"
                        )
                    continue

                page_results = self.parse_results(content_left)
                all_results.extend(page_results)

                if page < num_pages - 1:
                    delay = random.uniform(self.min_sleep, self.max_sleep)
                    if self.logger:
                        self.logger.debug(
                            f"Waiting {delay:.2f}s before scraping next page"
                        )
                    await asyncio.sleep(delay)

            if self.logger:
                self.logger.info(
                    f"[BAIDU]: Performing initial deduplication of {len(all_results)} results"
                )
            deduplicated_results = self.initial_deduplicate_results(all_results)

            if self.logger:
                self.logger.info(
                    f"[BAIDU]: Resolving {len(deduplicated_results)} URLs after initial deduplication"
                )
            final_results = await self.process_real_urls(session, deduplicated_results)

        if cache_to_file and cache_file:
            if self.logger:
                self.logger.info(f"[BAIDU]: Saving URL cache to file: {cache_file}")
            self.url_cache.save_to_file(cache_file)

        elapsed = time.time() - start_time
        if self.logger:
            self.logger.info(
                f"[BAIDU]: Search completed, retrieved {len(final_results)} results, elapsed time: {elapsed:.2f}s"
            )
        return final_results
