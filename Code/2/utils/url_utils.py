from urllib.parse import urljoin, urlparse
import aiohttp
import asyncio
from typing import Dict, List, Optional
import logging
import random


def is_valid_url(url: str) -> bool:
    """
    Check if URL is valid

    Args:
        url: URL to check

    Returns:
        Whether URL is valid
    """
    if not url:
        return False
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except (ValueError, TypeError):
        return False


def fix_url(url: str, base: str) -> str:
    """
    Fix incomplete URLs

    Args:
        url: Potentially incomplete URL
        base: Base URL for joining relative paths

    Returns:
        Fixed URL

    Raises:
        ValueError: If base is not a valid URL
    """
    if not url:
        return ""

    if not is_valid_url(base):
        raise ValueError(f"Invalid base URL: {base}")

    if not url.startswith(("http://", "https://")):
        try:
            url = urljoin(base, url)
        except (ValueError, TypeError):
            return url  # If joining fails, return original URL

    return url


def normalize_url(url: str, base: str, strip_params: bool = False) -> str:
    """
    Normalize URL, supporting relative path resolution based on base URL

    Args:
        url: URL to normalize
        base: Base URL for resolving relative paths
        strip_params: Whether to remove URL parameters

    Returns:
        Normalized URL
    """
    if not url or isinstance(url, Exception):
        return ""

    # If URL is invalid, try to fix with base
    if not is_valid_url(url):
        url = fix_url(url, base)

    try:
        parsed = urlparse(url)

        # Ensure protocol and domain
        scheme = parsed.scheme.lower() or "https"  # Default to https
        netloc = parsed.netloc.lower()

        # Remove www prefix
        if netloc.startswith("www."):
            netloc = netloc[4:]

        # Normalize path, remove trailing slashes and redundant segments
        path = parsed.path.rstrip("/")
        if not path:
            path = "/"

        # Whether to keep parameters
        query = "" if strip_params else parsed.query

        # Rebuild URL
        normalized = f"{scheme}://{netloc}{path}"
        if query:
            normalized += f"?{query}"

        return normalized
    except (ValueError, TypeError):
        return url  # Return original URL if parsing fails


async def fetch_real_url(
    session: aiohttp.ClientSession,
    org_link: str,
    headers: dict,
    proxy_list: List[str],
    base: str,
    max_semaphore: asyncio.Semaphore,
    timeout: int = 3,
    retries: int = 0,
    min_sleep: float = 0.1,
    max_sleep: float = 0.3,
    max_redirects: int = 5,
    logger: Optional[logging.Logger] = None,
    cache: Optional[Dict[str, str]] = None,
) -> str:
    """
    Fetch real URL by following redirects

    Args:
        session: HTTP client session
        org_link: Original link to resolve
        headers: Request headers
        proxy_list: List of proxy servers
        base: Base URL for resolving relative paths
        max_semaphore: Concurrency limiter
        timeout: Request timeout in seconds
        retries: Number of retry attempts
        min_sleep: Minimum delay between requests
        max_sleep: Maximum delay between requests
        max_redirects: Maximum redirects to follow
        logger: Logger instance
        cache: URL cache dictionary

    Returns:
        Resolved real URL
    """
    if not org_link:
        if logger:
            logger.debug("[URL_UTILS]: Empty link, returning empty string")
        return ""

    if cache and org_link in cache:
        if logger:
            logger.debug(f"URL returned from cache: {org_link} -> {cache[org_link]}")
        return cache[org_link]

    if not is_valid_url(org_link):
        try:
            fixed_link = fix_url(org_link, base)
            if logger:
                logger.debug(f"Fixed link format: {org_link} -> {fixed_link}")
            org_link = fixed_link
        except ValueError as e:
            if logger:
                logger.error(f"Invalid base URL: {str(e)}")
            return org_link

    request_headers = headers.copy()
    if "Cookie" in request_headers:
        del request_headers["Cookie"]

    async with max_semaphore:
        current_url = org_link
        redirect_count = 0

        while redirect_count < max_redirects:
            for attempt in range(retries + 1):
                proxy = random.choice(proxy_list) if proxy_list else None
                try:
                    if logger:
                        logger.debug(
                            f"Attempting to get URL: {current_url}"
                            + (f" via proxy: {proxy}" if proxy else "")
                        )

                    if "Referer" in request_headers:
                        parsed_url = urlparse(current_url)
                        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
                        request_headers["Referer"] = base_url

                    async with session.get(
                        current_url,
                        headers=request_headers,
                        allow_redirects=False,
                        timeout=aiohttp.ClientTimeout(total=timeout),
                        proxy=proxy,
                    ) as response:
                        if response.status in (301, 302, 303, 307, 308):
                            location = response.headers.get("Location")
                            if not location:
                                if logger:
                                    logger.warning(
                                        f"No redirect location found: {org_link}"
                                    )
                                if cache is not None:
                                    cache[org_link] = org_link
                                return org_link

                            current_url = urljoin(str(response.url), location)
                            redirect_count += 1

                            if logger:
                                logger.debug(
                                    f"[URL_UTILS]: Redirect detected ({redirect_count}/{max_redirects}): {current_url}"
                                )

                            await asyncio.sleep(random.uniform(min_sleep, max_sleep))
                            break
                        else:
                            result = str(response.url)
                            if logger:
                                logger.debug(
                                    f"Real URL obtained: {org_link} -> {result}"
                                )
                            if cache is not None:
                                cache[org_link] = result
                            return result

                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    if attempt == retries:
                        if logger:
                            logger.error(
                                f"Link resolution failed (attempt {attempt+1}/{retries+1}): {current_url}, "
                                f"proxy: {proxy}, error: {str(e)}"
                            )
                    if attempt < retries:
                        sleep_time = random.uniform(min_sleep, max_sleep)
                        if logger:
                            logger.debug(
                                f"Retry {attempt+1}/{retries}, waiting {sleep_time:.2f}s"
                            )
                        await asyncio.sleep(sleep_time)
                    continue
            else:
                if logger:
                    logger.warning(
                        f"Retries exhausted, resolution failed: {current_url}"
                    )
                if cache is not None:
                    cache[org_link] = current_url
                return current_url

        if logger:
            logger.warning(f"Maximum redirects exceeded: {org_link}")
        if cache is not None:
            cache[org_link] = current_url
        return current_url


async def batch_fetch_real_urls(
    session: aiohttp.ClientSession,
    urls: List[str],
    headers: dict,
    proxy_list: List[str],
    base: str,
    max_semaphore: asyncio.Semaphore,
    timeout: int = 3,
    retries: int = 0,
    min_sleep: float = 0.1,
    max_sleep: float = 0.3,
    max_redirects: int = 5,
    logger: Optional[logging.Logger] = None,
    cache: Optional[Dict[str, str]] = None,
    batch_size: int = 10,
) -> List[str]:
    """
    Fetch real URLs for a batch of links

    Args:
        session: HTTP client session
        urls: List of URLs to resolve
        headers: Request headers
        proxy_list: List of proxy servers
        base: Base URL for resolving relative paths
        max_semaphore: Concurrency limiter
        timeout: Request timeout in seconds
        retries: Number of retry attempts
        min_sleep: Minimum delay between requests
        max_sleep: Maximum delay between requests
        max_redirects: Maximum redirects to follow
        logger: Logger instance
        cache: URL cache dictionary
        batch_size: Number of URLs to process in each batch

    Returns:
        List of resolved URLs
    """
    request_headers = headers.copy()
    if "Cookie" in request_headers:
        del request_headers["Cookie"]

    results = []
    for i in range(0, len(urls), batch_size):
        batch = urls[i : i + batch_size]
        if logger:
            logger.debug(
                f"Processing URL batch {i//batch_size + 1}/{(len(urls)-1)//batch_size + 1}, containing {len(batch)} URLs"
            )

        tasks = [
            fetch_real_url(
                session,
                url,
                request_headers,
                proxy_list,
                base,
                max_semaphore,
                timeout,
                retries,
                min_sleep,
                max_sleep,
                max_redirects,
                logger,
                cache,
            )
            for url in batch
        ]

        batch_results = await asyncio.gather(*tasks, return_exceptions=True)
        results.extend(batch_results)

        if i + batch_size < len(urls):
            await asyncio.sleep(random.uniform(min_sleep, max_sleep))

    return results
