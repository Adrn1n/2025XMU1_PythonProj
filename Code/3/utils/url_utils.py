from urllib.parse import urljoin, urlparse
import aiohttp
import asyncio
from typing import Dict, List, Optional
import logging
import random


def _filter_valid_proxies(
    proxy_list: List[str], logger: Optional[logging.Logger] = None, context: str = ""
) -> List[str]:
    """
    Filter out invalid proxies that might be comments or empty lines.

    Args:
        proxy_list: List of proxy URLs to filter.
        logger: Optional logger instance.
        context: Optional context string for logging (e.g., "Batch fetch").

    Returns:
        List of valid proxy URLs.
    """
    if not proxy_list:
        return []

    valid_proxies = [
        p
        for p in proxy_list
        if p and not p.startswith("#") and p.startswith(("http://", "https://"))
    ]

    if logger and len(valid_proxies) != len(proxy_list):
        context_prefix = f"{context} - " if context else ""
        logger.warning(
            f"[URL_UTILS]: {context_prefix}Filtered out {len(proxy_list) - len(valid_proxies)} invalid proxies"
        )

    return valid_proxies


def is_valid_url(url: str) -> bool:
    """Check if a string represents a valid absolute URL (with scheme and netloc)."""
    if not url:
        return False
    try:
        result = urlparse(url)
        # A valid URL must have both a scheme (http, https) and a network location (domain)
        return all([result.scheme, result.netloc])
    except (ValueError, TypeError):
        # Handle potential errors during parsing
        return False


def fix_url(url: str, base: str) -> str:
    """
    Attempt to fix potentially incomplete URLs by joining them with a base URL.
    Useful for converting relative paths (e.g., '/path/to/page') to absolute URLs.
    """
    if not url:
        return ""  # Return empty string if input URL is empty

    # The base URL must be valid for joining to work correctly
    if not is_valid_url(base):
        raise ValueError(f"Invalid base URL provided for fixing: {base}")

    # If the URL doesn't already start with a scheme, assume it's relative or scheme-less
    if not url.startswith(("http://", "https://")):
        try:
            # urljoin handles joining base URL and relative paths correctly
            return urljoin(base, url)
        except (ValueError, TypeError):
            # If joining fails for some reason, return the original URL
            return url

    # If the URL already has a scheme, return it as is
    return url


def normalize_url(url: str, base: str, strip_params: bool = False) -> str:
    """
    Normalize a URL to a standard format.
    - Converts scheme and domain to lowercase.
    - Removes 'www.' prefix.
    - Ensures a path exists (at least '/').
    - Removes trailing slashes from the path.
    - Optionally removes query parameters.
    - Fixes relative URLs using the base URL.
    """
    if not url or isinstance(url, Exception):  # Handle empty or exceptional input
        return ""

    # If the URL isn't valid, try fixing it first using the base URL
    if not is_valid_url(url):
        url = fix_url(url, base)
        # If fixing still results in an invalid URL, return it as is
        if not is_valid_url(url):
            return url

    try:
        parsed = urlparse(url)

        # Ensure scheme and netloc are lowercase, default scheme to https if missing
        scheme = (parsed.scheme or "https").lower()
        netloc = parsed.netloc.lower()

        # Remove 'www.' prefix if present
        if netloc.startswith("www."):
            netloc = netloc[4:]

        # Normalize path: remove trailing slash, ensure root path is '/'
        path = parsed.path.rstrip("/")
        if not path:
            path = "/"  # Ensure there's always at least a root path

        # Decide whether to keep or strip query parameters
        query = "" if strip_params else parsed.query

        # Reconstruct the normalized URL
        normalized = f"{scheme}://{netloc}{path}"
        if query:
            normalized += f"?{query}"  # Add query string if not stripped

        return normalized
    except (ValueError, TypeError):
        # Fallback to original URL if any parsing/normalization step fails
        return url


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
    cache: Optional[Dict[str, str]] = None,  # Pass cache dict directly
) -> str:
    """
    Asynchronously fetches the final destination URL after following HTTP redirects.
    Uses a cache to avoid re-resolving the same URL.

    Args:
        session: Shared aiohttp client session.
        org_link: The initial URL to resolve.
        headers: HTTP headers to use for requests (Cookies might be excluded).
        proxy_list: List of potential proxy URLs.
        base: Base URL used for fixing relative URLs found during redirects.
        max_semaphore: Semaphore to limit concurrent requests.
        timeout: Timeout for each individual HTTP request.
        retries: Number of retries for each step of the redirect chain.
        min_sleep: Minimum delay between retries or redirect steps.
        max_sleep: Maximum delay between retries or redirect steps.
        max_redirects: Maximum number of redirects to follow.
        logger: Optional logger instance.
        cache: Optional dictionary to use as a URL cache (stores org_link -> real_link).

    Returns:
        The final resolved URL string, or the original URL if resolution fails or max redirects are exceeded.
    """
    if not org_link:
        if logger:
            logger.debug("[URL_UTILS]: Received empty link, returning empty string.")
        return ""

    # Check cache first
    if cache is not None and org_link in cache:
        cached_result = cache[org_link]
        if logger:
            logger.debug(f"[URL_UTILS]: Cache hit for {org_link} -> {cached_result}")
        return cached_result

    # Fix URL if it's not valid initially
    current_url = org_link
    if not is_valid_url(current_url):
        try:
            fixed_link = fix_url(current_url, base)
            if logger:
                logger.debug(
                    f"[URL_UTILS]: Fixed invalid link {current_url} -> {fixed_link}"
                )
            current_url = fixed_link
        except ValueError as e:  # Raised by fix_url if base is invalid
            if logger:
                logger.error(
                    f"[URL_UTILS]: Cannot fix link {current_url}, invalid base URL: {str(e)}"
                )
            # Cache the original invalid link as its own resolution
            if cache is not None:
                cache[org_link] = org_link
            return org_link  # Return original link if base is invalid
        # If fixing still results in invalid URL, return original
        if not is_valid_url(current_url):
            if logger:
                logger.warning(
                    f"[URL_UTILS]: Link remains invalid after fixing: {current_url}"
                )
            if cache is not None:
                cache[org_link] = org_link
            return org_link

    # Prepare headers for the resolution requests (often best to exclude cookies)
    request_headers = headers.copy()
    if "Cookie" in request_headers:
        del request_headers["Cookie"]
    # Set Referer based on the current URL being requested
    parsed_initial_url = urlparse(current_url)
    request_headers["Referer"] = (
        f"{parsed_initial_url.scheme}://{parsed_initial_url.netloc}"
    )

    async with max_semaphore:  # Acquire semaphore before starting requests
        redirect_count = 0
        resolved_url = current_url  # Start with the (potentially fixed) original URL

        # Filter out invalid proxies that might be comments or empty lines
        valid_proxies = _filter_valid_proxies(proxy_list, logger)

        while redirect_count < max_redirects:
            # Retry loop for the current step in the redirect chain
            for attempt in range(retries + 1):
                proxy = random.choice(valid_proxies) if valid_proxies else None
                try:
                    if logger:
                        log_msg = f"[URL_UTILS]: Resolving (R:{redirect_count+1}, A:{attempt+1}): {resolved_url}"
                        if proxy:
                            log_msg += f" via proxy {proxy}"
                        logger.debug(log_msg)

                    # Update Referer for the current URL
                    parsed_current = urlparse(resolved_url)
                    request_headers["Referer"] = (
                        f"{parsed_current.scheme}://{parsed_current.netloc}"
                    )

                    # Make request, *disallowing* automatic redirects by aiohttp
                    async with session.get(
                        resolved_url,
                        headers=request_headers,
                        allow_redirects=False,
                        timeout=aiohttp.ClientTimeout(total=timeout),
                        proxy=proxy,
                    ) as response:
                        # Check for redirect status codes
                        if response.status in (301, 302, 303, 307, 308):
                            location = response.headers.get("Location")
                            if not location:
                                if logger:
                                    logger.warning(
                                        f"[URL_UTILS]: Redirect status {response.status} but no Location header found for {resolved_url}"
                                    )
                                # Treat as final URL if no location is given
                                if cache is not None:
                                    cache[org_link] = resolved_url
                                return resolved_url

                            # Resolve the new location relative to the current URL
                            next_url = urljoin(str(response.url), location)
                            if logger:
                                logger.debug(
                                    f"[URL_UTILS]: Redirecting ({redirect_count+1}/{max_redirects}) from {resolved_url} to {next_url}"
                                )

                            resolved_url = next_url  # Update URL for the next iteration
                            redirect_count += 1

                            # Wait before following the redirect
                            await asyncio.sleep(random.uniform(min_sleep, max_sleep))
                            break  # Exit retry loop, proceed to next redirect step

                        else:
                            # Not a redirect status, consider this the final URL
                            final_url = str(
                                response.url
                            )  # Use the URL from the response object
                            if logger:
                                logger.debug(
                                    f"[URL_UTILS]: Resolved {org_link} -> {final_url} (Status: {response.status})"
                                )
                            # Store result in cache
                            if cache is not None:
                                cache[org_link] = final_url
                            return final_url

                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    if logger:
                        logger.warning(
                            f"[URL_UTILS]: Resolution error (R:{redirect_count+1}, A:{attempt+1}) for {resolved_url}: {type(e).__name__} - {str(e)}"
                        )
                    # If last retry attempt fails
                    if attempt == retries:
                        if logger:
                            logger.error(
                                f"[URL_UTILS]: Final resolution attempt failed for {resolved_url} after {retries+1} tries."
                            )
                        # Cache the original link mapping to the last known URL in the chain
                        if cache is not None:
                            cache[org_link] = resolved_url
                        return resolved_url  # Return the last URL we tried

                    # Wait before retrying this step
                    sleep_time = random.uniform(min_sleep, max_sleep) * (attempt + 1)
                    if logger:
                        logger.debug(
                            f"[URL_UTILS]: Retrying resolution step after {sleep_time:.2f}s"
                        )
                    await asyncio.sleep(sleep_time)
                    continue  # Go to next retry attempt for the current redirect step
            else:
                # This 'else' belongs to the 'for attempt' loop, executed if the loop finishes without 'break' (i.e., all retries failed)
                if logger:
                    logger.error(
                        f"[URL_UTILS]: All {retries+1} retry attempts failed for step {redirect_count+1} at URL {resolved_url}"
                    )
                if cache is not None:
                    cache[org_link] = resolved_url
                return resolved_url  # Return the URL that failed all retries

        # If loop finishes because max_redirects is reached
        if logger:
            logger.warning(
                f"[URL_UTILS]: Maximum redirects ({max_redirects}) exceeded for {org_link}. Final URL: {resolved_url}"
            )
        # Cache the result even if max redirects were hit
        if cache is not None:
            cache[org_link] = resolved_url
        return resolved_url


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
    Fetch real URLs for a list of links in batches using asyncio.gather.

    Args:
        session: Shared aiohttp client session.
        urls: List of URLs to resolve.
        headers: HTTP headers for requests.
        proxy_list: List of proxy URLs.
        base: Base URL for fixing relative links.
        max_semaphore: Semaphore controlling overall concurrency for fetch_real_url calls.
        timeout: Timeout for individual requests within fetch_real_url.
        retries: Retries for individual requests within fetch_real_url.
        min_sleep: Minimum delay used within fetch_real_url.
        max_sleep: Maximum delay used within fetch_real_url.
        max_redirects: Max redirects allowed by fetch_real_url.
        logger: Optional logger instance.
        cache: Optional URL cache dictionary passed to fetch_real_url.
        batch_size: Number of URLs to process concurrently in each batch.

    Returns:
        A list containing the resolved URL for each corresponding input URL.
        Order is preserved. Exceptions during resolution are returned as is in the list.
    """
    # Prepare headers (e.g., remove cookies if desired for resolution)
    request_headers = headers.copy()
    if "Cookie" in request_headers:
        del request_headers["Cookie"]

    # Filter out invalid proxies that might be comments or empty lines
    valid_proxies = _filter_valid_proxies(proxy_list, logger, context="Batch fetch")

    all_results = []
    num_batches = (len(urls) + batch_size - 1) // batch_size

    # Process URLs in batches
    for i in range(0, len(urls), batch_size):
        batch_urls = urls[i : i + batch_size]
        if logger:
            logger.info(
                f"[URL_UTILS]: Processing URL resolution batch {i//batch_size + 1}/{num_batches} ({len(batch_urls)} URLs)"
            )

        # Create tasks for resolving URLs in the current batch
        tasks = [
            fetch_real_url(  # Call the single URL resolver for each URL in the batch
                session=session,
                org_link=url,
                headers=request_headers,
                proxy_list=valid_proxies,
                base=base,
                max_semaphore=max_semaphore,  # Pass the shared semaphore
                timeout=timeout,
                retries=retries,
                min_sleep=min_sleep,
                max_sleep=max_sleep,
                max_redirects=max_redirects,
                logger=logger,
                cache=cache,
            )
            for url in batch_urls
        ]

        # Run tasks concurrently for the batch and gather results (including exceptions)
        batch_results = await asyncio.gather(*tasks, return_exceptions=True)
        all_results.extend(batch_results)

        # Optional delay between batches
        if i + batch_size < len(urls):
            batch_delay = random.uniform(min_sleep, max_sleep)
            if logger:
                logger.debug(
                    f"[URL_UTILS]: Waiting {batch_delay:.2f}s before next batch."
                )
            await asyncio.sleep(batch_delay)

    # Process results to handle potential exceptions returned by asyncio.gather
    final_urls = []
    for i, result in enumerate(all_results):
        original_url = urls[i]
        if isinstance(result, Exception):
            if logger:
                logger.error(
                    f"[URL_UTILS]: Exception during batch resolution for {original_url}: {result}"
                )
            final_urls.append(original_url)  # Return original URL on exception
        elif isinstance(result, str):
            final_urls.append(result)  # Append the resolved URL string
        else:
            if logger:
                logger.warning(
                    f"[URL_UTILS]: Unexpected result type for {original_url}: {type(result)}. Using original URL."
                )
            final_urls.append(
                original_url
            )  # Fallback to original URL for unexpected types

    return final_urls
