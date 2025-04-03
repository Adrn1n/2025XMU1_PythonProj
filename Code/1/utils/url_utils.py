from urllib.parse import urljoin, urlparse
import aiohttp
import asyncio
from typing import Dict, List, Optional
import logging
import random


def is_valid_url(url: str) -> bool:
    if not url:
        return False
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except Exception:
        return False


def fix_url(url: str, base: str) -> str:
    """
    修复不完整的URL

    Args:
        url: 可能不完整的URL
        head: 基准URL，用于拼接相对路径

    Returns:
        修复后的URL

    Raises:
        ValueError: 如果head不是有效的URL
    """
    if not url:
        return ""

    if not is_valid_url(base):
        raise ValueError(f"Invalid base URL: {base}")

    if not url.startswith(("http://", "https://")):
        try:
            url = urljoin(base, url)
        except Exception:
            return url  # 如果拼接失败，返回原URL

    return url


async def fetch_real_url(
    session: aiohttp.ClientSession,
    org_link: str,
    headers: dict,
    proxy_list: List[str],
    base: str,
    semaphore: asyncio.Semaphore,
    timeout: int = 3,
    retries: int = 0,
    min_sleep: float = 0.1,
    max_sleep: float = 0.3,
    max_redirects: int = 5,
    logger: Optional[logging.Logger] = None,
    cache: Optional[Dict[str, str]] = None,
) -> str:
    if not org_link:
        if logger:
            logger.debug("空链接，返回空")
        return ""

    if cache and org_link in cache:
        if logger:
            logger.debug(f"从缓存返回URL: {org_link} -> {cache[org_link]}")
        return cache[org_link]

    if not is_valid_url(org_link):
        try:
            fixed_link = fix_url(org_link, base)
            if logger:
                logger.debug(f"修正链接格式: {org_link} -> {fixed_link}")
            org_link = fixed_link
        except ValueError as e:
            if logger:
                logger.error(f"基准URL无效: {str(e)}")
            return org_link

    request_headers = headers.copy()
    if "Cookie" in request_headers:
        del request_headers["Cookie"]

    async with semaphore:
        current_url = org_link
        redirect_count = 0

        while redirect_count < max_redirects:
            for attempt in range(retries + 1):
                proxy = random.choice(proxy_list) if proxy_list else None
                try:
                    if logger:
                        logger.debug(
                            f"尝试获取URL: {current_url}"
                            + (f" 通过代理: {proxy}" if proxy else "")
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
                                    logger.warning(f"未找到重定向地址: {org_link}")
                                if cache is not None:
                                    cache[org_link] = org_link
                                return org_link

                            current_url = urljoin(str(response.url), location)
                            redirect_count += 1

                            if logger:
                                logger.debug(
                                    f"检测到重定向 ({redirect_count}/{max_redirects}): {current_url}"
                                )

                            await asyncio.sleep(random.uniform(min_sleep, max_sleep))
                            break
                        else:
                            result = str(response.url)
                            if logger:
                                logger.debug(f"获取到真实URL: {org_link} -> {result}")
                            if cache is not None:
                                cache[org_link] = result
                            return result

                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    if attempt == retries:
                        if logger:
                            logger.error(
                                f"链接解析失败 (尝试 {attempt+1}/{retries+1}): {current_url}, "
                                f"代理: {proxy}, 错误: {str(e)}"
                            )
                    if attempt < retries:
                        sleep_time = random.uniform(min_sleep, max_sleep)
                        if logger:
                            logger.debug(
                                f"重试 {attempt+1}/{retries}，等待 {sleep_time:.2f}秒"
                            )
                        await asyncio.sleep(sleep_time)
                    continue
            else:
                if logger:
                    logger.warning(f"重试次数耗尽，仍未解析: {current_url}")
                if cache is not None:
                    cache[org_link] = current_url
                return current_url

        if logger:
            logger.warning(f"超过最大重定向次数: {org_link}")
        if cache is not None:
            cache[org_link] = current_url
        return current_url


async def batch_fetch_real_urls(
    session: aiohttp.ClientSession,
    urls: List[str],
    headers: dict,
    proxy_list: List[str],
    base: str,
    semaphore: asyncio.Semaphore,
    timeout: int = 3,
    retries: int = 0,
    min_sleep: float = 0.1,
    max_sleep: float = 0.3,
    max_redirects: int = 5,
    logger: Optional[logging.Logger] = None,
    cache: Optional[Dict[str, str]] = None,
    batch_size: int = 10,
) -> List[str]:
    request_headers = headers.copy()
    if "Cookie" in request_headers:
        del request_headers["Cookie"]

    results = []
    for i in range(0, len(urls), batch_size):
        batch = urls[i : i + batch_size]
        if logger:
            logger.debug(
                f"处理URL批次 {i//batch_size + 1}/{(len(urls)-1)//batch_size + 1}, 包含 {len(batch)} 个URL"
            )

        tasks = [
            fetch_real_url(
                session,
                url,
                request_headers,
                proxy_list,
                base,
                semaphore,
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


def normalize_url(url: str, base: str, strip_params: bool = False) -> str:
    """
    规范化 URL，支持基于 base 的相对路径解析

    Args:
        url: 要规范化的 URL
        base: 基准 URL, 用于解析相对路径
        strip_params: 是否去除 URL 参数

    Returns:
        规范化后的 URL
    """
    if not url or isinstance(url, Exception):
        return ""

    # 如果 URL 无效，尝试用 base 修复
    if not is_valid_url(url):
        url = fix_url(url, base)

    try:
        parsed = urlparse(url)

        # 确保有协议和域名
        scheme = parsed.scheme.lower() or "https"  # 默认使用 https
        netloc = parsed.netloc.lower()

        # 去掉 www 前缀
        if netloc.startswith("www."):
            netloc = netloc[4:]

        # 规范化路径，去除尾部斜杠和冗余段
        path = parsed.path.rstrip("/")
        if not path:
            path = "/"

        # 是否保留参数
        query = "" if strip_params else parsed.query

        # 重建 URL
        normalized = f"{scheme}://{netloc}{path}"
        if query:
            normalized += f"?{query}"

        return normalized
    except Exception:
        return url  # 解析失败返回原 URL
