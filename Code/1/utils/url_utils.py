import aiohttp
import asyncio
from urllib.parse import urljoin, urlparse
import random
import logging
from typing import List, Dict, Optional


async def fetch_real_url(
    session: aiohttp.ClientSession,
    org_link: str,
    headers: dict,
    cookies: dict,
    proxy_list: List[str],
    semaphore: asyncio.Semaphore,
    timeout: int = 3,
    retries: int = 0,
    min_retries_sleep: float = 0.1,
    max_retries_sleep: float = 0.3,
    max_redirects: int = 5,
    logger: Optional[logging.Logger] = None,
    cache: Optional[Dict[str, str]] = None,
) -> str:
    """
    获取百度链接的真实URL

    Args:
        session: aiohttp会话
        org_link: 原始链接
        headers: 请求头
        cookies: Cookie
        proxy_list: 代理列表
        semaphore: 控制并发的信号量
        timeout: 请求超时时间(秒)
        retries: 失败重试次数
        min_retries_sleep: 重试最小等待时间
        max_retries_sleep: 重试最大等待时间
        max_redirects: 最大重定向次数
        logger: 日志记录器
        cache: URL缓存字典

    Returns:
        解析后的真实URL
    """
    # 使用缓存避免重复解析
    if cache is not None and org_link in cache:
        if logger:
            logger.debug(f"从缓存返回URL: {org_link} -> {cache[org_link]}")
        return cache[org_link]

    # 如果链接为空，直接返回空字符串
    if not org_link:
        if logger:
            logger.debug("空链接，返回空")
        return ""

    # 如果链接不是有效的URL，尝试修正
    if not is_valid_url(org_link):
        fixed_link = fix_url(org_link)
        if logger:
            logger.debug(f"修正链接格式: {org_link} -> {fixed_link}")
        org_link = fixed_link

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

                    async with session.get(
                        current_url,
                        headers=headers,
                        cookies=cookies,
                        allow_redirects=False,  # 手动处理重定向以提高控制
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

                            # 处理相对路径的重定向URL
                            current_url = urljoin(str(response.url), location)
                            redirect_count += 1

                            if logger:
                                logger.debug(
                                    f"检测到重定向 ({redirect_count}/{max_redirects}): {current_url}"
                                )

                            # 短暂暂停，避免过快请求
                            await asyncio.sleep(0.1)
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
                        sleep_time = random.uniform(
                            min_retries_sleep, max_retries_sleep
                        )
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


def is_valid_url(url: str) -> bool:
    """
    检查URL是否有效

    Args:
        url: 要检查的URL

    Returns:
        是否是有效的URL
    """
    if not url:
        return False

    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except Exception:
        return False


def fix_url(url: str) -> str:
    """
    修复不完整的URL

    Args:
        url: 可能不完整的URL

    Returns:
        修复后的URL
    """
    if not url:
        return ""

    # 如果链接不以http://或https://开头，进行修正
    if not url.startswith(("http://", "https://")):
        url = urljoin("https://www.baidu.com/", url)

    return url


async def batch_fetch_urls(
    session: aiohttp.ClientSession,
    urls: List[str],
    headers: dict,
    cookies: dict,
    proxy_list: List[str],
    semaphore: asyncio.Semaphore,
    timeout: int = 3,
    retries: int = 0,
    min_retries_sleep: float = 0.1,
    max_retries_sleep: float = 0.3,
    max_redirects: int = 5,
    logger: Optional[logging.Logger] = None,
    cache: Optional[Dict[str, str]] = None,
    batch_size: int = 10,
) -> List[str]:
    """
    批量获取真实URL

    Args:
        session: aiohttp会话
        urls: 原始URL列表
        headers: 请求头
        cookies: Cookie
        proxy_list: 代理列表
        semaphore: 控制并发的信号量
        timeout: 请求超时时间(秒)
        retries: 失败重试次数
        min_retries_sleep: 重试最小等待时间
        max_retries_sleep: 重试最大等待时间
        max_redirects: 最大重定向次数
        logger: 日志记录器
        cache: URL缓存字典
        batch_size: 批处理大小

    Returns:
        真实URL列表
    """
    results = []

    # 分批处理URL
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
                headers,
                cookies,
                proxy_list,
                semaphore,
                timeout,
                retries,
                min_retries_sleep,
                max_retries_sleep,
                max_redirects,
                logger,
                cache,
            )
            for url in batch
        ]

        batch_results = await asyncio.gather(*tasks, return_exceptions=True)
        results.extend(batch_results)

        # 在批次之间添加短暂休眠，避免过度请求
        if i + batch_size < len(urls):
            await asyncio.sleep(random.uniform(0.5, 1.0))

    return results
