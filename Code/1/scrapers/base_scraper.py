import aiohttp
import asyncio
import logging
import random
import time
from typing import List, Dict, Any, Optional, Union
from pathlib import Path

from utils.cache import URLCache
from utils.logging_utils import setup_logger


class BaseScraper:
    """网页抓取器基类"""

    def __init__(
        self,
        headers: Dict[str, str],
        proxies: List[str] = None,
        use_proxy_for_search: bool = False,
        semaphore_limit: int = 25,
        min_delay_between_requests: float = 0.1,
        max_delay_between_requests: float = 0.3,
        fetch_timeout: int = 3,
        fetch_retries: int = 2,
        min_retries_sleep: float = 0.1,
        max_retries_sleep: float = 0.3,
        max_redirects: int = 5,
        cache_size: int = 1000,
        cache_ttl: int = 86400,  # 默认缓存1天
        enable_logging: bool = False,
        log_level: int = logging.INFO,
        log_file: Optional[Union[str, Path]] = None,
        log_to_console: bool = True,
    ):
        """
        初始化基础爬虫

        Args:
            headers: 完整的请求头
            proxies: 代理列表
            use_proxy_for_search: 是否为搜索请求使用代理
            semaphore_limit: 并发请求限制
            min_delay_between_requests: 请求间最小延迟
            max_delay_between_requests: 请求间最大延迟
            fetch_timeout: 请求超时时间
            fetch_retries: 请求失败重试次数
            min_retries_sleep: 重试最小等待时间
            max_retries_sleep: 重试最大等待时间
            max_redirects: 最大重定向次数
            cache_size: 缓存大小
            cache_ttl: 缓存生存时间（秒）
            enable_logging: 是否启用日志
            log_level: 日志级别
            log_file: 日志文件路径
            log_to_console: 是否将日志输出到控制台
        """
        self.headers = headers
        self.proxies = proxies or []
        self.use_proxy_for_search = use_proxy_for_search
        self.semaphore_limit = semaphore_limit
        self.min_delay = min_delay_between_requests
        self.max_delay = max_delay_between_requests
        self.fetch_timeout = fetch_timeout
        self.fetch_retries = fetch_retries
        self.min_retries_sleep = min_retries_sleep
        self.max_retries_sleep = max_retries_sleep
        self.max_redirects = max_redirects
        self.url_cache = URLCache(max_size=cache_size, ttl=cache_ttl)
        self.logger = None

        # 初始化信号量
        self.semaphore = asyncio.Semaphore(semaphore_limit)

        # 初始化统计信息
        self.stats = {
            "requests_total": 0,
            "requests_success": 0,
            "requests_failed": 0,
            "start_time": None,
            "end_time": None,
        }

        if enable_logging:
            self.logger = setup_logger(
                self.__class__.__name__, log_level, log_file, log_to_console
            )
            self.logger.info(f"初始化 {self.__class__.__name__}")

    async def get_page(
        self,
        url: str,
        params: Dict[str, str] = None,
        use_proxy: bool = None,
        headers: Dict[str, str] = None,
        timeout: int = None,
        retries: int = None,
    ) -> Optional[str]:
        """
        获取网页内容

        Args:
            url: 请求URL
            params: URL参数
            use_proxy: 是否使用代理
            headers: 自定义请求头
            timeout: 自定义超时时间
            retries: 自定义重试次数

        Returns:
            网页内容或None（请求失败）
        """
        # 使用传入的参数，或者使用实例默认值
        use_proxy = use_proxy if use_proxy is not None else self.use_proxy_for_search
        headers = headers or self.headers
        timeout = timeout or self.fetch_timeout
        retries = retries if retries is not None else self.fetch_retries

        # 更新统计信息
        self.stats["requests_total"] += 1
        if self.stats["start_time"] is None:
            self.stats["start_time"] = time.time()

        async with aiohttp.ClientSession() as session:
            # 请求前添加随机延迟
            await asyncio.sleep(random.uniform(self.min_delay, self.max_delay))

            # 如果需要代理且有代理列表，随机选择一个代理
            proxy = random.choice(self.proxies) if self.proxies and use_proxy else None

            if self.logger:
                self.logger.debug(
                    f"发送请求: {url}"
                    + (f" 带参数: {params}" if params else "")
                    + (f" 通过代理: {proxy}" if proxy else "")
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
                                    f"请求失败，状态码：{response.status}"
                                )

                            # 如果是最后一次尝试，更新失败统计
                            if attempt == retries:
                                self.stats["requests_failed"] += 1

                            # 对于某些状态码，可能不需要重试
                            if response.status in (404, 403):
                                return None

                            # 对于其他状态码，如果还有重试次数，则继续重试
                            if attempt < retries:
                                sleep_time = random.uniform(
                                    self.min_retries_sleep, self.max_retries_sleep
                                )
                                if self.logger:
                                    self.logger.debug(
                                        f"重试 ({attempt+1}/{retries}) 等待 {sleep_time:.2f}秒"
                                    )
                                await asyncio.sleep(sleep_time)
                                continue

                            return None

                        text = await response.text()
                        if self.logger:
                            self.logger.debug(f"响应长度: {len(text)} 字节")

                        # 更新成功统计
                        self.stats["requests_success"] += 1
                        self.stats["end_time"] = time.time()

                        return text

                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    if attempt == retries:
                        if self.logger:
                            self.logger.error(f"请求过程中发生错误: {str(e)}")
                        self.stats["requests_failed"] += 1
                        return None

                    sleep_time = random.uniform(
                        self.min_retries_sleep, self.max_retries_sleep
                    )
                    if self.logger:
                        self.logger.debug(
                            f"重试 ({attempt+1}/{retries}) 等待 {sleep_time:.2f}秒，错误: {str(e)}"
                        )
                    await asyncio.sleep(sleep_time)

            return None  # 如果所有重试都失败

    def get_stats(self) -> Dict[str, Any]:
        """获取爬虫统计信息"""
        stats = self.stats.copy()

        # 添加运行时间计算
        if stats["start_time"] and stats["end_time"]:
            stats["duration"] = stats["end_time"] - stats["start_time"]
        else:
            stats["duration"] = 0

        # 添加成功率计算
        if stats["requests_total"] > 0:
            stats["success_rate"] = stats["requests_success"] / stats["requests_total"]
        else:
            stats["success_rate"] = 0

        # 添加缓存统计
        if hasattr(self, "url_cache"):
            stats["cache"] = self.url_cache.stats()

        return stats
