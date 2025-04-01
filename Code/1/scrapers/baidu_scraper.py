import asyncio
import time
import random
import aiohttp
from bs4 import BeautifulSoup
from typing import List, Dict, Any, Tuple, Optional
from pathlib import Path

from scrapers.base_scraper import BaseScraper
from utils.url_utils import fetch_real_url, batch_fetch_urls


class BaiduScraper(BaseScraper):
    """百度搜索结果抓取器"""

    async def _parse_results(
        self, soup: BeautifulSoup, no_a_title_tag_strip_n: int = 50
    ) -> Tuple[List[Dict[str, Any]], List[str]]:
        """
        解析百度搜索结果页面

        Args:
            soup: BeautifulSoup解析对象
            no_a_title_tag_strip_n: 标题截断长度

        Returns:
            (解析结果列表, 百度中转链接列表)
        """
        if self.logger:
            self.logger.debug("开始解析搜索结果页面")

        # 查找所有搜索结果容器
        results = soup.select(
            "div[class*='result'], div[class*='c-container'], article[class*='c-container']"
        )

        if self.logger:
            self.logger.info(f"找到 {len(results)} 个搜索结果")

        data = []
        baidu_links = []
        seen_entries = set()

        for i, result in enumerate(results):
            # 提取标题和链接
            title_tag = result.find(
                "h3", class_=lambda x: x in ["t", "c-title", None]
            ) or result.find("a", class_=lambda x: x and "title" in x.lower())

            title = (
                title_tag.find("a").get_text(strip=True)
                if title_tag and title_tag.find("a")
                else result.get_text(strip=True)[:no_a_title_tag_strip_n]
            )

            main_link = (
                title_tag.find("a")["href"]
                if title_tag
                and title_tag.find("a")
                and "href" in title_tag.find("a").attrs
                else ""
            )

            # 检查是否为重复结果
            entry_key = (title, main_link)
            if entry_key in seen_entries:
                if self.logger:
                    self.logger.debug(f"跳过重复结果: {title}")
                continue

            seen_entries.add(entry_key)

            # 提取内容摘要和时间
            main_link_content = self._extract_content(result)
            main_link_time = self._extract_time(result)

            # 收集百度中转链接
            baidu_links.append(main_link)

            if self.logger:
                self.logger.debug(f"解析结果 #{i+1}: {title} - {main_link}")

            # 提取其他相关链接
            main_link_moreInfo, related_links_data = self._extract_related_links(
                result, soup, main_link
            )

            # 构建结果项
            data.append(
                {
                    "title": title,
                    "main_link": None,  # 将在后续处理中解析为真实URL
                    "main_link_content": main_link_content,
                    "main_link_time": main_link_time,
                    "main_link_moreInfo": main_link_moreInfo,
                    "related_links": related_links_data,
                }
            )

        return data, baidu_links

    def _extract_content(self, result) -> str:
        """提取搜索结果内容摘要"""
        if "result-op" in result.get("class", []):
            desc = result.find(class_=lambda x: x and "description" in x)
            return desc.get_text(strip=True) if desc else ""
        elif "result" in result.get("class", []):
            content = result.find(
                class_=lambda x: x and "content-right" in x
            ) or result.find("span", class_="c-line-clamp2")
            return content.get_text(strip=True) if content else ""
        else:
            content = result.find("span", class_="c-line-clamp2")
            return content.get_text(strip=True) if content else ""

    def _extract_time(self, result) -> str:
        """提取搜索结果时间信息"""
        time_tag = result.find(
            "span", class_=lambda x: x and "time" in x.lower()
        ) or result.find("span", class_="c-color-gray2")
        return time_tag.get_text(strip=True) if time_tag else ""

    def _extract_related_links(
        self, result, soup, main_link
    ) -> Tuple[Dict[str, str], List[Dict[str, Any]]]:
        """提取相关链接和更多信息"""
        main_link_moreInfo = {}
        related_links_seen = set()
        related_links_data = []

        # 查找所有链接
        for a in result.find_all("a"):
            href = a.get("href", "")

            # 跳过无效链接
            if not href or href.startswith(("javascript:", "#")):
                continue

            text = a.get_text(strip=True)

            # 处理主链接的额外信息
            if href == main_link:
                content = ""

                # 查找站点链接摘要
                for sl in soup.select(".sitelink_summary"):
                    if sl.find("a") and sl.find("a")["href"] == href:
                        p = sl.find("p")
                        content = p.get_text(strip=True) if p else ""
                        break

                # 查找按钮链接文本
                for btn in soup.select('[class*="button"] a'):
                    if btn["href"] == href:
                        content = btn.get_text(strip=True)
                        break

                # 保存主链接额外信息
                if text or content:
                    if text in main_link_moreInfo:
                        if len(content) > len(main_link_moreInfo[text]):
                            main_link_moreInfo[text] = content
                    else:
                        main_link_moreInfo[text] = content
                continue

            # 处理相关链接
            if href not in related_links_seen:
                related_links_seen.add(href)
                content = ""
                time = ""

                # 查找相关链接的摘要和时间
                for sl in soup.select(".sitelink_summary"):
                    if sl.find("a") and sl.find("a")["href"] == href:
                        p = sl.find("p")
                        content = p.get_text(strip=True) if p else ""
                        time_tag = sl.find(
                            "span", class_=lambda x: x and "time" in x.lower()
                        ) or sl.find("span", class_="c-color-gray2")
                        time = time_tag.get_text(strip=True) if time_tag else ""
                        break

                # 查找按钮链接的文本和时间
                for btn in soup.select('[class*="button"] a'):
                    if btn["href"] == href:
                        content = btn.get_text(strip=True)
                        time_tag = (
                            btn.parent.find(
                                "span",
                                class_=lambda x: x and "time" in x.lower(),
                            )
                            or btn.parent.find("span", class_="c-color-gray2")
                            if btn.parent
                            else None
                        )
                        time = time_tag.get_text(strip=True) if time_tag else ""
                        break

                # 构建信息字典
                info_dict = {}
                if text or content:
                    if text in info_dict:
                        if len(content) > len(info_dict[text]):
                            info_dict[text] = content
                    else:
                        info_dict[text] = content

                # 添加到相关链接列表
                related_links_data.append(
                    {
                        "href": href,
                        "info": info_dict,
                        "time": time,
                    }
                )

        return main_link_moreInfo, related_links_data

    async def _process_real_urls(
        self,
        session: aiohttp.ClientSession,
        data: List[Dict[str, Any]],
        baidu_links: List[str],
    ) -> List[Dict[str, Any]]:
        """处理百度中转链接，获取真实URL"""
        if self.logger:
            self.logger.info(f"开始处理 {len(baidu_links)} 个百度中转链接")

        # 为并发控制创建信号量
        semaphore = asyncio.Semaphore(self.semaphore_limit)

        # 创建获取真实URL的任务
        tasks = [
            fetch_real_url(
                session,
                link,
                self.headers,
                self.cookies,
                self.proxies,
                semaphore,
                self.fetch_timeout,
                self.fetch_retries,
                self.min_retries_sleep,
                self.max_retries_sleep,
                self.max_redirects,
                self.logger,
                self.url_cache.cache,
            )
            for link in baidu_links
        ]

        # 并发执行所有任务
        real_urls = await asyncio.gather(*tasks, return_exceptions=True)

        if self.logger:
            self.logger.info(f"已获取 {len(real_urls)} 个真实URL")

        # 更新主链接的真实URL
        for i, entry in enumerate(data):
            entry["main_link"] = real_urls[i]

            # 处理相关链接
            if entry["related_links"]:
                all_related_links = []

                # 批量处理相关链接
                for batch_start in range(0, len(entry["related_links"]), 10):
                    batch = entry["related_links"][batch_start : batch_start + 10]

                    if self.logger:
                        self.logger.debug(
                            f"处理 '{entry['title']}' 的相关链接批次 "
                            f"{batch_start//10 + 1}/{(len(entry['related_links'])-1)//10 + 1}, "
                            f"包含 {len(batch)} 个链接"
                        )

                    batch_links = [link["href"] for link in batch]
                    batch_tasks = [
                        fetch_real_url(
                            session,
                            link,
                            self.headers,
                            self.cookies,
                            self.proxies,
                            semaphore,
                            self.fetch_timeout,
                            self.fetch_retries,
                            self.min_retries_sleep,
                            self.max_retries_sleep,
                            self.max_redirects,
                            self.logger,
                            self.url_cache.cache,
                        )
                        for link in batch_links
                    ]

                    batch_results = await asyncio.gather(
                        *batch_tasks, return_exceptions=True
                    )
                    all_related_links.extend(batch_results)

                    # 在批次之间添加短暂休眠
                    if batch_start + 10 < len(entry["related_links"]):
                        await asyncio.sleep(random.uniform(0.3, 0.6))

                # 更新相关链接的真实URL
                for j, link in enumerate(entry["related_links"]):
                    link["href"] = all_related_links[j]

        # 去重和处理结果
        return self._deduplicate_results(data)

    def _deduplicate_results(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """对搜索结果进行去重处理"""
        if self.logger:
            self.logger.info("开始去重处理搜索结果")

        unique_data = {}

        # 使用URL作为唯一键
        for entry in data:
            main_url = entry["main_link"]

            # 跳过无效URL
            if isinstance(main_url, Exception):
                if self.logger:
                    self.logger.warning(f"跳过错误URL: {str(main_url)}")
                continue

            if not main_url:
                if self.logger:
                    self.logger.debug(f"跳过空URL，标题: {entry['title']}")
                continue

            # 如果URL已存在，合并信息
            if main_url in unique_data:
                existing = unique_data[main_url]

                # 如果新条目标题更长，使用新标题
                if len(entry["title"]) > len(existing["title"]):
                    existing["title"] = entry["title"]

                # 如果新条目内容更长，使用新内容
                if len(entry["main_link_content"]) > len(existing["main_link_content"]):
                    existing["main_link_content"] = entry["main_link_content"]

                # 合并更多信息
                for k, v in entry["main_link_moreInfo"].items():
                    if k not in existing["main_link_moreInfo"]:
                        existing["main_link_moreInfo"][k] = v
                    elif len(v) > len(existing["main_link_moreInfo"][k]):
                        existing["main_link_moreInfo"][k] = v

                # 合并相关链接
                existing_related_urls = {
                    link["href"] for link in existing["related_links"]
                }
                for link in entry["related_links"]:
                    if link["href"] and link["href"] not in existing_related_urls:
                        existing["related_links"].append(link)
                        existing_related_urls.add(link["href"])
            else:
                # 添加新条目
                unique_data[main_url] = entry

        # 转换回列表
        result = list(unique_data.values())

        if self.logger:
            self.logger.info(
                f"去重完成，原 {len(data)} 个结果 -> {len(result)} 个唯一结果"
            )

        return result

    async def scrape(
        self,
        query: str,
        num_pages: int = 1,
        no_a_title_tag_strip_n: int = 50,
        cache_to_file: bool = True,
        cache_file: Optional[Path] = None,
    ) -> List[Dict[str, Any]]:
        """
        抓取百度搜索结果

        Args:
            query: 搜索关键词
            num_pages: 要抓取的页数
            no_a_title_tag_strip_n: 标题截断长度
            cache_to_file: 是否将URL缓存保存到文件
            cache_file: 缓存文件路径

        Returns:
            处理后的搜索结果
        """
        start_time = time.time()
        if self.logger:
            self.logger.info(
                f"开始抓取百度搜索结果，关键词：'{query}'，页数：{num_pages}"
            )

        all_results = []

        async with aiohttp.ClientSession(cookies=self.cookies) as session:
            for page in range(num_pages):
                page_start = page * 10  # 百度默认每页10个结果

                if self.logger:
                    self.logger.info(f"抓取第 {page+1}/{num_pages} 页")

                # 构建请求参数
                params = {
                    "wd": query,  # 搜索关键词
                    "pn": str(page_start),  # 结果偏移量
                    "oq": query,  # 原始查询词
                    "ie": "utf-8",  # 输入编码
                    "usm": "1",  # 用户搜索模式
                    "rsv_pq": str(int(time.time() * 1000)),  # 时间戳
                    "rsv_t": "dc31ReKVFdLpfLEdVpJPPVYuKsRKX4AuOEpIA1daCjmCovnbj7QfeLKQKw",  # 随机值
                }

                # 发送请求获取页面内容
                html_content = await self.get_page(
                    url="https://www.baidu.com/s",
                    params=params,
                    use_proxy=self.use_proxy_for_search,
                )

                if not html_content:
                    if self.logger:
                        self.logger.error(f"第 {page+1} 页获取失败，跳过")
                    continue

                # 解析页面
                soup = BeautifulSoup(html_content, "html.parser")

                # 提取搜索结果
                data, baidu_links = await self._parse_results(
                    soup, no_a_title_tag_strip_n
                )

                if not data:
                    if self.logger:
                        self.logger.warning(f"第 {page+1} 页未发现搜索结果")
                    continue

                # 处理真实URL
                processed_data = await self._process_real_urls(
                    session, data, baidu_links
                )
                all_results.extend(processed_data)

                # 添加页间延迟（避免被封）
                if page < num_pages - 1:
                    delay = random.uniform(1.0, 2.0)
                    if self.logger:
                        self.logger.debug(f"等待 {delay:.2f} 秒后抓取下一页")
                    await asyncio.sleep(delay)

        # 保存URL缓存
        if cache_to_file and cache_file:
            if self.logger:
                self.logger.info(f"保存URL缓存到文件：{cache_file}")
            self.url_cache.save_to_file(cache_file)

        # 统计和日志
        elapsed = time.time() - start_time
        if self.logger:
            self.logger.info(
                f"搜索完成，共获取 {len(all_results)} 个结果，耗时 {elapsed:.2f} 秒"
            )
            self.logger.info(
                f"缓存命中率：{self.url_cache.hits}/{self.url_cache.hits + self.url_cache.misses}"
            )

        return all_results
