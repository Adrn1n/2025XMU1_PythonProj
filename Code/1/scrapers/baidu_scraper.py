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

    def _parse_results(
        self, soup: BeautifulSoup, no_a_title_tag_strip_n: int = 50
    ) -> Tuple[List[Dict[str, Any]], List[str]]:
        if self.logger:
            self.logger.debug("开始解析搜索结果页面")

        try:
            results = soup.select(
                "div[class*='result'], div[class*='c-container'], article[class*='c-container']"
            )

            if self.logger:
                self.logger.info(f"找到 {len(results)} 个搜索结果")

            data = []
            baidu_links = []
            seen_entries = set()

            for i, result in enumerate(results):
                try:
                    search_data = self._process_normal_result(
                        result, soup, seen_entries, no_a_title_tag_strip_n
                    )
                    if search_data:
                        data.append(search_data["data"])
                        baidu_links.append(search_data["link"])

                except Exception as e:
                    if self.logger:
                        self.logger.error(f"处理结果 #{i+1} 时出错: {str(e)}")
                    continue

            return data, baidu_links
        except Exception as e:
            if self.logger:
                self.logger.error(f"解析结果时出错: {str(e)}")
            return [], []

    def _process_normal_result(
        self, result, soup, seen_entries, no_a_title_tag_strip_n
    ) -> Optional[Dict]:
        """处理普通搜索结果"""
        title_tag = result.find(
            "h3",
            class_=lambda x: x and any(c in x for c in ["t", "c-title"]) if x else True,
        ) or result.find(
            "a", class_=lambda x: x and "title" in x.lower() if x else False
        )

        title = ""
        main_link = ""

        if title_tag:
            a_tag = title_tag.find("a") if title_tag.name != "a" else title_tag
            if a_tag:
                title = a_tag.get_text(strip=True)
                if "href" in a_tag.attrs:
                    main_link = a_tag["href"]

        if not title:
            title = result.get_text(strip=True)[:no_a_title_tag_strip_n]

        entry_key = (title, main_link)
        if entry_key in seen_entries or not main_link:
            if self.logger and title:
                self.logger.debug(f"跳过重复结果: {title}")
            return None

        seen_entries.add(entry_key)

        main_link_content = self._extract_content(result)
        main_link_time = self._extract_time(result)
        main_link_moreInfo, related_links_data = self._extract_related_links(result)

        if self.logger:
            self.logger.debug(f"解析普通结果: {title} - {main_link}")

        return {
            "data": {
                "title": title,
                "main_link": None,
                "main_link_content": main_link_content,
                "main_link_time": main_link_time,
                "main_link_moreInfo": main_link_moreInfo,
                "related_links": related_links_data,
            },
            "link": main_link,
        }

    def _extract_content(self, result) -> str:
        """提取搜索结果内容摘要"""
        if result.select("div[class*='render-item']"):
            return ""

        content_selectors = [
            lambda r: r.select_one("span[class*='content-right']"),
            lambda r: r.select_one("div[class*='group-sub-abs']"),
            lambda r: r.select_one("div[class*='description'], div[class*='desc']"),
            lambda r: r.select_one("div[class*='content'], div[class*='cont']"),
            lambda r: r.select_one("span[class*='line-clamp'], span[class*='clamp']"),
            lambda r: r.select_one("div[class*='abs'], p[class*='abs']"),
            lambda r: r.select_one("div[class*='ala-container'] p[class*='paragraph']"),
        ]

        for selector in content_selectors:
            content = selector(result)
            if content:
                return content.get_text(strip=True)

        return ""

    def _extract_time(self, result) -> str:
        """提取搜索结果时间信息"""
        time_tag = result.select_one("span[class*='time'], span.c-color-gray2")
        if time_tag and any(char.isdigit() for char in time_tag.get_text(strip=True)):
            return time_tag.get_text(strip=True)
        return ""

    def _extract_related_links(
        self, result
    ) -> Tuple[Dict[str, str], List[Dict[str, Any]]]:
        """提取相关链接和主链接的额外信息"""
        main_link_moreInfo = {}
        related_links = []

        try:
            # 查找结构化的相关链接容器
            sitelink_container = result.select_one("div[class*='sitelink']")
            if sitelink_container:
                for summary in sitelink_container.select("div[class*='summary']"):
                    link_tag = summary.select_one("a")
                    if not link_tag or not link_tag.get("href"):
                        continue
                    href = link_tag.get("href")
                    text = link_tag.get_text(strip=True)

                    if not text:
                        continue

                    content_tag = summary.select_one(
                        "p.c-color-text, p[class*='line-clamp']"
                    )
                    content = content_tag.get_text(strip=True) if content_tag else ""

                    related_links.append(
                        {"text": text, "href": href, "content": content, "time": ""}
                    )

            # 查找新闻项目和其他相关内容
            news_items = result.select(
                "[class*='render-item'], [class*='group-content'], [class*='blog-item']"
            )
            for item in news_items:
                title_link = item.select_one(
                    "a[class*='title'], a[class*='sub-title'], a.c-font-medium"
                )
                if not title_link or not title_link.get("href"):
                    continue

                href = title_link.get("href")
                text = title_link.get_text(strip=True)

                if not text:
                    continue

                content = self._extract_content(item)
                time = self._extract_time(item)
                related_links.append(
                    {"text": text, "href": href, "content": content, "time": time}
                )

            # 补充遍历其他可能的链接
            if not related_links:
                for a in result.find_all("a"):
                    href = a.get("href", "")
                    text = a.get_text(strip=True)
                    if not href or not text:
                        continue

                    # 检查链接是否已存在
                    if any(link["href"] == href for link in related_links):
                        continue

                    # 查找链接周围的内容和时间信息
                    parent = a.find_parent()
                    content = ""
                    time = ""

                    if parent:
                        time_tag = parent.select_one(
                            "span[class*='time'], span.c-color-gray2"
                        )
                        if time_tag and any(
                            char.isdigit() for char in time_tag.get_text(strip=True)
                        ):
                            time = time_tag.get_text(strip=True)

                        content_tag = parent.select_one(
                            "div[class*='content'], div[class*='desc'], p[class*='content']"
                        )
                        if content_tag:
                            content = content_tag.get_text(strip=True)

                    related_links.append(
                        {"text": text, "href": href, "content": content, "time": time}
                    )

            return main_link_moreInfo, related_links
        except Exception as e:
            if self.logger:
                self.logger.error(f"提取相关链接时出错: {str(e)}")
            return {}, []

    async def _process_real_urls(
        self,
        session: aiohttp.ClientSession,
        data: List[Dict[str, Any]],
        baidu_links: List[str],
    ) -> List[Dict[str, Any]]:
        """处理百度中转链接，获取真实URL"""
        if not baidu_links:
            return data

        if self.logger:
            self.logger.info(f"开始处理 {len(baidu_links)} 个百度中转链接")

        try:
            semaphore = asyncio.Semaphore(self.semaphore_limit)
            request_headers = {k: v for k, v in self.headers.items() if k != "Cookie"}

            all_links = baidu_links.copy()
            link_indices = {link: i for i, link in enumerate(baidu_links)}
            rel_link_to_entries = {}

            for i, entry in enumerate(data):
                for j, link in enumerate(entry.get("related_links", [])):
                    href = link["href"]
                    if href and href not in all_links:
                        all_links.append(href)
                        rel_link_to_entries[(i, j)] = len(all_links) - 1

            real_urls = [None] * len(all_links)
            batch_size = min(20, max(5, self.semaphore_limit))

            for batch_start in range(0, len(all_links), batch_size):
                batch_end = min(batch_start + batch_size, len(all_links))
                batch = [link for link in all_links[batch_start:batch_end] if link]
                if not batch:
                    continue

                batch_tasks = [
                    fetch_real_url(
                        session,
                        link,
                        request_headers,
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
                    for link in batch
                ]

                batch_results = await asyncio.gather(
                    *batch_tasks, return_exceptions=True
                )
                for i, result in enumerate(batch_results):
                    if batch_start + i < len(real_urls):
                        real_urls[batch_start + i] = result

                if batch_end < len(all_links):
                    await asyncio.sleep(random.uniform(0.2, 0.5))

            for i, entry in enumerate(data):
                if i < len(baidu_links):
                    entry["main_link"] = real_urls[i]

            for (entry_idx, link_idx), url_idx in rel_link_to_entries.items():
                if (
                    entry_idx < len(data)
                    and "related_links" in data[entry_idx]
                    and link_idx < len(data[entry_idx]["related_links"])
                    and url_idx < len(real_urls)
                ):
                    data[entry_idx]["related_links"][link_idx]["href"] = real_urls[
                        url_idx
                    ]

            return self._deduplicate_results(data)
        except Exception as e:
            if self.logger:
                self.logger.error(f"处理真实URL时出错: {str(e)}")
            return data

    def _deduplicate_results(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not data:
            return []

        if self.logger:
            self.logger.info("开始去重处理搜索结果")

        try:
            unique_data = {}
            for entry in data:
                main_url = entry.get("main_link")
                if isinstance(main_url, Exception) or not main_url:
                    continue
                normalized_url = self._normalize_url(main_url)
                if not normalized_url:
                    continue
                if normalized_url in unique_data:
                    existing = unique_data[normalized_url]
                    if len(entry.get("title", "")) > len(existing.get("title", "")):
                        existing["title"] = entry["title"]
                    content = entry.get("main_link_content", "")
                    if content and (
                        not existing.get("main_link_content")
                        or len(content) > len(existing["main_link_content"])
                    ):
                        existing["main_link_content"] = content
                    time_info = entry.get("main_link_time", "")
                    if time_info and not existing.get("main_link_time"):
                        existing["main_link_time"] = time_info
                    for k, v in entry.get("main_link_moreInfo", {}).items():
                        if k in existing["main_link_moreInfo"]:
                            if len(v) > len(existing["main_link_moreInfo"][k]):
                                existing["main_link_moreInfo"][k] = v
                        else:
                            existing["main_link_moreInfo"][k] = v
                    existing["related_links"].extend(entry.get("related_links", []))
                else:
                    unique_data[normalized_url] = entry.copy()
                    unique_data[normalized_url]["main_link"] = main_url

            for url, entry in unique_data.items():
                processed_related_links = set()
                filtered_related_links = []
                for link in entry.get("related_links", []):
                    rel_url = link.get("href")
                    if isinstance(rel_url, Exception) or not rel_url:
                        continue
                    normalized_rel_url = self._normalize_url(rel_url)
                    if not normalized_rel_url or normalized_rel_url == url:
                        continue
                    if normalized_rel_url not in processed_related_links:
                        processed_related_links.add(normalized_rel_url)
                        filtered_related_links.append(link)
                entry["related_links"] = filtered_related_links

            final_result = list(unique_data.values())
            if self.logger:
                self.logger.info(
                    f"去重完成，原 {len(data)} 个结果 -> {len(final_result)} 个唯一结果"
                )
                total_related = sum(
                    len(entry.get("related_links", [])) for entry in final_result
                )
                self.logger.info(f"共 {total_related} 个相关链接")
            return final_result
        except Exception as e:
            if self.logger:
                self.logger.error(f"去重过程中出错: {str(e)}")
            return data

    def _normalize_url(self, url: str) -> str:
        """规范化URL以便更好地匹配相同内容的URL"""
        if not url or isinstance(url, Exception):
            return ""
        url = url.lower()
        if "://" in url:
            url = url.split("://", 1)[1]
        url = url.rstrip("/")
        if url.startswith("www."):
            url = url[4:]
        return url

    async def scrape(
        self,
        query: str,
        num_pages: int = 1,
        no_a_title_tag_strip_n: int = 50,
        cache_to_file: bool = True,
        cache_file: Optional[Path] = None,
    ) -> List[Dict[str, Any]]:
        start_time = time.time()
        if self.logger:
            self.logger.info(
                f"开始抓取百度搜索结果，关键词：'{query}'，页数：{num_pages}"
            )
        all_results = []

        try:
            async with aiohttp.ClientSession() as session:
                for page in range(num_pages):
                    page_start = page * 10
                    if self.logger:
                        self.logger.info(f"抓取第 {page+1}/{num_pages} 页")

                    params = {
                        "wd": query,
                        "pn": str(page_start),
                        "oq": query,
                        "ie": "utf-8",
                        "usm": "1",
                        "rsv_pq": str(int(time.time() * 1000)),
                        "rsv_t": "dc31ReKVFdLpfLEdVpJPPVYuKsRKX4AuOEpIA1daCjmCovnbj7QfeLKQKw",
                        "rqlang": "cn",
                        "rsv_enter": "1",
                        "rsv_dl": "tb",
                    }

                    current_headers = self.headers.copy()
                    if "Referer" in current_headers:
                        current_headers["Referer"] = "https://www.baidu.com/"

                    html_content = await self.get_page(
                        url="https://www.baidu.com/s",
                        params=params,
                        use_proxy=self.use_proxy_for_search,
                        headers=current_headers,
                    )
                    if not html_content:
                        if self.logger:
                            self.logger.error(f"第 {page+1} 页获取失败，跳过")
                        continue

                    try:
                        soup = BeautifulSoup(html_content, "html.parser")
                        data, baidu_links = self._parse_results(
                            soup, no_a_title_tag_strip_n
                        )
                        if not data:
                            if self.logger:
                                self.logger.warning(f"第 {page+1} 页未发现搜索结果")
                            continue
                        processed_data = await self._process_real_urls(
                            session, data, baidu_links
                        )
                        all_results.extend(processed_data)
                    except Exception as e:
                        if self.logger:
                            self.logger.error(f"第 {page+1} 页处理错误: {str(e)}")
                        continue

                    if page < num_pages - 1:
                        delay = random.uniform(1.0, 2.0)
                        if self.logger:
                            self.logger.debug(f"等待 {delay:.2f} 秒后抓取下一页")
                        await asyncio.sleep(delay)

            if cache_to_file and cache_file:
                if self.logger:
                    self.logger.info(f"保存URL缓存到文件：{cache_file}")
                self.url_cache.save_to_file(cache_file)

            elapsed = time.time() - start_time
            if self.logger:
                self.logger.info(
                    f"搜索完成，共获取 {len(all_results)} 个结果，耗时 {elapsed:.2f} 秒"
                )
                self.logger.info(
                    f"缓存命中率：{self.url_cache.hits}/{self.url_cache.hits + self.url_cache.misses}"
                )
            return all_results
        except Exception as e:
            if self.logger:
                self.logger.error(f"搜索过程中发生错误：{str(e)}")
            return all_results
