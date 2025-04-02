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

        results = soup.select(
            "div[class*='result'], div[class*='c-container'], article[class*='c-container']"
        )
        if self.logger:
            self.logger.info(f"找到 {len(results)} 个搜索结果")

        data = []
        baidu_links = []
        seen_entries = set()

        for i, result in enumerate(results):
            search_data = self._process_normal_result(
                result, soup, seen_entries, no_a_title_tag_strip_n
            )
            if search_data:
                data.append(search_data["data"])
                baidu_links.append(search_data["link"])

        return data, baidu_links

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
            "span[class*='content-right']",
            "div[class*='group-sub-abs']",
            "div[class*='description'], div[class*='desc']",
            "div[class*='content'], div[class*='cont']",
            "span[class*='line-clamp'], span[class*='clamp']",
            "div[class*='abs'], p[class*='abs']",
            "div[class*='ala-container'] p[class*='paragraph']",
        ]

        for selector in content_selectors:
            content = result.select_one(selector)
            if content:
                return content.get_text(strip=True)
        return ""

    def _extract_time(self, result) -> str:
        """提取搜索结果时间信息"""
        time_tag = result.select_one("span[class*='time'], span.c-color-gray2")
        return (
            time_tag.get_text(strip=True)
            if time_tag
            and any(char.isdigit() for char in time_tag.get_text(strip=True))
            else ""
        )

    def _extract_related_links(
        self, result
    ) -> Tuple[Dict[str, str], List[Dict[str, Any]]]:
        """提取相关链接和主链接的额外信息"""
        main_link_moreInfo = {}
        related_links = []

        sitelink_container = result.select_one("div[class*='sitelink']")
        if sitelink_container:
            for summary in sitelink_container.select("div[class*='summary']"):
                link_tag = summary.select_one("a")
                if link_tag and link_tag.get("href"):
                    href = link_tag.get("href")
                    text = link_tag.get_text(strip=True)
                    if text:
                        content = summary.select_one(
                            "p.c-color-text, p[class*='line-clamp']"
                        )
                        related_links.append(
                            {
                                "text": text,
                                "href": href,
                                "content": (
                                    content.get_text(strip=True) if content else ""
                                ),
                                "time": "",
                            }
                        )

        news_items = result.select("[class*='item'], [class*='content']")
        for item in news_items:
            title_link = item.select_one("a[class*='title'], a[class*='sub-title']")
            if title_link and title_link.get("href"):
                href = title_link.get("href")
                text = title_link.get_text(strip=True)
                if text:
                    related_links.append(
                        {
                            "text": text,
                            "href": href,
                            "content": self._extract_content(item),
                            "time": self._extract_time(item),
                        }
                    )

        return main_link_moreInfo, related_links

    def _normalize_url(self, url: str) -> str:
        """简化的URL规范化方法"""
        if not url or isinstance(url, Exception):
            return ""
        if url.startswith("/"):
            url = f"https://www.baidu.com{url}"
        if "://" in url:
            url = url.split("://", 1)[1]
        url = url.lower().rstrip("/")
        if url.startswith("www."):
            url = url[4:]
        return url

    def _merge_entries(self, target: Dict[str, Any], source: Dict[str, Any]) -> None:
        """合并两个搜索结果条目"""
        if len(source.get("title", "")) > len(target.get("title", "")):
            target["title"] = source["title"]
        source_content = source.get("main_link_content", "")
        if source_content and (
            not target.get("main_link_content")
            or len(source_content) > len(target["main_link_content"])
        ):
            target["main_link_content"] = source_content
        source_time = source.get("main_link_time", "")
        if source_time and not target.get("main_link_time"):
            target["main_link_time"] = source_time
        for k, v in source.get("main_link_moreInfo", {}).items():
            if k not in target.get("main_link_moreInfo", {}) or len(v) > len(
                target["main_link_moreInfo"][k]
            ):
                target["main_link_moreInfo"][k] = v
        target["related_links"].extend(source.get("related_links", []))

    def _initial_deduplicate(
        self, data: List[Dict[str, Any]], baidu_links: List[str]
    ) -> List[Dict[str, Any]]:
        """优化的初步去重方法"""
        if not data:
            return []
        if self.logger:
            self.logger.info("开始进行初步去重处理")

        processed_data = []
        for i, entry in enumerate(data):
            if i >= len(baidu_links):
                continue
            baidu_link = baidu_links[i]
            if baidu_link.startswith("/"):
                baidu_link = f"https://www.baidu.com{baidu_link}"
            for link in entry.get("related_links", []):
                if link["href"].startswith("/"):
                    link["href"] = f"https://www.baidu.com{link['href']}"
            entry_copy = entry.copy()
            entry_copy["main_link"] = baidu_link
            processed_data.append(entry_copy)

        unique_entries = {}
        for entry in processed_data:
            norm_url = self._normalize_url(entry["main_link"])
            title = entry.get("title", "").lower()
            merged = False
            for key, existing in list(unique_entries.items()):
                existing_title = existing.get("title", "").lower()
                if (
                    title
                    and existing_title
                    and (
                        title == existing_title
                        or (
                            len(title) > 10
                            and len(existing_title) > 10
                            and (title in existing_title or existing_title in title)
                        )
                    )
                ):
                    self._merge_entries(existing, entry)
                    merged = True
                    break
            if not merged:
                unique_entries[norm_url] = entry

        result = list(unique_entries.values())
        if self.logger:
            self.logger.info(f"初步去重完成: {len(data)} → {len(result)}")
        return result

    async def _process_real_urls(
        self,
        session: aiohttp.ClientSession,
        data: List[Dict[str, Any]],
        baidu_links: List[str],
    ) -> List[Dict[str, Any]]:
        """优化的URL处理方法"""
        if not baidu_links:
            return data
        if self.logger:
            self.logger.info(f"开始处理 {len(baidu_links)} 个中转链接")

        data = self._initial_deduplicate(data, baidu_links)
        if self.logger:
            self.logger.info(f"预处理后需要处理 {len(data)} 个结果")

        all_links = []
        link_map = {}
        for i, entry in enumerate(data):
            main_link = entry.get("main_link")
            if main_link:
                all_links.append(main_link)
                link_map[(i, "main")] = len(all_links) - 1
            for j, link in enumerate(entry.get("related_links", [])):
                href = link.get("href")
                if href and href not in all_links:
                    all_links.append(href)
                    link_map[(i, j)] = len(all_links) - 1

        semaphore = asyncio.Semaphore(self.semaphore_limit)
        headers = {k: v for k, v in self.headers.items() if k != "Cookie"}
        batch_size = min(20, max(5, self.semaphore_limit))
        real_urls = await batch_fetch_urls(
            session,
            all_links,
            headers,
            self.proxies,
            semaphore,
            self.fetch_timeout,
            self.fetch_retries,
            self.min_retries_sleep,
            self.max_retries_sleep,
            self.max_redirects,
            self.logger,
            self.url_cache.cache,
            batch_size,
        )

        for (i, pos), url_idx in link_map.items():
            if url_idx < len(real_urls):
                if pos == "main":
                    data[i]["main_link"] = real_urls[url_idx]
                else:
                    data[i]["related_links"][pos]["href"] = real_urls[url_idx]

        return await self._deduplicate_results(data)

    async def _deduplicate_results(
        self, data: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """优化的精确去重方法"""
        if not data:
            return []
        if self.logger:
            self.logger.info("开始进行精确去重处理")

        unique_data = {}
        for entry in data:
            main_url = entry.get("main_link")
            if isinstance(main_url, Exception) or not main_url:
                continue
            norm_url = self._normalize_url(main_url)
            if not norm_url:
                continue
            if norm_url in unique_data:
                self._merge_entries(unique_data[norm_url], entry)
            else:
                entry_copy = entry.copy()
                entry_copy["main_link"] = main_url
                unique_data[norm_url] = entry_copy

        for url, entry in unique_data.items():
            seen_rel_urls = set()
            filtered_links = []
            for link in entry.get("related_links", []):
                rel_url = link.get("href")
                if isinstance(rel_url, Exception) or not rel_url:
                    continue
                norm_rel_url = self._normalize_url(rel_url)
                if not norm_rel_url or norm_rel_url == url:
                    continue
                if norm_rel_url not in seen_rel_urls:
                    seen_rel_urls.add(norm_rel_url)
                    filtered_links.append(link)
            entry["related_links"] = filtered_links

        result = list(unique_data.values())
        if self.logger:
            self.logger.info(f"精确去重完成: {len(data)} → {len(result)}")
        return result

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

                soup = BeautifulSoup(html_content, "html.parser")
                data, baidu_links = self._parse_results(soup, no_a_title_tag_strip_n)
                if not data:
                    if self.logger:
                        self.logger.warning(f"第 {page+1} 页未发现搜索结果")
                    continue
                processed_data = await self._process_real_urls(
                    session, data, baidu_links
                )
                all_results.extend(processed_data)

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
