from typing import Any, Dict, List, Optional, Tuple
from bs4 import BeautifulSoup
import asyncio
import aiohttp
from pathlib import Path
import time
import random

from scrapers.base_scraper import BaseScraper
from utils.url_utils import batch_fetch_real_urls


class BaiduScraper(BaseScraper):
    """百度搜索结果抓取器"""

    def extract_main_title_and_link(self, result) -> Tuple[str, str]:
        """提取标题和链接"""
        title_tag = result.find("h3", class_=lambda x: x and "title" or "t" in x)
        if title_tag:
            a_tag = title_tag.find("a")
            if a_tag:
                return a_tag.get_text(strip=True), a_tag.get("href", "")
        return "", ""

    def extract_main_content(self, result) -> str:
        """提取搜索结果内容摘要"""
        # 调整选择器顺序和匹配逻辑
        selectors = [
            ("span[class*='content-right']", 0),  # 优先匹配包含content-right的span
            ("div[class*='desc']", 0),
            ("div[class*='text']", 0),
            ("span[class*='text']", 0),
        ]

        for selector, idx in selectors:
            elements = result.select(selector)
            if elements:
                try:
                    # 取第一个匹配元素的文本
                    return elements[idx].get_text(strip=True)
                except IndexError:
                    continue
        return ""

    def extract_time(self, result) -> str:
        """提取时间信息；如果匹配多个返回空"""
        for selector in ["span[class*='time']", "span.c-color-gray2", "span.n2n9e2q"]:
            elements = result.select(selector)
            if elements and selector == "span.c-color-gray2":
                # 过滤只有class属性且值为c-color-gray2的元素
                filtered_elements = [
                    el
                    for el in elements
                    if len(el.attrs) == 1 and el.get("class") == ["c-color-gray2"]
                ]
                if len(filtered_elements) == 1:  # 确保只有一个匹配项
                    return filtered_elements[0].get_text(strip=True)
                elif len(filtered_elements) > 1:  # 如果多个匹配项，返回空
                    return ""
            elif elements:
                if len(elements) == 1:  # 其他选择器也确保只有一个匹配项
                    return elements[0].get_text(strip=True)
                elif len(elements) > 1:  # 如果多个匹配项，返回空
                    return ""
        return ""

    def extract_source(self, result) -> str:
        """提取来源信息；如果匹配多个返回空"""
        for selector in [
            "div[class*='showurl'], div[class*='source-text']",
            "span[class*='showurl'], span[class*='source-text'], span.c-color-gray",
        ]:
            elements = result.select(selector)
            if elements:
                return elements[0].get_text(strip=True) if len(elements) == 1 else ""
        return ""

    def extract_related_links(self, result) -> List[Dict[str, Any]]:
        """提取相关链接：遍历结果中的所有a标签，排除主标题链接，提取链接、内容、来源及时间"""
        related_links = []
        # 使用更完善的标题匹配条件
        main_title = result.find(
            "h3", class_=lambda x: x and any(kw in str(x) for kw in ["title", "t"])
        )

        for link_tag in result.find_all("a"):
            if main_title and link_tag in main_title.find_all("a"):
                continue
            href = link_tag.get("href", "")
            title = link_tag.get_text(strip=True)

            # 寻找链接的容器元素 - 优先查找特定类型的容器
            container = None
            current = link_tag.parent
            while current and current != result:
                if current.name == "div" and current.get("class"):
                    class_str = " ".join(current.get("class", []))
                    # 模糊匹配常见的容器类名关键词
                    if any(
                        kw in class_str.lower()
                        for kw in ["item", "container", "result", "sitelink"]
                    ):
                        container = current
                        break
                current = current.parent

            # 如果没找到特定容器，使用最近的父级div
            if not container:
                current = link_tag.parent
                while current and current != result:
                    if current.name == "div":
                        container = current
                        break
                    current = current.parent

            # 查找内容
            content = ""
            if container:
                content_selectors = [
                    "div[class*=text], div[class*=abs], div[class*=desc], div[class*=content]",
                    "p[class*=text], p[class*=desc], p[class*=content]",
                    "span[class*=text], span[class*=desc], span[class*=content], span[class*=clamp]",
                ]

                content_candidates = []
                for selector in content_selectors:
                    content_candidates.extend(container.select(selector))

                if content_candidates:
                    content = content_candidates[0].get_text(strip=True)

            # 查找来源
            source = ""
            if container:
                source_selectors = [
                    "span[class*=small], span[class*=showurl], span[class*=source-text], span[class*=site-name]",
                    "div[class*=source-text], div[class*=showurl], div[class*=small]",
                ]

                for selector in source_selectors:
                    source_tags = container.select(selector)
                    if source_tags:
                        source = source_tags[0].get_text(strip=True)
                        break

            # 提取时间
            time_str = self.extract_time(container) if container else ""

            if title or href:
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

    def _initial_deduplicate_results(
        self, data: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """初步去重：针对原始搜索结果，合并重复的主链接及其相关链接"""
        main_map = {}
        for entry in data:
            key = entry.get("url", "")
            if not key:
                continue
            if key in main_map:
                existing = main_map[key]
                # 对主链接，补全缺失字段及合并 title, content 到 more
                if not existing["title"] and entry["title"]:
                    existing["title"] = entry["title"]
                if not existing["content"] and entry["content"]:
                    existing["content"] = entry["content"]
                if not existing["source"] and entry["source"]:
                    existing["source"] = entry["source"]
                if not existing["time"] and entry["time"]:
                    existing["time"] = entry["time"]
                if entry["title"] and entry["content"]:
                    existing["more"].append({entry["title"]: entry["content"]})
                existing["related_links"].extend(entry["related_links"])
            else:
                main_map[key] = entry
        results = list(main_map.values())
        # 针对每个主链接的相关链接做去重：
        for entry in results:
            rl_map = {}
            new_more = entry["more"][:]  # 保留已有的合并信息
            for rl in entry["related_links"]:
                rl_key = rl.get("url", "")
                # 如果相关链接与主链接相同，则合并到主链接上
                if rl_key == entry.get("url", ""):
                    if not entry["title"] and rl["title"]:
                        entry["title"] = rl["title"]
                    if not entry["content"] and rl["content"]:
                        entry["content"] = rl["content"]
                    if rl["title"] and rl["content"]:
                        new_more.append({rl["title"]: rl["content"]})
                    continue
                if rl_key in rl_map:
                    existing_rl = rl_map[rl_key]
                    if not existing_rl["title"] and rl["title"]:
                        existing_rl["title"] = rl["title"]
                    if not existing_rl["content"] and rl["content"]:
                        existing_rl["content"] = rl["content"]
                    if rl["title"] and rl["content"]:
                        existing_rl["more"].append({rl["title"]: rl["content"]})
                else:
                    rl_map[rl_key] = rl
            entry["related_links"] = list(rl_map.values())
            entry["more"] = new_more
        return results

    def _final_deduplicate_results(
        self, data: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """最终去重：基于真实链接对主链接及其相关链接再次去重"""
        main_map = {}
        for entry in data:
            key = entry.get("url", "")
            if not key:
                continue
            if key in main_map:
                existing = main_map[key]
                if not existing["title"] and entry["title"]:
                    existing["title"] = entry["title"]
                if not existing["content"] and entry["content"]:
                    existing["content"] = entry["content"]
                if not existing["source"] and entry["source"]:
                    existing["source"] = entry["source"]
                if not existing["time"] and entry["time"]:
                    existing["time"] = entry["time"]
                if entry["title"] and entry["content"]:
                    existing["more"].append({entry["title"]: entry["content"]})
                existing["related_links"].extend(entry["related_links"])
            else:
                main_map[key] = entry
        results = list(main_map.values())
        for entry in results:
            rl_map = {}
            new_more = entry["more"][:]
            for rl in entry["related_links"]:
                rl_key = rl.get("url", "")
                if rl_key == entry.get("url", ""):
                    if not entry["title"] and rl["title"]:
                        entry["title"] = rl["title"]
                    if not entry["content"] and rl["content"]:
                        entry["content"] = rl["content"]
                    if rl["title"] and rl["content"]:
                        new_more.append({rl["title"]: rl["content"]})
                    continue
                if rl_key in rl_map:
                    existing_rl = rl_map[rl_key]
                    if not existing_rl["title"] and rl["title"]:
                        existing_rl["title"] = rl["title"]
                    if not existing_rl["content"] and rl["content"]:
                        existing_rl["content"] = rl["content"]
                    if rl["title"] and rl["content"]:
                        existing_rl["more"].append({rl["title"]: rl["content"]})
                else:
                    rl_map[rl_key] = rl
            entry["related_links"] = list(rl_map.values())
            entry["more"] = new_more
        return results

    def parse_results(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """解析百度搜索结果"""
        if self.logger:
            self.logger.debug("开始解析搜索结果页面")

        results = soup.select("div[class*='c-container'][class*='result']")
        if not results:
            if self.logger:
                self.logger.error("未找到任何搜索结果")
            return []
        # 处理搜索结果
        if self.logger:
            self.logger.info(f"找到 {len(results)} 个搜索结果")

        data = []
        for result in results:
            # if "!important" in result.get("style", ""):
            #     continue (初步过滤掉广告, 测试用, 暂不启用)

            title, url = self.extract_main_title_and_link(result)
            content = self.extract_main_content(result)
            source = self.extract_source(result)
            time = self.extract_time(result)
            related_links = self.extract_related_links(result)

            if title or url:
                data.append(
                    {
                        "title": title,
                        "url": url,
                        "content": content,
                        "source": source,
                        "time": time,
                        "more": [],
                        "related_links": related_links,
                    }
                )

        return self._initial_deduplicate_results(data)

    async def _process_real_urls(
        self, session: aiohttp.ClientSession, data: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """处理百度跳转链接，获取真实URL"""
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
        batch_size = min(20, max(5, self.max_semaphore))
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
            batch_size,
        )

        for (i, pos), url_idx in link_map.items():
            if url_idx < len(real_urls):
                if pos == "main":
                    data[i]["url"] = real_urls[url_idx]
                else:
                    data[i]["related_links"][pos]["url"] = real_urls[url_idx]

        return self._final_deduplicate_results(data)

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
                        self.logger.error(f"第 {page+1} 页获取失败，跳过")
                    continue

                soup = BeautifulSoup(html_content, "html.parser")
                content_left = soup.find("div", id="content_left")
                if not content_left:
                    if self.logger:
                        self.logger.error(
                            f"第 {page+1} 页未找到 'content_left' div，跳过"
                        )
                    continue

                page_results = self.parse_results(content_left)
                processed_results = await self._process_real_urls(session, page_results)
                all_results.extend(processed_results)

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
        return all_results
