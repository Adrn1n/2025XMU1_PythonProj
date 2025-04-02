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
            single_cards = soup.select(
                "div[class*='card-wrapper'], div[class*='single-card']"
            )
            results.extend(single_cards)

            if self.logger:
                self.logger.info(f"找到 {len(results)} 个搜索结果")

            data = []
            baidu_links = []
            seen_entries = set()
            card_related_links = {}

            for i, result in enumerate(results):
                if any(
                    card_class in " ".join(result.get("class", []))
                    for card_class in ["single-card", "card-wrapper"]
                ):
                    card_id = result.get("id", f"card_{i}")
                    card_title_tag = result.select_one(
                        "a[class*='card-title'], h3 a, a[class*='title']"
                    )
                    card_title = (
                        card_title_tag.get_text(strip=True) if card_title_tag else ""
                    )
                    card_main_link = (
                        card_title_tag["href"]
                        if card_title_tag and "href" in card_title_tag.attrs
                        else ""
                    )

                    if (
                        card_title
                        and card_main_link
                        and (card_title, card_main_link) not in seen_entries
                    ):
                        seen_entries.add((card_title, card_main_link))
                        baidu_links.append(card_main_link)
                        card_related_links[card_id] = []

                        data.append(
                            {
                                "title": card_title,
                                "main_link": None,
                                "main_link_content": self._extract_content(result),
                                "main_link_time": self._extract_time(result),
                                "main_link_moreInfo": {},
                                "related_links": [],
                            }
                        )
                        if self.logger:
                            self.logger.debug(
                                f"解析卡片: {card_title} - {card_main_link}"
                            )

                    sub_items = result.select(
                        "div[class*='group-content'], div[class*='render-item'], div[class*='c-row']"
                    )
                    for sub_item in sub_items:
                        if sub_item == result:
                            continue

                        title_tag = sub_item.select_one(
                            "a[class*='title'], a[class*='sub-title'], h3 a"
                        )
                        if not title_tag:
                            continue

                        title = title_tag.get_text(strip=True)
                        link = title_tag["href"] if "href" in title_tag.attrs else ""

                        if not title or not link or (title, link) in seen_entries:
                            continue

                        seen_entries.add((title, link))
                        baidu_links.append(link)

                        content = ""
                        content_tag = sub_item.select_one(
                            "div[class*='abs'], div[class*='content'], p"
                        )
                        if content_tag:
                            content = content_tag.get_text(strip=True)

                        time_text = ""
                        time_tag = sub_item.select_one(
                            "span[class*='time'], span.c-color-gray2"
                        )
                        if time_tag:
                            time_text = time_tag.get_text(strip=True)

                        if not card_title and len(data) == 0:
                            data.append(
                                {
                                    "title": title,
                                    "main_link": None,
                                    "main_link_content": content,
                                    "main_link_time": time_text,
                                    "main_link_moreInfo": {},
                                    "related_links": [],
                                }
                            )
                        else:
                            if card_id in card_related_links:
                                card_related_links[card_id].append(
                                    {
                                        "text": title,
                                        "href": link,
                                        "content": content,
                                        "time": time_text,
                                    }
                                )
                            elif data:
                                if "related_links" not in data[-1]:
                                    data[-1]["related_links"] = []
                                data[-1]["related_links"].append(
                                    {
                                        "text": title,
                                        "href": link,
                                        "content": content,
                                        "time": time_text,
                                    }
                                )
                else:
                    title_tag = result.find(
                        "h3",
                        class_=lambda x: (
                            x and any(c in x for c in ["t", "c-title"]) if x else True
                        ),
                    ) or result.find(
                        "a", class_=lambda x: x and "title" in x.lower() if x else False
                    )

                    title = ""
                    main_link = ""

                    if title_tag:
                        a_tag = (
                            title_tag.find("a") if title_tag.name != "a" else title_tag
                        )
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
                        continue

                    seen_entries.add(entry_key)
                    main_link_content = self._extract_content(result)
                    main_link_time = self._extract_time(result)
                    baidu_links.append(main_link)

                    if self.logger:
                        self.logger.debug(f"解析结果 #{i+1}: {title} - {main_link}")

                    main_link_moreInfo, related_links_data = (
                        self._extract_related_links(result, soup, main_link)
                    )

                    data.append(
                        {
                            "title": title,
                            "main_link": None,
                            "main_link_content": main_link_content,
                            "main_link_time": main_link_time,
                            "main_link_moreInfo": main_link_moreInfo,
                            "related_links": related_links_data,
                        }
                    )

            for i, entry in enumerate(data):
                for card_id, related_links in card_related_links.items():
                    if i == list(card_related_links.keys()).index(card_id):
                        entry["related_links"].extend(related_links)

            return data, baidu_links
        except Exception as e:
            if self.logger:
                self.logger.error(f"解析结果时出错: {str(e)}")
            return [], []

    def _extract_content(self, result) -> str:
        """Extract search result content summary"""
        if result.select("div[class*='render-item']"):
            return ""

        content_tag = result.select_one("span[class*='content-right']")
        if content_tag:
            return content_tag.get_text(strip=True)

        content_selectors = [
            lambda r: r.select_one("div.group-sub-abs_N-I8P"),
            lambda r: r.select_one("div[class*='description'], div[class*='desc']"),
            lambda r: r.select_one("div[class*='content'], div[class*='cont']"),
            lambda r: r.select_one("span[class*='line-clamp'], span[class*='clamp']"),
            lambda r: r.select_one("div[class*='abs'], p[class*='abs']"),
        ]

        for selector in content_selectors:
            content = selector(result)
            if content:
                return content.get_text(strip=True)

        return ""

    def _extract_time(self, result) -> str:
        """Extract search result time information"""
        if result.select("div[class*='render-item']"):
            return ""

        time_selectors = [
            lambda r: r.select_one("span[class*='time']"),
            lambda r: r.select_one("span.c-color-gray2"),
            lambda r: r.select_one("time"),
        ]

        for selector in time_selectors:
            time_tag = selector(result)
            if time_tag:
                time_text = time_tag.get_text(strip=True)
                if any(char.isdigit() for char in time_text):
                    return time_text

        return ""

    def _extract_related_links(
        self, result, soup, main_link
    ) -> Tuple[Dict[str, str], List[Dict[str, Any]]]:
        """Extract related links and additional info for the main link"""
        main_link_moreInfo = {}
        related_links = []

        try:
            sitelink_container = result.select_one(
                "div[class*='sitelink'], div[class*='sitelink-container']"
            )
            if sitelink_container:
                sitelink_summaries = sitelink_container.select(
                    "div[class*='sitelink-summary'], div[class*='summary']"
                )
                for summary in sitelink_summaries:
                    link_tag = summary.select_one("a")
                    if not link_tag or not link_tag.get("href"):
                        continue
                    href = link_tag.get("href")
                    text = link_tag.get_text(strip=True)
                    content = ""
                    content_tag = summary.select_one(
                        "p.c-color-text, p[class*='line-clamp']"
                    )
                    if content_tag:
                        content = content_tag.get_text(strip=True)
                    if text and href and "翻译此页" not in text:
                        related_links.append(
                            {"text": text, "href": href, "content": content, "time": ""}
                        )

            news_items = result.select(
                "[class*='render-item'], [class*='group-content'], [class*='blog-item']"
            )
            for item in news_items:
                title_link = item.select_one(
                    "a[class*='title'], a[class*='sub-title'], a.c-font-medium"
                )
                if not title_link:
                    continue
                href = title_link.get("href", "")
                if not href:
                    continue
                text = title_link.get_text(strip=True)
                content = self._extract_content(item)
                time = self._extract_time(item)

                link_exists = False
                for link in related_links:
                    if link["href"] == href:
                        link_exists = True
                        if content and (
                            not link["content"] or len(content) > len(link["content"])
                        ):
                            link["content"] = content
                        if time and not link["time"]:
                            link["time"] = time
                        break

                if not link_exists and text:
                    related_links.append(
                        {"text": text, "href": href, "content": content, "time": time}
                    )

            source_groups = soup.select(
                "div[class*='source-wrapper'], div[class*='source-group']"
            )
            for source_group in source_groups:
                link = source_group.find("a")
                if not link or not link.get("href"):
                    continue
                href = link.get("href")
                time = self._extract_time(source_group)
                if time:
                    for link_data in related_links:
                        if link_data["href"] == href and not link_data["time"]:
                            link_data["time"] = time
                            break

            for a in result.find_all("a"):
                href = a.get("href", "")
                if href == main_link:
                    text = a.get_text(strip=True)
                    if not text:
                        continue
                    content = ""
                    containers = soup.select(
                        "div[class*='summary'], div[class*='abs'], div[class*='content'], div[class*='c-color-text']"
                    )
                    for container in containers:
                        container_link = container.find("a", href=href)
                        if container_link:
                            current_content = container.get_text(strip=True)
                            from bs4.element import Comment

                            for comment in container.find_all(
                                string=lambda text: isinstance(text, Comment)
                            ):
                                if "s-text" in comment:
                                    comment_text = comment.strip()
                                    if comment_text.startswith("s-text"):
                                        comment_text = comment_text[6:]
                                    if comment_text.endswith("/s-text"):
                                        comment_text = comment_text[:-7]
                                    if comment_text and len(comment_text.strip()) > len(
                                        current_content
                                    ):
                                        current_content = comment_text.strip()
                            if len(current_content) > len(content):
                                content = current_content
                    if text or content:
                        if text in main_link_moreInfo:
                            if len(content) > len(main_link_moreInfo[text]):
                                main_link_moreInfo[text] = content
                        else:
                            main_link_moreInfo[text] = content

            related_links_final = [
                link
                for link in related_links
                if link["text"]
                and len(link["text"]) > 2
                and not any(
                    term in link["text"].lower()
                    for term in ["查看更多", "更多相关", "举报", "反馈"]
                )
            ]

            return main_link_moreInfo, related_links_final
        except Exception as e:
            if self.logger:
                self.logger.error(f"提取相关链接时出错: {str(e)}")
            return {}, []

    def _find_parent_container(self, element, max_depth=4):
        """查找元素的包含内容的父容器"""
        current = element
        depth = 0
        content_class_keywords = {
            "c-container",
            "result",
            "content",
            "item",
            "card",
            "render-item",
        }

        while current and depth < max_depth:
            parent = current.parent
            if not parent:
                break
            if parent.name in ["div", "article", "section"] and parent.get("class"):
                parent_classes = " ".join(parent.get("class", []))
                if any(keyword in parent_classes for keyword in content_class_keywords):
                    return parent
            if parent.name == "li":
                content_elem = parent.select_one(
                    "p[class*='content'], p[class*='desc'], div[class*='content'], div[class*='desc']"
                )
                if content_elem:
                    return parent
            current = parent
            depth += 1

        return current if current != element else None

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
                    if not normalized_rel_url:
                        continue
                    if normalized_rel_url == url:
                        for k, v in {
                            link.get("text", ""): link.get("content", "")
                        }.items():
                            if k and k in entry.get("main_link_moreInfo", {}):
                                if len(v) > len(entry["main_link_moreInfo"][k]):
                                    entry["main_link_moreInfo"][k] = v
                            elif k:
                                if "main_link_moreInfo" not in entry:
                                    entry["main_link_moreInfo"] = {}
                                entry["main_link_moreInfo"][k] = v
                        if link.get("time") and not entry.get("main_link_time"):
                            entry["main_link_time"] = link["time"]
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
