import aiohttp
import asyncio
from bs4 import BeautifulSoup
from fetch_utils import fetch_real_url
import random


async def scrape_baidu(
    query: str,
    headers: dict,
    cookies: dict,
    proxies: list[str] = None,
    use_proxy_for_search: bool = False,
    print_to_console: bool = True,
    semaphore_limit: int = 25,
    min_delay_between_requests: float = 0.1,
    max_delay_between_requests: float = 0.3,
    fetch_real_url_timeout: int = 3,
    fetch_real_url_retries: int = 0,
    fetch_real_url_min_retries_sleep: float = 0.1,
    fetch_real_url_max_retries_sleep: float = 0.3,
    fetch_real_url_max_redirects: int = 5,
    no_a_title_tag_strip_n: int = 50,
):
    url = "https://www.baidu.com/s"
    params = {"wd": query}
    semaphore = asyncio.Semaphore(semaphore_limit)

    async with aiohttp.ClientSession(cookies=cookies) as session:
        await asyncio.sleep(
            random.uniform(min_delay_between_requests, max_delay_between_requests)
        )
        search_proxy = (
            random.choice(proxies) if proxies and use_proxy_for_search else None
        )
        async with session.get(
            url, headers=headers, params=params, proxy=search_proxy
        ) as response:
            if response.status == 200:
                text = await response.text()
                soup = BeautifulSoup(text, "lxml")
                results = soup.select(
                    "div[class*='result'], div[class*='c-container'], article[class*='c-container']"
                )
                data = []
                baidu_links = []
                seen_entries = set()

                for result in results:
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

                    entry_key = (title, main_link)
                    if entry_key in seen_entries:
                        continue
                    seen_entries.add(entry_key)

                    main_link_content = ""
                    if "result-op" in result.get("class", []):
                        desc = result.find(class_=lambda x: x and "description" in x)
                        main_link_content = desc.get_text(strip=True) if desc else ""
                    elif "result" in result.get("class", []):
                        content = result.find(
                            class_=lambda x: x and "content-right" in x
                        ) or result.find("span", class_="c-line-clamp2")
                        main_link_content = (
                            content.get_text(strip=True) if content else ""
                        )
                    else:
                        content = result.find("span", class_="c-line-clamp2")
                        main_link_content = (
                            content.get_text(strip=True) if content else ""
                        )
                    baidu_links.append(main_link)

                    # 提取主链接的时间信息
                    main_link_time = ""
                    # 查找普通结果的时间信息 (如 "2021年3月23日" 或 "4天前")
                    time_tag = result.find(
                        "span", class_=lambda x: x and "time" in x.lower()
                    )
                    if time_tag:
                        main_link_time = time_tag.get_text(strip=True)
                    else:
                        time_tag = result.find(
                            "span", class_=lambda x: x == "c-color-gray2"
                        )
                        if time_tag:
                            main_link_time = time_tag.get_text(strip=True)

                    related_links = [
                        {
                            "text": (
                                a.get_text(strip=True) if a.get_text(strip=True) else ""
                            ),
                            "href": a.get("href", ""),
                            "content": "",
                            "time": "",  # 初始化时间字段为空字符串
                        }
                        for a in result.find_all("a")
                        if a.get("href")
                    ]

                    data.append(
                        {
                            "title": title,
                            "main_link": None,
                            "main_link_content": main_link_content,
                            "main_link_time": main_link_time,  # 添加主链接时间字段
                            "related_links": related_links,
                        }
                    )

                tasks = [
                    fetch_real_url(
                        session,
                        link,
                        headers,
                        cookies,
                        proxies,
                        semaphore,
                        timeout=fetch_real_url_timeout,
                        retries=fetch_real_url_retries,
                        min_retries_sleep=fetch_real_url_min_retries_sleep,
                        max_retries_sleep=fetch_real_url_max_retries_sleep,
                        max_redirects=fetch_real_url_max_redirects,
                    )
                    for link in baidu_links
                ]
                real_links = await asyncio.gather(*tasks, return_exceptions=True)

                for entry in data:
                    entry["main_link"] = real_links[data.index(entry)]

                    related_tasks = [
                        fetch_real_url(
                            session,
                            link["href"],
                            headers,
                            cookies,
                            proxies,
                            semaphore,
                            timeout=fetch_real_url_timeout,
                            retries=fetch_real_url_retries,
                            min_retries_sleep=fetch_real_url_min_retries_sleep,
                            max_retries_sleep=fetch_real_url_max_retries_sleep,
                            max_redirects=fetch_real_url_max_redirects,
                        )
                        for link in entry["related_links"]
                    ]
                    real_related_links = (
                        await asyncio.gather(*related_tasks, return_exceptions=True)
                        if related_tasks
                        else []
                    )

                    sitelinks = soup.select(".sitelink_summary")
                    buttons = soup.select(
                        '[class*="button"] a'
                    )  # 匹配类名中含 "button" 的元素
                    for i, link in enumerate(entry["related_links"]):
                        content = ""
                        link_time = ""  # 初始化该链接的时间变量

                        # 查找sitelink中的内容和时间
                        for sl in sitelinks:
                            if sl.find("a") and sl.find("a")["href"] == link["href"]:
                                p = sl.find("p")
                                content = p.get_text(strip=True) if p else ""
                                # 查找时间信息
                                time_tag = sl.find(
                                    "span",
                                    class_=lambda x: x and "time" in x.lower(),
                                ) or sl.find(
                                    "span",
                                    class_=lambda x: x == "c-color-gray2",
                                )
                                # )
                                if time_tag:
                                    link_time = time_tag.get_text(strip=True)
                                break

                        # 查找按钮中的内容
                        for btn in buttons:
                            if btn["href"] == link["href"]:
                                content = btn.get_text(strip=True)
                                # 查找按钮附近的时间标签
                                time_tag = (
                                    btn.parent.find(
                                        "span",
                                        class_=lambda x: x and "time" in x.lower(),
                                    )
                                    or btn.parent.find(
                                        "span",
                                        class_=lambda x: x == "c-color-gray2",
                                    )
                                    if btn.parent
                                    else None
                                )
                                if time_tag:
                                    link_time = time_tag.get_text(strip=True)
                                break

                        # 如果还没找到时间，在整个结果区域查找
                        if not link_time:
                            for parent in soup.select(".result, .c-container"):
                                link_a = parent.find("a", href=link["href"])
                                if link_a:
                                    time_tag = parent.find(
                                        "span",
                                        class_=lambda x: x and "time" in x.lower(),
                                    ) or parent.find(
                                        "span",
                                        class_=lambda x: x == "c-color-gray2",
                                    )
                                    # )
                                    if time_tag:
                                        link_time = time_tag.get_text(strip=True)
                                    break

                        link["content"] = content
                        link["time"] = link_time  # 设置时间信息
                        link["href"] = real_related_links[i]

                    if print_to_console:
                        print(f"标题: {entry['title']}")
                        print(f"主链接: {entry['main_link']}")
                        print(f"主链接内容: {entry['main_link_content']}")
                        print(f"主链接时间: {entry['main_link_time']}")
                        for rel_link in entry["related_links"]:
                            print(
                                f"相关链接: {rel_link['text']} -> {rel_link['href']} (内容: {rel_link['content']}, 时间: {rel_link['time']})"
                            )
                        print("-" * 50)

                return data
            else:
                print(f"请求失败，状态码：{response.status}")
                return []
