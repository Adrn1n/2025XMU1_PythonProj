import aiohttp
import asyncio
from bs4 import BeautifulSoup
from fetch_utils import fetch_real_url
import random


async def scrape_baidu(
    query: str,
    headers: dict,
    cookies: dict,
    print_to_console: bool = True,
    semaphore_limit: int = 10,
    fetch_realURL_timeout: int = 3,
    no_a_titleTag_stripN: int = 50,
    delay_between_requests: int = 5,
):
    url = "https://www.baidu.com/s"
    params = {"wd": query}
    semaphore = asyncio.Semaphore(semaphore_limit)

    async with aiohttp.ClientSession(cookies=cookies) as session:
        await asyncio.sleep(random.uniform(2, delay_between_requests))
        async with session.get(
            url, headers=headers, params=params, proxy=None
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
                        else result.get_text(strip=True)[:no_a_titleTag_stripN]
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

                    related_links = [
                        {"text": a.get_text(strip=True), "href": a["href"]}
                        for a in result.find_all("a", href=True)
                        if a.get_text(strip=True)
                    ]
                    data.append(
                        {
                            "title": title,
                            "main_link": None,
                            "related_links": related_links,
                        }
                    )
                    baidu_links.append(main_link)

                tasks = [
                    fetch_real_url(
                        session,
                        link,
                        headers,
                        cookies,
                        semaphore,
                        fetch_realURL_timeout,
                    )
                    for link in baidu_links
                ]
                real_links = await asyncio.gather(*tasks, return_exceptions=True)

                for entry in data:
                    related_tasks = [
                        fetch_real_url(
                            session, link["href"], headers, cookies, semaphore
                        )
                        for link in entry["related_links"]
                    ]
                    real_related_links = (
                        await asyncio.gather(*related_tasks, return_exceptions=True)
                        if related_tasks
                        else []
                    )
                    for link, real_url in zip(
                        entry["related_links"], real_related_links
                    ):
                        link["href"] = real_url
                    entry["main_link"] = real_links[data.index(entry)]

                    if print_to_console:
                        print(f"标题: {entry['title']}")
                        print(f"主链接: {entry['main_link']}")
                        for rel_link in entry["related_links"]:
                            print(f"相关链接: {rel_link['text']} -> {rel_link['href']}")
                        print("-" * 50)

                return data
            else:
                print(f"请求失败，状态码：{response.status}")
                return []
