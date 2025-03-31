import aiohttp
from aiohttp import ClientSession, ClientTimeout
import asyncio
from urllib.parse import urljoin
import random


async def fetch_real_url(
    session: ClientSession,
    org_link: str,
    headers: dict,
    cookies: dict,
    proxy_list: list[str],
    semaphore: asyncio.Semaphore,
    timeout: int = 3,
    retries: int = 0,
    min_retries_sleep: int = 0.5,
    max_retries_sleep: int = 1,
    max_redirects: int = 5,
) -> str:
    if not org_link or not org_link.startswith(("http://", "https://")):
        org_link = urljoin("https://www.baidu.com/", org_link if org_link else "")

    async with semaphore:
        current_url = org_link
        redirect_count = 0

        while redirect_count < max_redirects:
            for attempt in range(retries + 1):
                proxy = random.choice(proxy_list) if proxy_list else None
                try:
                    async with session.get(
                        current_url,
                        headers=headers,
                        cookies=cookies,
                        allow_redirects=True,
                        timeout=ClientTimeout(total=timeout),
                        proxy=proxy,
                    ) as response:
                        if response.status in (301, 302, 303, 307, 308):
                            current_url = response.headers.get("Location")
                            if not current_url:
                                print(f"未找到重定向地址: {org_link}")
                                return org_link
                            current_url = urljoin(str(response.url), current_url)
                            redirect_count += 1
                            print(f"检测到重定向: {current_url}")
                            break
                        else:
                            return response.url.human_repr()
                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    print(
                        f"链接解析失败 (尝试 {attempt+1}/{retries+1}): {current_url}, "
                        f"代理: {proxy}, 错误: {str(e)}"
                    )
                    if attempt < retries - 1:
                        await asyncio.sleep(
                            random.uniform(min_retries_sleep, max_retries_sleep)
                        )
            else:
                print(f"重试次数耗尽，仍未解析: {current_url}")
                return current_url

        print(f"超过最大重定向次数: {org_link}")
        return current_url
