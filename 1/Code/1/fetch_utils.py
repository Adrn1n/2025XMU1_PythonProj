import aiohttp
from aiohttp import ClientSession, ClientTimeout
import asyncio
from urllib.parse import urljoin
import random


async def fetch_real_url(
    session: ClientSession,
    baidu_link: str,
    headers: dict,
    cookies: dict,
    semaphore: asyncio.Semaphore,
    timeout: int = 3,
    retries: int = 1,
    max_redirects: int = 5,
) -> str:
    if not baidu_link or not baidu_link.startswith(("http://", "https://")):
        baidu_link = urljoin("https://www.baidu.com/", baidu_link if baidu_link else "")

    async with semaphore:
        current_url = baidu_link
        redirect_count = 0

        while redirect_count < max_redirects:
            for attempt in range(retries):
                try:
                    async with session.get(
                        current_url,
                        headers=headers,
                        cookies=cookies,
                        allow_redirects=False,
                        timeout=ClientTimeout(total=timeout),
                        proxy=None,
                    ) as response:
                        if response.status in (301, 302, 303, 307, 308):
                            current_url = response.headers.get("Location")
                            if not current_url:
                                print(f"未找到重定向地址: {baidu_link}")
                                return baidu_link
                            current_url = urljoin(str(response.url), current_url)
                            redirect_count += 1
                            print(f"检测到重定向: {current_url}")
                            break
                        else:
                            final_url = response.url.human_repr()
                            if final_url == baidu_link:
                                print(f"未检测到重定向: {baidu_link}")
                            return final_url
                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    print(
                        f"链接解析失败 (尝试 {attempt+1}/{retries}): {current_url}, 错误: {str(e)}"
                    )
                    if attempt < retries - 1:
                        await asyncio.sleep(random.uniform(2, 5))
            else:
                print(f"重试次数耗尽，仍未解析: {current_url}")
                return current_url

        print(f"超过最大重定向次数: {baidu_link}")
        return current_url
