import asyncio
from scraper import scrape_baidu
from config import HEADERS, COOKIES, PROXY_LIST, SEARCH_CACHE_FILE
from file_utils import write_to_file


async def main():
    query = input("请输入搜索内容: ")
    print_choice = input("是否打印到终端？(y/[n]): ").lower() == "y"
    write_choice = input("是否写入文件？(y/[n]): ").lower() == "y"

    data = await scrape_baidu(
        query=query,
        headers=HEADERS,
        cookies=COOKIES,
        proxies=PROXY_LIST,
        print_to_console=print_choice,
        semaphore_limit=15,
        min_delay_between_requests=0.1,
        max_delay_between_requests=0.3,
        fetch_real_url_timeout=3,
        fetch_real_url_retries=0,
        fetch_real_url_min_retries_sleep=0.1,
        fetch_real_url_max_retries_sleep=0.3,
        fetch_real_url_max_redirects=5,
        no_a_title_tag_strip_n=50,
    )

    if write_choice and data:
        await write_to_file(data, SEARCH_CACHE_FILE)


if __name__ == "__main__":
    asyncio.run(main())
