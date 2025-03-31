import asyncio
from scraper import scrape_baidu
from file_utils import write_to_file
from config import HEADERS, COOKIES, SEARCH_CACHE_FILE

query = input("请输入搜索内容: ")
print_choice = input("是否打印到终端？(y/[n]): ").lower() == "y"
write_choice = input("是否写入文件？(y/[n]): ").lower() == "y"

data = asyncio.run(
    scrape_baidu(
        query=query,
        headers=HEADERS,
        cookies=COOKIES,
        print_to_console=print_choice,
    )
)

if write_choice and data:
    asyncio.run(write_to_file(data, SEARCH_CACHE_FILE))
