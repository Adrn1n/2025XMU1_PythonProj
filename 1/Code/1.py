import requests
from bs4 import BeautifulSoup
import json


def scrape_baidu(query, print_to_console=True):
    url = "https://www.baidu.com/s"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
    }
    params = {"wd": query}

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, "html.parser")
        results = soup.select("div.c-container")
        data = []

        for result in results:
            title_tag = result.find("h3", class_="t")
            title = (
                title_tag.find("a").get_text(strip=True)
                if title_tag and title_tag.find("a")
                else "无标题"
            )
            baidu_link = (
                title_tag.find("a")["href"]
                if title_tag
                and title_tag.find("a")
                and "href" in title_tag.find("a").attrs
                else "无链接"
            )
            # Follow the Baidu redirect to get the real URL
            if baidu_link != "无链接":
                real_response = requests.get(
                    baidu_link, headers=headers, allow_redirects=True
                )
                real_link = real_response.url
            else:
                real_link = "无链接"

            entry = {"title": title, "link": real_link}
            data.append(entry)
            if print_to_console:
                print(f"标题: {title}, 链接: {real_link}")

        with open("baidu_results.json", "w", encoding="utf-8") as file:
            json.dump(data, file, ensure_ascii=False, indent=2)
        print("数据已写入 baidu_results.json")
    else:
        print(f"请求失败，状态码：{response.status_code}")


# 用户输入
query = input("请输入搜索内容: ")
print_choice = input("是否打印到终端？(y/n): ").lower() == "y"
scrape_baidu(query, print_to_console=print_choice)
