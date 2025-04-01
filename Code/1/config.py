from pathlib import Path
import re
import random

UA_FILE = Path("config/ua.txt")
COOKIES_FILE = Path("config/cookies.txt")
PROXY_FILE = Path("config/proxy.txt")
SEARCH_CACHE_FILE = "cache/baidu_search_res.json"


def load_ua_list():
    if not UA_FILE.exists():
        raise FileNotFoundError(f"UA file not found: {UA_FILE}")
    with UA_FILE.open("r", encoding="utf-8") as f:
        ua_list = [line.strip() for line in f if line.strip()]
    if not ua_list:
        raise ValueError(f"UA file is empty: {UA_FILE}")
    return ua_list


def load_cookies_list():
    if not COOKIES_FILE.exists():
        raise FileNotFoundError(f"Cookies file not found: {COOKIES_FILE}")
    with COOKIES_FILE.open("r", encoding="utf-8") as f:
        cookies_list = []
        for line in f:
            line = line.strip()
            if line:
                cookies_dict = {
                    re.sub(r"[\[\]]", "", k): v
                    for cookie in line.split("; ")
                    for k, v in [cookie.split("=", 1)]
                }
                cookies_list.append(cookies_dict)
        if not cookies_list:
            raise ValueError(f"Cookies file is empty: {COOKIES_FILE}")
        return cookies_list


def load_proxy_list():
    if not PROXY_FILE.exists():
        raise FileNotFoundError(f"Proxy file not found: {PROXY_FILE}")
    with PROXY_FILE.open("r", encoding="utf-8") as f:
        proxy_list = [line.strip() for line in f if line.strip()]
    if not proxy_list:
        raise ValueError(f"Proxy file is empty: {PROXY_FILE}")
    return proxy_list


try:
    HEADERS_LIST = load_ua_list()
    HEADERS = {"User-Agent": random.choice(HEADERS_LIST)}
except (FileNotFoundError, ValueError) as e:
    print(f"Error: {e}")
    HEADERS = {"User-Agent": ""}

try:
    COOKIES_LIST = load_cookies_list()
    COOKIES = random.choice(COOKIES_LIST)
except (FileNotFoundError, ValueError) as e:
    print(f"Error: {e}")
    COOKIES = {}

try:
    PROXY_LIST = load_proxy_list()
    PROXY = random.choice(PROXY_LIST)
except (FileNotFoundError, ValueError) as e:
    print(f"Error: {e}")
    PROXY_LIST = []
    PROXY = None
