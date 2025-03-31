import re
import random
from pathlib import Path

# Paths to UA and cookies files
UA_FILE = Path("config/ua.txt")
COOKIES_FILE = Path("config/cookies.txt")


# Read User-Agent strings from file
def load_ua_list():
    if not UA_FILE.exists():
        raise FileNotFoundError(f"UA file not found: {UA_FILE}")
    with UA_FILE.open("r", encoding="utf-8") as f:
        ua_list = [line.strip() for line in f if line.strip()]
    if not ua_list:
        raise ValueError(f"UA file is empty: {UA_FILE}")
    return ua_list


# Read cookies from file
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


# Load User-Agent strings with error handling
try:
    HEADERS_LIST = load_ua_list()
    HEADERS = {"User-Agent": random.choice(HEADERS_LIST)}
except (FileNotFoundError, ValueError) as e:
    print(f"Error: {e}")
    HEADERS = {"User-Agent": ""}

# Load cookies with error handling
try:
    COOKIES_LIST = load_cookies_list()
    COOKIES = random.choice(COOKIES_LIST)
except (FileNotFoundError, ValueError) as e:
    print(f"Error: {e}")
    COOKIES = {}

# Cache file path
SEARCH_CACHE_FILE = "cache/baidu_search_res.json"
