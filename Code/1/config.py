from pathlib import Path
import re
import random
from typing import List, Dict, Callable, Optional, TypeVar
import logging

T = TypeVar("T")

# 配置常量
HEADERS_FILE = Path("config/headers.txt")
PROXY_FILE = Path("config/proxy.txt")
SEARCH_CACHE_FILE = Path("cache/baidu_search_res.json")
LOG_FILE = Path("logs/scraper.log")

# 添加缺失的配置变量
CACHE_DIR = Path("cache")
LOG_DIR = Path("logs")

# 默认爬虫配置
DEFAULT_CONFIG = {
    "semaphore_limit": 25,
    "min_delay_between_requests": 0.1,
    "max_delay_between_requests": 0.3,
    "fetch_timeout": 3,
    "fetch_retries": 1,
    "min_retries_sleep": 0.1,
    "max_retries_sleep": 0.3,
    "max_redirects": 5,
    "cache_size": 1000,
}

# 初始化日志器，但不立即配置，等待 main.py 中用户选择
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)  # 默认级别，稍后可调整


def load_file_lines(
    file_path: Path, process_line: Callable[[str], str] = str.strip
) -> List[str]:
    """通用文件加载函数"""
    try:
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        with file_path.open("r", encoding="utf-8") as f:
            lines = [process_line(line) for line in f if line.strip()]
        if not lines:
            raise ValueError(f"File is empty: {file_path}")
        return lines
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"加载文件 {file_path} 失败: {e}")
        return []


def parse_cookies(line: str) -> Dict[str, str]:
    """解析cookie行"""
    cookies_dict = {}
    for cookie in line.split("; "):
        if "=" in cookie:
            k, v = cookie.split("=", 1)
            cookies_dict[re.sub(r"[\[\]]", "", k)] = v
    return cookies_dict


def load_http_headers(file_path: Path) -> List[Dict[str, str]]:
    """加载完整的HTTP请求头信息"""
    try:
        if not file_path.exists():
            raise FileNotFoundError(f"文件未找到: {file_path}")
        with file_path.open("r", encoding="utf-8") as f:
            content = f.read()
        header_blocks = [
            block.strip() for block in content.split("\n\n") if block.strip()
        ]
        if not header_blocks:
            raise ValueError(f"文件为空或格式不正确: {file_path}")

        headers_list = []
        for block in header_blocks:
            headers_dict = {}
            lines = block.splitlines()
            for line in lines[1:]:
                if ": " in line:
                    key, value = line.split(": ", 1)
                    if key.lower() == "cookie":
                        cookies_dict = parse_cookies(value)
                        headers_dict["cookies"] = cookies_dict
                    headers_dict[key] = value
            if headers_dict:
                headers_list.append(headers_dict)
        if not headers_list:
            raise ValueError(f"未能解析任何有效的请求头: {file_path}")
        return headers_list
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"加载HTTP请求头文件 {file_path} 失败: {e}")
        return []


def random_choice(items: List[T], default: Optional[T] = None) -> T:
    """从列表中随机选择一项，若列表为空返回默认值"""
    return random.choice(items) if items else default


HEADERS_LIST = load_http_headers(HEADERS_FILE)
if not HEADERS_LIST:
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    COOKIES = {}
else:
    HEADERS_ITEM = random_choice(HEADERS_LIST)
    COOKIES = HEADERS_ITEM.pop("cookies", {}) if "cookies" in HEADERS_ITEM else {}
    HEADERS = HEADERS_ITEM

PROXY_LIST = load_file_lines(PROXY_FILE)
PROXY = random_choice(PROXY_LIST, None)
