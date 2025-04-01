from pathlib import Path
import re
import random
from typing import List, Dict, Callable, Optional, TypeVar
import logging

T = TypeVar("T")

# 配置常量
UA_FILE = Path("config/ua.txt")
COOKIES_FILE = Path("config/cookies.txt")
PROXY_FILE = Path("config/proxy.txt")
SEARCH_CACHE_FILE = Path("cache/baidu_search_res.json")
LOG_FILE = Path("logs/scraper.log")

# 添加缺失的配置变量
CACHE_DIR = Path("cache")
LOG_DIR = Path("logs")

# 默认爬虫配置
DEFAULT_CONFIG = {
    "semaphore_limit": 15,  # 并发限制
    "min_delay_between_requests": 0.1,  # 请求间最小延迟
    "max_delay_between_requests": 0.3,  # 请求间最大延迟
    "fetch_timeout": 5,  # 请求超时时间(秒)
    "fetch_retries": 2,  # 失败重试次数
    "min_retries_sleep": 0.1,  # 重试最小等待时间
    "max_retries_sleep": 0.3,  # 重试最大等待时间
    "max_redirects": 5,  # 最大重定向次数
    "cache_size": 1000,  # 缓存大小
}

# 确保日志目录存在
try:
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
except Exception as e:
    # 如果无法创建目录，使用控制台日志作为后备
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.error(f"无法创建日志目录 {LOG_FILE.parent}: {e}")
else:
    # 设置日志
    logging.basicConfig(
        filename=LOG_FILE,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        encoding="utf-8",
    )
    logger = logging.getLogger(__name__)


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


def random_choice(items: List[T], default: Optional[T] = None) -> T:
    """从列表中随机选择一项，若列表为空返回默认值"""
    return random.choice(items) if items else default


# 加载配置
UA_LIST = load_file_lines(UA_FILE)
HEADERS = {"User-Agent": random_choice(UA_LIST, "")}

COOKIES_LIST = load_file_lines(COOKIES_FILE, parse_cookies)
COOKIES = random_choice(COOKIES_LIST, {})

PROXY_LIST = load_file_lines(PROXY_FILE)
PROXY = random_choice(PROXY_LIST, None)
