from typing import List, Dict, TypeVar
import logging
from pathlib import Path
import re
import random

from utils.config_manager import ConfigManager, DEFAULT_CONFIG_TEMPLATES

T = TypeVar("T")

# 创建日志记录器
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# 初始化配置管理器
config_manager = ConfigManager()
config_manager.ensure_default_configs()  # 确保默认配置存在

# 从配置文件加载路径配置
paths_config = config_manager.load_config("paths", DEFAULT_CONFIG_TEMPLATES["paths"])
files_config = config_manager.load_config("files", DEFAULT_CONFIG_TEMPLATES["files"])

# 创建实际路径对象
paths = {key: Path(value) for key, value in paths_config.items()}
files = {}
for key, value in files_config.items():
    base_path = Path(value)
    # 如果是相对路径，则基于相应目录
    if not base_path.is_absolute() and key.endswith("_file"):
        dir_key = f"{key.rsplit('_', 1)[0]}_dir"
        if dir_key in paths:
            files[key] = paths[dir_key] / base_path.name
        else:
            files[key] = base_path
    else:
        files[key] = base_path

# 确保所有目录都存在
for key, path in paths.items():
    if key.endswith("_dir") and not path.exists():
        try:
            path.mkdir(parents=True, exist_ok=True)
            logger.debug(f"创建目录: {path}")
        except Exception as e:
            logger.error(f"创建目录失败: {path}, 错误: {e}")


def parse_cookies(line: str) -> Dict[str, str]:
    """解析cookie行"""
    cookies_dict = {}
    for cookie in line.split("; "):
        if "=" in cookie:
            k, v = cookie.split("=", 1)
            cookies_dict[re.sub(r"[\[\]]", "", k)] = v
    return cookies_dict


def load_http_headers(file_path: Path) -> List[Dict[str, str]]:
    """加载HTTP请求头信息"""
    try:
        if not file_path.exists():
            logger.debug(f"HTTP头文件不存在: {file_path}")
            return []

        with file_path.open("r", encoding="utf-8") as f:
            content = f.read()

        # 按空行分隔头信息块
        header_blocks = [
            block.strip() for block in content.split("\n\n") if block.strip()
        ]
        if not header_blocks:
            return []

        headers_list = []
        for block in header_blocks:
            headers_dict = {}
            lines = block.splitlines()
            for line in lines[1:]:  # 跳过第一行
                if ": " in line:
                    key, value = line.split(": ", 1)
                    if key.lower() == "cookie":
                        cookies_dict = parse_cookies(value)
                        headers_dict["cookies"] = cookies_dict
                    headers_dict[key] = value
            if headers_dict:
                headers_list.append(headers_dict)
        return headers_list
    except Exception as e:
        logger.error(f"加载HTTP头文件失败: {file_path}, 错误: {e}")
        return []


def load_file_lines(file_path: Path) -> List[str]:
    """加载文件行"""
    try:
        if not file_path.exists():
            return []
        with file_path.open("r", encoding="utf-8") as f:
            return [line.strip() for line in f if line.strip()]
    except Exception as e:
        logger.error(f"加载文件失败: {file_path}, 错误: {e}")
        return []


# 加载HTTP头和代理
headers_list = load_http_headers(files.get("headers_file", Path("config/headers.txt")))
if headers_list:
    headers_item = random.choice(headers_list)
    cookies = headers_item.pop("cookies", {}) if "cookies" in headers_item else {}
    headers = headers_item
else:
    headers = {}
    cookies = {}

proxy_list = load_file_lines(files.get("proxy_file", Path("config/proxy.txt")))
proxy = random.choice(proxy_list) if proxy_list else None

# 加载爬虫配置
scraper_config = config_manager.load_config(
    "scraper", DEFAULT_CONFIG_TEMPLATES["scraper"]
)

# 构建最终统一配置
CONFIG = {
    "paths": paths,
    "files": files,
    "headers": headers,
    "cookies": cookies,
    "proxy_list": proxy_list,
    "proxy": proxy,
    "scraper": scraper_config,
}

# 导出常用配置项，保持向后兼容性
HEADERS_FILE = files.get("headers_file")
PROXY_FILE = files.get("proxy_file")
SEARCH_CACHE_FILE = files.get("search_cache_file")
LOG_FILE = files.get("log_file")

CACHE_DIR = paths.get("cache_dir")
LOG_DIR = paths.get("log_dir")

HEADERS = headers
PROXY_LIST = proxy_list
PROXY = proxy
DEFAULT_CONFIG = scraper_config
