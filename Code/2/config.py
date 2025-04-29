from typing import List, Dict, TypeVar
import logging
from pathlib import Path
import re
import random

from utils.config_manager import ConfigManager, DEFAULT_CONFIG_TEMPLATES

T = TypeVar("T")

# Create logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Initialize configuration manager
config_manager = ConfigManager()
config_manager.ensure_default_configs()  # Ensure default configurations exist

# Load path configurations from config files
paths_config = config_manager.load_config("paths", DEFAULT_CONFIG_TEMPLATES["paths"])
files_config = config_manager.load_config("files", DEFAULT_CONFIG_TEMPLATES["files"])

# Create actual path objects
paths = {key: Path(value) for key, value in paths_config.items()}
files = {}
for key, value in files_config.items():
    base_path = Path(value)
    # If it's a relative path, base it on the corresponding directory
    if not base_path.is_absolute() and key.endswith("_file"):
        dir_key = f"{key.rsplit('_', 1)[0]}_dir"
        if dir_key in paths:
            files[key] = paths[dir_key] / base_path.name
        else:
            files[key] = base_path
    else:
        files[key] = base_path

# Ensure all directories exist
for key, path in paths.items():
    if key.endswith("_dir") and not path.exists():
        try:
            path.mkdir(parents=True, exist_ok=True)
            logger.debug(f"[CONFIG]: Created directory: {path}")
        except Exception as e:
            logger.error(f"[CONFIG]: Failed to create directory: {path}, error: {e}")


def parse_cookies(line: str) -> Dict[str, str]:
    """Parse cookie line"""
    cookies_dict = {}
    for cookie in line.split("; "):
        if "=" in cookie:
            k, v = cookie.split("=", 1)
            cookies_dict[re.sub(r"[\[\]]", "", k)] = v
    return cookies_dict


def load_http_headers(file_path: Path) -> List[Dict[str, str]]:
    """
    Load HTTP headers information

    Args:
        file_path: Path to headers file

    Returns:
        List of header dictionaries
    """
    try:
        if not file_path.exists():
            logger.debug(f"HTTP headers file does not exist: {file_path}")
            return []

        with file_path.open("r", encoding="utf-8") as f:
            content = f.read()

        # Split by blank lines to separate header blocks
        header_blocks = [
            block.strip() for block in content.split("\n\n") if block.strip()
        ]
        if not header_blocks:
            return []

        headers_list_ = []
        for block in header_blocks:
            headers_dict = {}
            lines = block.splitlines()
            for line in lines[1:]:  # Skip first line
                if ": " in line:
                    key, value = line.split(": ", 1)
                    if key.lower() == "cookie":
                        cookies_dict = parse_cookies(value)
                        headers_dict["cookies"] = cookies_dict
                    headers_dict[key] = value
            if headers_dict:
                headers_list_.append(headers_dict)
        return headers_list_
    except Exception as headers_load_error:
        logger.error(
            f"Failed to load HTTP headers file: {file_path}, error: {headers_load_error}"
        )
        return []


def load_file_lines(file_path: Path) -> List[str]:
    """
    Load lines from a file

    Args:
        file_path: Path to the file

    Returns:
        List of non-empty lines
    """
    try:
        if not file_path.exists():
            return []
        with file_path.open("r", encoding="utf-8") as f:
            return [line.strip() for line in f if line.strip()]
    except Exception as file_load_error:
        logger.error(f"Failed to load file: {file_path}, error: {file_load_error}")
        return []


# Load HTTP headers and proxies
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

# Load scraper configuration
scraper_config = config_manager.load_config(
    "scraper", DEFAULT_CONFIG_TEMPLATES["scraper"]
)

# Build final unified configuration
CONFIG = {
    "paths": paths,
    "files": files,
    "headers": headers,
    "cookies": cookies,
    "proxy_list": proxy_list,
    "proxy": proxy,
    "scraper": scraper_config,
}

# Export commonly used configuration items, maintain backward compatibility
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
