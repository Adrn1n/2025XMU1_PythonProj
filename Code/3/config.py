from typing import List, Dict, TypeVar
import logging
from pathlib import Path
import re
import random

from utils.config_manager import ConfigManager, DEFAULT_CONFIG_TEMPLATES

T = TypeVar("T")

# Create a logger for this module
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)  # Default level, can be overridden by application setup

# Initialize the configuration manager
config_manager = ConfigManager()
# Ensure default configuration files and directories exist
config_manager.ensure_default_configs()

# Load path configurations from the 'paths.json' file
paths_config = config_manager.load_config("paths", DEFAULT_CONFIG_TEMPLATES["paths"])
# Load file path configurations from the 'files.json' file
files_config = config_manager.load_config("files", DEFAULT_CONFIG_TEMPLATES["files"])
# Load Ollama configurations
ollama_config = config_manager.load_config("ollama", DEFAULT_CONFIG_TEMPLATES["ollama"])

# Convert path strings from config into Path objects
paths = {key: Path(value) for key, value in paths_config.items()}
files = {}
# Resolve file paths, making relative paths based on corresponding directories
for key, value in files_config.items():
    base_path = Path(value)
    # If the path is relative and it's a file path (ends with '_file')
    if not base_path.is_absolute() and key.endswith("_file"):
        # Construct the corresponding directory key (e.g., 'log_file' -> 'log_dir')
        dir_key = f"{key.rsplit('_', 1)[0]}_dir"
        if dir_key in paths:
            # Join the directory path with the filename
            files[key] = paths[dir_key] / base_path.name
        else:
            # If no corresponding directory is defined, use the relative path as is
            files[key] = base_path
    else:
        # Use absolute paths directly
        files[key] = base_path

# Ensure all configured directories exist
for key, path in paths.items():
    if key.endswith("_dir") and not path.exists():
        try:
            path.mkdir(parents=True, exist_ok=True)
            logger.debug(f"[CONFIG]: Created directory: {path}")
        except Exception as e:
            logger.error(f"[CONFIG]: Failed to create directory: {path}, error: {e}")


def parse_cookies(line: str) -> Dict[str, str]:
    """Parse a 'Cookie:' header line into a dictionary."""
    cookies_dict = {}
    for cookie in line.split("; "):
        if "=" in cookie:
            k, v = cookie.split("=", 1)
            # Remove potential brackets from cookie names (observed in some cases)
            cookies_dict[re.sub(r"[\[\]]", "", k)] = v
    return cookies_dict


def load_http_headers(file_path: Path) -> List[Dict[str, str]]:
    """
    Load HTTP headers from a file. Handles multiple header blocks separated by blank lines.

    Args:
        file_path: Path object pointing to the headers file.

    Returns:
        A list of dictionaries, each representing a set of headers.
        Includes a 'cookies' key if a Cookie header is present.
    """
    try:
        if not file_path.exists():
            logger.debug(f"HTTP headers file does not exist: {file_path}")
            return []

        with file_path.open("r", encoding="utf-8") as f:
            content = f.read()

        # Split the content by double newlines to handle multiple header sets
        header_blocks = [
            block.strip() for block in content.split("\n\n") if block.strip()
        ]
        if not header_blocks:
            logger.warning(f"No valid header blocks found in {file_path}")
            return []

        headers_list_ = []
        for block in header_blocks:
            headers_dict = {}
            lines = block.splitlines()
            # Skip the first line (e.g., "GET / HTTP/1.1")
            for line in lines[1:]:
                if ": " in line:
                    key, value = line.split(": ", 1)
                    # Special handling for the Cookie header
                    if key.lower() == "cookie":
                        cookies_dict = parse_cookies(value)
                        # Store parsed cookies under a separate 'cookies' key
                        headers_dict["cookies"] = cookies_dict
                    headers_dict[key] = value
            if headers_dict:
                headers_list_.append(headers_dict)
        logger.info(f"Loaded {len(headers_list_)} header sets from {file_path}")
        return headers_list_
    except Exception as headers_load_error:
        logger.error(
            f"Failed to load HTTP headers file: {file_path}, error: {headers_load_error}"
        )
        return []


def load_file_lines(file_path: Path) -> List[str]:
    """
    Load non-empty lines from a text file.

    Args:
        file_path: Path object pointing to the file.

    Returns:
        A list of strings, each representing a non-empty line from the file.
    """
    try:
        if not file_path.exists():
            logger.debug(f"File doesn't exist, cannot load lines: {file_path}")
            return []
        with file_path.open("r", encoding="utf-8") as f:
            # Read lines, strip whitespace, and filter out empty lines
            return [line.strip() for line in f if line.strip()]
    except Exception as file_load_error:
        logger.error(f"Failed to load file: {file_path}, error: {file_load_error}")
        return []


# Load HTTP headers from the configured file path
headers_list = load_http_headers(files.get("headers_file", Path("config/headers.txt")))
# Randomly select one set of headers to use for this run
if headers_list:
    headers_item = random.choice(headers_list)
    # Extract cookies if present, otherwise use an empty dict
    cookies = headers_item.pop("cookies", {})
    headers = headers_item
else:
    # Use empty dicts if no headers were loaded
    headers = {}
    cookies = {}

# Load proxy list from the configured file path
proxy_list = load_file_lines(files.get("proxy_file", Path("config/proxy.txt")))
# Randomly select one proxy to use (if any are available)
proxy = random.choice(proxy_list) if proxy_list else None

# Load scraper-specific configuration from 'scraper.json'
scraper_config = config_manager.load_config(
    "scraper", DEFAULT_CONFIG_TEMPLATES["scraper"]
)

# Build the final unified configuration dictionary accessible to other modules
CONFIG = {
    "paths": paths,
    "files": files,
    "headers": headers,  # The randomly selected headers (without cookies)
    "cookies": cookies,  # The cookies from the selected header set
    "proxy_list": proxy_list,  # The full list of proxies
    "proxy": proxy,  # The randomly selected proxy
    "scraper": scraper_config,  # Scraper-specific settings
    "ollama": ollama_config,  # Ollama-specific settings
}

# Export commonly used configuration items directly for convenience and backward compatibility
HEADERS_FILE = files.get("headers_file")
PROXY_FILE = files.get("proxy_file")
SEARCH_CACHE_FILE = files.get("search_cache_file")
LOG_FILE = files.get("log_file")

CACHE_DIR = paths.get("cache_dir")
LOG_DIR = paths.get("log_dir")

HEADERS = headers  # Export the selected headers
PROXY_LIST = proxy_list  # Export the full proxy list
PROXY = proxy  # Export the selected proxy
DEFAULT_CONFIG = scraper_config  # Export scraper defaults
OLLAMA_CONFIG = ollama_config  # Export Ollama configs
