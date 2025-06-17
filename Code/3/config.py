"""
Centralized configuration management for the project.
Provides unified access to all configuration settings and logging setup.
"""

import logging
import random
import re
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional

from utils.config_manager import (
    OptimizedConfigManager as ConfigManager,
    DEFAULT_CONFIG_TEMPLATES,
)

logger = logging.getLogger(__name__)


class OptimizedProjectConfig:
    """Centralized configuration management with singleton pattern."""

    _instance = None
    _initialized = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self.config_manager = ConfigManager()
        self.config_manager.ensure_default_configs()

        self._configs = {
            name: self.config_manager.load_config(name, template)
            for name, template in DEFAULT_CONFIG_TEMPLATES.items()
        }

        self._setup_paths()
        self._load_external_files()
        self._initialized = True

    def _setup_paths(self):
        """Setup and ensure all required paths exist."""
        self.paths = {key: Path(value) for key, value in self._configs["paths"].items()}
        self.files = {}

        for key, value in self._configs["files"].items():
            base_path = Path(value)
            if not base_path.is_absolute() and key.endswith("_file"):
                dir_key = f"{key.rsplit('_', 1)[0]}_dir"
                self.files[key] = (
                    self.paths.get(dir_key, Path()) / base_path.name
                    if dir_key in self.paths
                    else base_path
                )
            else:
                self.files[key] = base_path

        # Ensure directories exist (batch operation)
        dirs_to_create = [
            path
            for key, path in self.paths.items()
            if key.endswith("_dir") and not path.exists()
        ]
        for path in dirs_to_create:
            try:
                path.mkdir(parents=True, exist_ok=True)
                logger.debug(f"Created directory: {path}")
            except Exception as e:
                logger.error(f"Failed to create directory: {path}, error: {e}")



    @lru_cache(maxsize=1)
    def _load_external_files(self):
        """Load external configuration files like headers and proxies (cached)."""
        headers_file = self.files.get("headers_file", Path("config/headers.txt"))
        proxy_file = self.files.get("proxy_file", Path("config/proxy.txt"))

        self.headers_list = self._load_http_headers(headers_file)
        self.proxy_list = self._load_file_lines(proxy_file)

        # Select random headers and proxy
        if self.headers_list:
            headers_item = random.choice(self.headers_list).copy()
            self.cookies = headers_item.pop("cookies", {})
            self.headers = headers_item
        else:
            self.headers = {}
            self.cookies = {}

        self.proxy = random.choice(self.proxy_list) if self.proxy_list else None

    @staticmethod
    def _parse_cookies(line: str) -> Dict[str, str]:
        """Parse a 'Cookie:' header line into a dictionary."""
        return {
            re.sub(r"[\[\]]", "", k): v
            for cookie in line.split("; ")
            if "=" in cookie
            for k, v in [cookie.split("=", 1)]
        }

    def _load_http_headers(self, file_path: Path) -> List[Dict[str, str]]:
        """Load HTTP headers from a file."""
        try:
            if not file_path.exists():
                logger.warning(f"Headers file not found: {file_path}")
                return []

            content = file_path.read_text(encoding="utf-8")
            headers_list = []

            for block in content.strip().split("\n\n"):
                if not block.strip():
                    continue

                headers_dict = {}
                for line in block.strip().split("\n"):
                    line = line.strip()
                    if not line or line.startswith(("GET ", "HTTP/")):
                        continue

                    if ":" in line:
                        key, value = line.split(":", 1)
                        key, value = key.strip(), value.strip()

                        if key.lower() == "cookie":
                            headers_dict["cookies"] = self._parse_cookies(value)
                        else:
                            headers_dict[key] = value

                if headers_dict:
                    headers_list.append(headers_dict)

            return headers_list
        except Exception as e:
            logger.error(f"Error loading headers from {file_path}: {e}")
            return []

    @staticmethod
    def _load_file_lines(file_path: Path) -> List[str]:
        """Load non-empty lines from a text file."""
        try:
            if not file_path.exists():
                logger.warning(f"File not found: {file_path}")
                return []

            return [
                line.strip()
                for line in file_path.read_text(encoding="utf-8").splitlines()
                if line.strip() and not line.strip().startswith("#")
            ]
        except Exception as e:
            logger.error(f"Error loading file {file_path}: {e}")
            return []

    def get_raw_config(self, name: str) -> Dict[str, Any]:
        """Get raw configuration dictionary for a specific config name."""
        return self._configs.get(name, {})

    def get_config(
        self, name: str, key: Optional[str] = None, default: Any = None
    ) -> Any:
        """Get configuration value with optional nested key access."""
        config = self._configs.get(name, {})
        if key is None:
            return config

        # Support nested key access (e.g., "server.host")
        keys = key.split(".")
        value = config
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        return value

    @property
    def config(self) -> Dict[str, Any]:
        """Get the unified configuration dictionary."""
        return {
            "paths": self.paths,
            "files": self.files,
            "headers": self.headers,
            "cookies": self.cookies,
            "proxy_list": self.proxy_list,
            "proxy": self.proxy,
            "scraper": self.get_raw_config("scraper"),
            "ollama": self.get_raw_config("ollama"),
            "logging": self.get_raw_config("logging"),
            "api": self.get_raw_config("api"),
        }


# Create global configuration instance (singleton)
_config = OptimizedProjectConfig()

# Export configuration for backward compatibility
CONFIG = _config.config
paths = _config.paths
files = _config.files

# Export commonly used items
HEADERS_FILE = files.get("headers_file")
PROXY_FILE = files.get("proxy_file")
SEARCH_CACHE_FILE = files.get("search_cache_file")
LOG_FILE = files.get("log_file")

# Export all log files (kept for backward compatibility)
SCRAPER_LOG_FILE = files.get("scraper_log_file")
OLLAMA_LOG_FILE = files.get("ollama_log_file")
CACHE_LOG_FILE = files.get("cache_log_file")
CONFIG_LOG_FILE = files.get("config_log_file")
MAIN_LOG_FILE = files.get("main_log_file")
UTILS_LOG_FILE = files.get("utils_log_file")
API_LOG_FILE = files.get("api_log_file")

CACHE_DIR = paths.get("cache_dir")
LOG_DIR = paths.get("log_dir")
API_DIR = paths.get("api_dir")

HEADERS = _config.headers
PROXY_LIST = _config.proxy_list
PROXY = _config.proxy
DEFAULT_CONFIG = _config.get_raw_config("scraper")
OLLAMA_CONFIG = _config.get_raw_config("ollama")
API_CONFIG = _config.get_raw_config("api")

# For backward compatibility, expose config manager
config_manager = _config.config_manager


# Helper function to get logger with appropriate log file
def get_module_logger(
    module_name: str, log_level: int = logging.INFO, log_to_console: bool = True
) -> logging.Logger:
    """Get a logger configured for the specific module with appropriate log file."""
    from utils.logging_utils import setup_module_logger

    return setup_module_logger(
        name=module_name,
        log_level=log_level,
        config_files=files,  # Use files mapping directly
        log_to_console=log_to_console,
    )


# Expose unified config access
def get_config(name: str, key: Optional[str] = None, default: Any = None) -> Any:
    """Unified configuration access function."""
    return _config.get_config(name, key, default)
