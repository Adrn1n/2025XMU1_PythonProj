"""
Centralized configuration management for the project.
Provides unified access to all configuration settings and logging setup.
"""

import logging
import random
import re
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from utils.config_manager import (
    OptimizedConfigManager as ConfigManager,
    DEFAULT_CONFIG_TEMPLATES,
)

logger = None  # 将在模块末尾初始化


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

        # Create module-specific log file mapping (simplified mapping)
        self.module_log_files = self._create_module_log_mapping()

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

    def _create_module_log_mapping(self) -> Dict[str, Path]:
        """Create simplified module-to-log-file mapping."""
        return {
            # Direct module name mapping (no need for complex mappings)
            "api": self.files.get("api_log_file"),
            "scrapers": self.files.get("scraper_log_file"),
            "ollama": self.files.get("ollama_log_file"),
            "cache": self.files.get("cache_log_file"),
            "config": self.files.get("config_log_file"),
            "utils": self.files.get("utils_log_file"),
            "main": self.files.get("main_log_file"),
            "log_file": self.files.get("log_file"),  # fallback
        }

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

# Export the simplified module log file mapping
module_log_files = _config.module_log_files

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


# ============================================================================
# 简化的日志函数 - 无需传递字符串参数
# ============================================================================

def get_logger(log_level: int = logging.INFO, log_to_console: bool = True) -> logging.Logger:
    """
    自动获取适合当前模块的日志器（无需传递模块名）。
    
    Args:
        log_level: 日志级别，默认INFO
        log_to_console: 是否输出到控制台，默认True
    
    Returns:
        配置好的Logger实例
    
    Usage:
        from config import get_logger
        logger = get_logger()
        logger.info("Log message")
    """
    from utils.logging_utils import get_logger as _get_logger
    return _get_logger(log_level, log_to_console)


def get_class_logger(cls_instance: object) -> logging.Logger:
    """
    为类实例获取日志器，自动使用类名。
    
    Args:
        cls_instance: 类实例（通常传入self）
    
    Returns:
        配置好的Logger实例
    
    Usage:
        class MyClass:
            def __init__(self):
                self.logger = get_class_logger(self)
    """
    from utils.logging_utils import get_class_logger as _get_class_logger
    return _get_class_logger(cls_instance)


def get_current_logger() -> logging.Logger:
    """
    获取当前模块的日志器实例
    
    Returns:
        当前模块配置好的Logger实例
    
    Usage:
        logger = get_current_logger()
        logger.info("Using logger directly")
    """
    from utils.logging_utils import get_current_logger as _get_current_logger
    return _get_current_logger()


# ============================================================================
# 直接日志函数 - 最简化的使用方式
# ============================================================================

def log_debug(message: str, *args, **kwargs) -> None:
    """直接记录DEBUG日志，自动检测模块"""
    from utils.logging_utils import log_debug as _log_debug
    _log_debug(message, *args, **kwargs)


def log_info(message: str, *args, **kwargs) -> None:
    """直接记录INFO日志，自动检测模块"""
    from utils.logging_utils import log_info as _log_info
    _log_info(message, *args, **kwargs)


def log_warning(message: str, *args, **kwargs) -> None:
    """直接记录WARNING日志，自动检测模块"""
    from utils.logging_utils import log_warning as _log_warning
    _log_warning(message, *args, **kwargs)


def log_error(message: str, *args, **kwargs) -> None:
    """直接记录ERROR日志，自动检测模块"""
    from utils.logging_utils import log_error as _log_error
    _log_error(message, *args, **kwargs)


def log_critical(message: str, *args, **kwargs) -> None:
    """直接记录CRITICAL日志，自动检测模块"""
    from utils.logging_utils import log_critical as _log_critical
    _log_critical(message, *args, **kwargs)


# Expose unified config access
def get_config(name: str, key: Optional[str] = None, default: Any = None) -> Any:
    """Unified configuration access function."""
    return _config.get_config(name, key, default)
