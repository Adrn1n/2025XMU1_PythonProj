"""
Configuration manager for unified project configuration management.
Provides centralized access to configuration settings and templates.
"""

import json
import logging
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Optional, Union

# 创建一个简单的logger以避免循环导入
logger = logging.getLogger("config_manager")

DEFAULT_CONFIG_TEMPLATES = {
    "paths": {
        "config_dir": "config",
        "cache_dir": "cache",
        "log_dir": "logs",
        "data_dir": "data",
        "api_dir": "api",
    },
    "scraper": {
        "filter_ads": True,
        "max_semaphore": 25,
        "batch_size": 25,
        "max_concurrent_pages": 5,
        "timeout": 3,
        "retries": 0,
        "min_sleep": 0.1,
        "max_sleep": 0.3,
        "max_redirects": 5,
        "cache_size": 1000,
    },
    "api": {
        "api_keys_file": "api_keys.txt",
        "auth_required": True,
        "cache_models": True,
        "models_cache_ttl": 300,
        "cache_api_keys": True,
        "api_key_cache_ttl": 300,
        "search": {
            "use_search_for_api": True,
            "api_search_pages": 5,
            "api_search_timeout": 5,
            "api_search_retries": 1,
            "search_command_enabled": True,
            "no_search_command_enabled": True,
        },
        "performance": {
            "max_concurrent_pages": 3,
            "max_semaphore": 15,
            "batch_size": 15,
            "connection_pool_size": 20,
            "chunk_size_words": 3,
            "streaming_delay": 0.05,
        },
        "server": {
            "host": "0.0.0.0",
            "port": 8000,
            "reload": False,
            "workers": 1,
            "log_level": "info",
            "access_log": True,
            "keep_alive": 65,
        },
        "cors": {
            "allow_origins": ["*"],
            "allow_credentials": True,
            "allow_methods": ["*"],
            "allow_headers": ["*"],
        },
        "security": {
            "rate_limiting": False,
            "max_requests_per_minute": 60,
            "trusted_proxies": [],
            "enable_api_versioning": True,
        },
    },
    "ollama": {
        "base_url": "http://localhost:11434",
        "default_model": "llama3",
        "timeout": 60,
        "max_retries": 3,
        "stream": True,
        "temperature": 0.7,
        "top_p": 0.9,
        "top_k": 40,
        "context_size": 2048,
        "max_tokens": None,
    },
    "logging": {
        "console_level": "INFO",
        "file_level": "DEBUG",
        "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        "date_format": "%Y-%m-%d %H:%M:%S",
    },
    "files": {
        "headers_file": "config/headers.txt",
        "proxy_file": "config/proxy.txt",
        "search_cache_file": "cache/baidu_search_res.json",
        "log_file": "logs/scraper.log",
        "scraper_log_file": "logs/scraper.log",
        "ollama_log_file": "logs/ollama.log",
        "cache_log_file": "logs/cache.log",
        "config_log_file": "logs/config.log",
        "main_log_file": "logs/main.log",
        "utils_log_file": "logs/utils.log",
        "api_log_file": "logs/api.log",
    },
}

HEADERS_TEMPLATE = """GET / HTTP/1.1
Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7
Accept-Encoding: gzip, deflate, br, zstd
Accept-Language: en-US,en;q=0.9
Cache-Control: max-age=0
Connection: keep-alive
Cookie:
DNT: 1
Host: www.baidu.com
Sec-Fetch-Dest: document
Sec-Fetch-Mode: navigate
Sec-Fetch-Site: none
Sec-Fetch-User: ?1
Upgrade-Insecure-Requests: 1
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36
sec-ch-ua: "Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"
sec-ch-ua-mobile: ?0
sec-ch-ua-platform: "Windows"

"""


class OptimizedConfigManager:
    """Configuration manager with performance optimization and caching."""

    def __init__(
        self, config_dir: Union[str, Path] = "config", create_if_missing: bool = True
    ):
        self.config_dir = Path(config_dir)
        self.config_cache: Dict[str, Dict[str, Any]] = {}

        if create_if_missing and not self.config_dir.exists():
            try:
                self.config_dir.mkdir(parents=True, exist_ok=True)
                logger.info(f"Created configuration directory: {self.config_dir}")
            except Exception as e:
                logger.error(f"Failed to create configuration directory: {e}")

    @lru_cache(maxsize=128)
    def get_config_path(self, name: str) -> Path:
        """Get configuration file path with caching."""
        return self.config_dir / f"{name}.json"

    def config_exists(self, name: str) -> bool:
        """Check if configuration file exists."""
        return self.get_config_path(name).exists()

    def load_config(
        self, name: str, default: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Load configuration with caching support."""
        if name in self.config_cache:
            return self.config_cache[name]

        config_path = self.get_config_path(name)

        if not config_path.exists():
            logger.debug(f"Configuration file doesn't exist: {config_path}")
            return default or {}

        try:
            with open(config_path, "r", encoding="utf-8") as f:
                config = json.load(f)

            self.config_cache[name] = config
            logger.debug(f"Successfully loaded configuration: {name}")
            return config

        except json.JSONDecodeError as e:
            logger.error(
                f"Failed to parse configuration file: {config_path}, error: {e}"
            )
            return default or {}
        except Exception as e:
            logger.error(
                f"Failed to load configuration file: {config_path}, error: {e}"
            )
            return default or {}

    def get(self, name: str, key: str, default: Any = None) -> Any:
        """Get specific configuration value."""
        config = self.load_config(name)
        return config.get(key, default)

    def save_config(
        self, name: str, config: Dict[str, Any], merge: bool = False
    ) -> bool:
        """Save configuration with optional merging."""
        config_path = self.get_config_path(name)

        try:
            # Ensure directory exists
            config_path.parent.mkdir(parents=True, exist_ok=True)

            if merge and config_path.exists():
                existing_config = self.load_config(name)
                config = self._deep_merge(existing_config, config)

            with open(config_path, "w", encoding="utf-8") as f:
                json.dump(config, f, ensure_ascii=False, indent=2)

            # Update cache
            self.config_cache[name] = config
            logger.debug(f"Successfully saved configuration: {name}")
            return True

        except Exception as e:
            logger.error(
                f"Failed to save configuration file: {config_path}, error: {e}"
            )
            return False

    def _deep_merge(
        self, base: Dict[str, Any], override: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Deep merge two dictionaries."""
        result = base.copy()
        for key, value in override.items():
            if (
                key in result
                and isinstance(result[key], dict)
                and isinstance(value, dict)
            ):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value
        return result

    def ensure_default_configs(self) -> bool:
        """Ensure all default configurations exist."""
        overall_success = True

        # Create directories
        paths_config = DEFAULT_CONFIG_TEMPLATES["paths"]
        for dir_name in paths_config.values():
            path = Path(dir_name)
            if not path.exists():
                try:
                    path.mkdir(parents=True, exist_ok=True)
                    logger.debug(f"Created directory: {path}")
                except Exception as e:
                    logger.error(f"Failed to create directory: {path}, error: {e}")
                    overall_success = False

        # Create configuration files
        for name, config_template in DEFAULT_CONFIG_TEMPLATES.items():
            if not self.config_exists(name):
                if not self.save_config(name, config_template):
                    logger.error(f"Failed to create default configuration: {name}")
                    overall_success = False
                else:
                    logger.debug(f"Created default configuration: {name}")

        # Create headers file
        headers_path = Path(paths_config["config_dir"]) / "headers.txt"
        if not headers_path.exists():
            try:
                headers_path.write_text(HEADERS_TEMPLATE, encoding="utf-8")
                logger.debug(f"Created default headers file: {headers_path}")
            except Exception as e:
                logger.error(
                    f"Failed to create headers file: {headers_path}, error: {e}"
                )
                overall_success = False

        # Create proxy file
        proxy_path = Path(paths_config["config_dir"]) / "proxy.txt"
        if not proxy_path.exists():
            try:
                proxy_path.write_text(
                    "# One proxy per line, format: http://host:port or https://host:port\n",
                    encoding="utf-8",
                )
                logger.debug(f"Created default proxy file: {proxy_path}")
            except Exception as e:
                logger.error(f"Failed to create proxy file: {proxy_path}, error: {e}")
                overall_success = False

        return overall_success

    def clear_cache(self, name: Optional[str] = None) -> None:
        """Clear configuration cache."""
        if name:
            self.config_cache.pop(name, None)
        else:
            self.config_cache.clear()
