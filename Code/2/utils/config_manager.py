import logging
from typing import Any, Dict, List, Optional, Union
from pathlib import Path
import json

# Create logger
logger = logging.getLogger(__name__)

# Default configuration templates
DEFAULT_CONFIG_TEMPLATES = {
    "paths": {
        "config_dir": "config",
        "cache_dir": "cache",
        "log_dir": "logs",
        "data_dir": "data",
    },
    "scraper": {
        "max_semaphore": 25,
        "batch_size": 25,
        "timeout": 3,
        "retries": 0,
        "min_sleep": 0.1,
        "max_sleep": 0.3,
        "max_redirects": 5,
        "cache_size": 1000,
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
    },
}

# Default HTTP headers template
HEADERS_TEMPLATE = """
GET / HTTP/1.1
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


class ConfigManager:
    """Configuration manager for unified project configuration management"""

    def __init__(
        self, config_dir: Union[str, Path] = "config", create_if_missing: bool = True
    ):
        """
        Initialize configuration manager

        Args:
            config_dir: Configuration directory path
            create_if_missing: Whether to create the directory if it doesn't exist
        """
        self.config_dir = Path(config_dir)
        self.config_cache: Dict[str, Dict[str, Any]] = {}

        # Create configuration directory if needed and it doesn't exist
        if create_if_missing and not self.config_dir.exists():
            try:
                self.config_dir.mkdir(parents=True, exist_ok=True)
                logger.info(f"Created configuration directory: {self.config_dir}")
            except Exception as e:
                logger.error(f"Failed to create configuration directory: {e}")

    def get_config_path(self, name: str) -> Path:
        """
        Get configuration file path

        Args:
            name: Configuration name (without .json extension)

        Returns:
            Configuration file path
        """
        return self.config_dir / f"{name}.json"

    def config_exists(self, name: str) -> bool:
        """
        Check if configuration file exists

        Args:
            name: Configuration name

        Returns:
            Whether the configuration file exists
        """
        return self.get_config_path(name).exists()

    def load_config(
        self, name: str, default: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Load configuration file

        Args:
            name: Configuration name (without .json extension)
            default: Default value if configuration doesn't exist

        Returns:
            Configuration dictionary
        """
        # If configuration is cached, return directly
        if name in self.config_cache:
            return self.config_cache[name]

        # Get configuration file path
        config_path = self.get_config_path(name)

        # If configuration file doesn't exist, return default value
        if not config_path.exists():
            logger.debug(f"Configuration file doesn't exist: {config_path}")
            return {} if default is None else default

        # Try to load configuration
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                config = json.load(f)

            # Cache and return configuration
            self.config_cache[name] = config
            logger.debug(f"Successfully loaded configuration: {name}")
            return config

        except json.JSONDecodeError as e:
            logger.error(
                f"Failed to parse configuration file: {config_path}, error: {e}"
            )
            return {} if default is None else default
        except Exception as e:
            logger.error(
                f"Failed to load configuration file: {config_path}, error: {e}"
            )
            return {} if default is None else default

    def get(self, name: str, key: str, default: Any = None) -> Any:
        """
        Get configuration item

        Args:
            name: Configuration name
            key: Configuration item key or path (use . for nested, e.g., 'server.host')
            default: Default value

        Returns:
            Configuration value
        """
        config = self.load_config(name)

        # Handle nested keys
        if "." in key:
            parts = key.split(".")
            # Recursively get nested value
            value = config
            for part in parts:
                if not isinstance(value, dict) or part not in value:
                    return default
                value = value[part]
            return value

        return config.get(key, default)

    def get_all_configs(self) -> List[str]:
        """
        Get all available configuration names

        Returns:
            List of configuration names
        """
        configs = []
        for file in self.config_dir.glob("*.json"):
            configs.append(file.stem)
        return configs

    def deep_merge(
        self, base: Dict[str, Any], override: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Deep merge two configuration dictionaries

        Args:
            base: Base configuration
            override: Configuration to override

        Returns:
            Merged configuration
        """
        result = base.copy()

        for key, value in override.items():
            # If both sides are dictionaries, recursively merge
            if (
                key in result
                and isinstance(result[key], dict)
                and isinstance(value, dict)
            ):
                result[key] = self.deep_merge(result[key], value)
            else:
                # Otherwise directly override
                result[key] = value

        return result

    def save_config(
        self, name: str, config: Dict[str, Any], merge: bool = False
    ) -> bool:
        """
        Save configuration to file

        Args:
            name: Configuration name
            config: Configuration dictionary
            merge: Whether to merge with existing configuration

        Returns:
            Whether saving was successful
        """
        # Ensure configuration directory exists
        self.config_dir.mkdir(parents=True, exist_ok=True)

        # Get configuration file path
        config_path = self.get_config_path(name)

        # If merging is required, load existing configuration first
        if merge and config_path.exists():
            try:
                with open(config_path, "r", encoding="utf-8") as f:
                    existing_config = json.load(f)
                # Deep merge configurations
                merged_config = self.deep_merge(existing_config, config)
                config = merged_config
            except Exception as e:
                logger.warning(
                    f"Configuration merge failed: {e}, will directly overwrite"
                )

        # Save configuration
        try:
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

    def set(self, name: str, key: str, value: Any, create_parents: bool = True) -> bool:
        """
        Set configuration item

        Args:
            name: Configuration name
            key: Configuration item key or path (use . for nested, e.g., 'server.host')
            value: Configuration value
            create_parents: Whether to create non-existent parent configurations

        Returns:
            Whether setting was successful
        """
        config = self.load_config(name)

        # Handle nested keys
        if "." in key:
            parts = key.split(".")
            last_key = parts.pop()

            # Recursively find or create parent configurations
            current = config
            for part in parts:
                if part not in current or not isinstance(current[part], dict):
                    if create_parents:
                        current[part] = {}
                    else:
                        logger.error(f"Parent configuration does not exist: {part}")
                        return False
                current = current[part]

            current[last_key] = value
        else:
            config[key] = value

        return self.save_config(name, config)

    def ensure_default_configs(self) -> bool:
        """
        Ensure all default configuration files exist, create if missing

        Returns:
            Whether all configurations were successfully created
        """
        success = True

        # Create base directories with standardized logging
        paths_config = DEFAULT_CONFIG_TEMPLATES["paths"]
        for dir_name in paths_config.values():
            path = Path(dir_name)
            if not path.exists():
                try:
                    path.mkdir(parents=True, exist_ok=True)
                    logger.debug(f"[CONFIG_MANAGER]: Created directory: {path}")
                except Exception as e:
                    logger.error(
                        f"[CONFIG_MANAGER]: Failed to create directory: {path}, error: {e}"
                    )
                    success = False

        # Write configuration files
        for name, config in DEFAULT_CONFIG_TEMPLATES.items():
            if not self.config_exists(name):
                if not self.save_config(name, config):
                    logger.error(
                        f"[CONFIG_MANAGER]: Failed to create default configuration: {name}"
                    )
                    success = False
                else:
                    logger.debug(
                        f"[CONFIG_MANAGER]: Created default configuration: {name}"
                    )
            else:
                logger.debug(
                    f"[CONFIG_MANAGER]: Configuration already exists, skipping creation: {name}"
                )

        # Create default headers.txt
        headers_path = Path(paths_config["config_dir"]) / "headers.txt"
        if not headers_path.exists():
            try:
                with open(headers_path, "w", encoding="utf-8") as f:
                    f.write(HEADERS_TEMPLATE)
                logger.debug(f"Created default headers file: {headers_path}")
            except Exception as e:
                logger.error(
                    f"Failed to create headers file: {headers_path}, error: {e}"
                )
                success = False

        # Create empty proxy file
        proxy_path = Path(paths_config["config_dir"]) / "proxy.txt"
        if not proxy_path.exists():
            try:
                with open(proxy_path, "w", encoding="utf-8") as f:
                    f.write(
                        "# One proxy per line, format: http://host:port or https://host:port\n"
                    )
                logger.debug(f"Created default proxy file: {proxy_path}")
            except Exception as e:
                logger.error(f"Failed to create proxy file: {proxy_path}, error: {e}")
                success = False

        return success

    def delete(self, name: str, key: Optional[str] = None) -> bool:
        """
        Delete configuration item or entire configuration file

        Args:
            name: Configuration name
            key: Configuration item key to delete (None means delete entire configuration file)

        Returns:
            Whether deletion was successful
        """
        if key is None:
            # Delete entire configuration file
            config_path = self.get_config_path(name)
            if not config_path.exists():
                return True

            try:
                config_path.unlink()
                if name in self.config_cache:
                    del self.config_cache[name]
                logger.debug(f"Deleted configuration file: {config_path}")
                return True
            except Exception as e:
                logger.error(
                    f"Failed to delete configuration file: {config_path}, error: {e}"
                )
                return False
        else:
            # Delete specific configuration item
            config = self.load_config(name)
            if not config:
                return True

            if "." in key:
                # Handle nested keys
                parts = key.split(".")
                last_key = parts.pop()

                # Recursively find parent configuration
                current = config
                for part in parts:
                    if part not in current or not isinstance(current[part], dict):
                        return (
                            True  # Parent doesn't exist, consider deletion successful
                        )
                    current = current[part]

                if last_key in current:
                    del current[last_key]
            else:
                if key in config:
                    del config[key]

            return self.save_config(name, config)

    def clear_cache(self, name: Optional[str] = None) -> None:
        """
        Clear configuration cache

        Args:
            name: Configuration name to clear (None means clear all)
        """
        if name is None:
            self.config_cache.clear()
            logger.debug("Cleared all configuration caches")
        elif name in self.config_cache:
            del self.config_cache[name]
            logger.debug(f"Cleared configuration cache: {name}")
