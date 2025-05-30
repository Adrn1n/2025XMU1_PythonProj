import logging
from typing import Any, Dict, List, Optional, Union
from pathlib import Path
import json

# Create logger for this module
logger = logging.getLogger(__name__)

# Default configuration templates used if config files are missing
DEFAULT_CONFIG_TEMPLATES = {
    "paths": {
        "config_dir": "config",
        "cache_dir": "cache",
        "log_dir": "logs",
        "data_dir": "data",  # Directory for general data storage
    },
    "scraper": {
        "filter_ads": True,  # Whether to filter out advertisement results
        "max_semaphore": 25,  # Max concurrent network requests globally
        "batch_size": 25,  # Number of URLs to process in parallel batches
        "max_concurrent_pages": 5,  # Max search result pages to scrape concurrently
        "timeout": 3,  # Network request timeout in seconds
        "retries": 0,  # Number of retries for failed requests
        "min_sleep": 0.1,  # Minimum delay between requests (seconds)
        "max_sleep": 0.3,  # Maximum delay between requests (seconds)
        "max_redirects": 5,  # Maximum number of redirects to follow for a URL
        "cache_size": 1000,  # Maximum number of items in the URL cache
    },
    "logging": {
        "console_level": "INFO",  # Default log level for console output
        "file_level": "DEBUG",  # Default log level for file output
        "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",  # Log message format
        "date_format": "%Y-%m-%d %H:%M:%S",  # Timestamp format in logs
    },
    "files": {
        "headers_file": "config/headers.txt",  # Path to HTTP headers file
        "proxy_file": "config/proxy.txt",  # Path to proxy list file
        "search_cache_file": "cache/baidu_search_res.json",  # Default cache file for search results (may be overridden)
        "log_file": "logs/scraper.log",  # Default log file path
    },
    "ollama": {
        "base_url": "http://localhost:11434",  # Default Ollama API base URL
        "timeout": 60,  # Default timeout for Ollama API requests
        "stream": True,  # Whether to use streaming response by default
        "temperature": 0.7,  # Default temperature for text generation
        "top_p": 0.9,  # Default top_p value
        "top_k": 40,  # Default top_k value
        "default_model": "",  # Default model to use (empty means user will be prompted)
        "system_prompt": "",  # Default system prompt (empty means use built-in default)
    },
}

# Default HTTP headers template written to headers.txt if it doesn't exist
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

"""  # Note the trailing blank line to potentially separate multiple header blocks


class ConfigManager:
    """Manages loading, saving, and accessing JSON configuration files."""

    def __init__(
        self, config_dir: Union[str, Path] = "config", create_if_missing: bool = True
    ):
        """
        Initialize the ConfigManager.

        Args:
            config_dir: The directory where configuration files are stored.
            create_if_missing: If True, create the config directory if it doesn't exist.
        """
        self.config_dir = Path(config_dir)
        # In-memory cache for loaded configurations to avoid repeated file reads
        self.config_cache: Dict[str, Dict[str, Any]] = {}

        # Create the configuration directory if specified and it doesn't exist
        if create_if_missing and not self.config_dir.exists():
            try:
                self.config_dir.mkdir(parents=True, exist_ok=True)
                logger.info(f"Created configuration directory: {self.config_dir}")
            except Exception as e:
                # Log error but allow continuation; loading will use defaults
                logger.error(f"Failed to create configuration directory: {e}")

    def get_config_path(self, name: str) -> Path:
        """Construct the full path to a configuration file."""
        return self.config_dir / f"{name}.json"

    def config_exists(self, name: str) -> bool:
        """Check if a specific configuration file exists."""
        return self.get_config_path(name).exists()

    def load_config(
        self, name: str, default: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Load a configuration file by name. Returns cached version if available.

        Args:
            name: The name of the configuration (e.g., 'paths', 'scraper').
            default: A default dictionary to return if the file doesn't exist or fails to load.

        Returns:
            The loaded configuration dictionary or the default value.
        """
        # Return cached config if already loaded
        if name in self.config_cache:
            return self.config_cache[name]

        config_path = self.get_config_path(name)

        # If file doesn't exist, return the provided default or an empty dict
        if not config_path.exists():
            logger.debug(f"Configuration file doesn't exist: {config_path}")
            return {} if default is None else default

        # Try loading the JSON configuration from the file
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                config = json.load(f)

            # Store the loaded config in the cache
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
        Get a specific value from a loaded configuration. Supports nested keys.

        Args:
            name: The name of the configuration.
            key: The key of the value to retrieve. Use dot notation for nested keys (e.g., 'server.host').
            default: The default value to return if the key is not found.

        Returns:
            The configuration value or the default.
        """
        config = self.load_config(name)  # Load config (uses cache if available)

        # Handle nested keys using dot notation
        if "." in key:
            parts = key.split(".")
            value = config
            # Traverse the dictionary structure
            for part in parts:
                if not isinstance(value, dict) or part not in value:
                    return default  # Key path not found
                value = value[part]
            return value
        else:
            # Handle top-level keys
            return config.get(key, default)

    def get_all_configs(self) -> List[str]:
        """Get a list of names of all available .json configuration files."""
        configs = []
        for file in self.config_dir.glob("*.json"):
            configs.append(file.stem)  # Get the filename without extension
        return configs

    def deep_merge(
        self, base: Dict[str, Any], override: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Recursively merge two dictionaries. Values in 'override' take precedence.

        Args:
            base: The base dictionary.
            override: The dictionary with overriding values.

        Returns:
            A new dictionary representing the merged result.
        """
        result = base.copy()  # Start with a copy of the base dictionary

        for key, value in override.items():
            # If the key exists in both and both values are dictionaries, merge recursively
            if (
                key in result
                and isinstance(result[key], dict)
                and isinstance(value, dict)
            ):
                result[key] = self.deep_merge(result[key], value)
            else:
                # Otherwise, the value from 'override' replaces the value in 'result'
                result[key] = value

        return result

    def save_config(
        self, name: str, config: Dict[str, Any], merge: bool = False
    ) -> bool:
        """
        Save a configuration dictionary to a JSON file.

        Args:
            name: The name of the configuration.
            config: The configuration dictionary to save.
            merge: If True, merge the provided config with the existing file content before saving.

        Returns:
            True if saving was successful, False otherwise.
        """
        # Ensure the configuration directory exists
        self.config_dir.mkdir(parents=True, exist_ok=True)

        config_path = self.get_config_path(name)
        final_config = config

        # If merging, load the existing config and merge with the new one
        if merge and config_path.exists():
            try:
                with open(config_path, "r", encoding="utf-8") as f:
                    existing_config = json.load(f)
                # Perform a deep merge
                final_config = self.deep_merge(existing_config, config)
            except Exception as e:
                logger.warning(
                    f"Configuration merge failed for '{name}': {e}. Overwriting file."
                )
                # Fallback to overwriting if merge fails

        # Save the final configuration dictionary to the file
        try:
            with open(config_path, "w", encoding="utf-8") as f:
                # Use indent for readability, ensure_ascii=False for non-ASCII chars
                json.dump(final_config, f, ensure_ascii=False, indent=2)

            # Update the in-memory cache with the saved configuration
            self.config_cache[name] = final_config
            logger.debug(f"Successfully saved configuration: {name}")
            return True

        except Exception as e:
            logger.error(
                f"Failed to save configuration file: {config_path}, error: {e}"
            )
            return False

    def set(self, name: str, key: str, value: Any, create_parents: bool = True) -> bool:
        """
        Set a specific value in a configuration and save the file. Supports nested keys.

        Args:
            name: The name of the configuration.
            key: The key to set (use dot notation for nested keys).
            value: The value to set.
            create_parents: If True, create intermediate dictionaries for nested keys if they don't exist.

        Returns:
            True if setting and saving were successful, False otherwise.
        """
        config = self.load_config(name)  # Load current config

        # Handle nested keys
        if "." in key:
            parts = key.split(".")
            last_key = parts.pop()  # The final key to set

            # Traverse or create parent dictionaries
            current = config
            for part in parts:
                if part not in current or not isinstance(current[part], dict):
                    if create_parents:
                        current[part] = {}  # Create missing parent dict
                    else:
                        logger.error(
                            f"Parent configuration does not exist: {part} in {name}"
                        )
                        return False  # Cannot set value if parent doesn't exist and creation is disabled
                current = current[part]  # Move to the next level

            current[last_key] = value  # Set the value at the final level
        else:
            # Handle top-level keys
            config[key] = value

        # Save the modified configuration back to the file
        return self.save_config(name, config)

    def ensure_default_configs(self) -> bool:
        """
        Ensure default directories and configuration files exist, creating them if necessary.

        Uses templates defined in DEFAULT_CONFIG_TEMPLATES and HEADERS_TEMPLATE.

        Returns:
            True if all defaults were ensured successfully, False otherwise.
        """
        overall_success = True

        # Ensure base directories from 'paths' template exist
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
                    overall_success = False  # Mark failure but continue

        # Ensure default JSON configuration files exist
        for name, config_template in DEFAULT_CONFIG_TEMPLATES.items():
            if not self.config_exists(name):
                # Save the default template content to the file
                if not self.save_config(name, config_template):
                    logger.error(
                        f"[CONFIG_MANAGER]: Failed to create default configuration: {name}"
                    )
                    overall_success = False
                else:
                    logger.debug(
                        f"[CONFIG_MANAGER]: Created default configuration: {name}"
                    )
            else:
                logger.debug(
                    f"[CONFIG_MANAGER]: Configuration already exists, skipping creation: {name}"
                )

        # Ensure default headers.txt exists
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
                overall_success = False

        # Ensure default (empty) proxy.txt exists
        proxy_path = Path(paths_config["config_dir"]) / "proxy.txt"
        if not proxy_path.exists():
            try:
                with open(proxy_path, "w", encoding="utf-8") as f:
                    # Add a comment explaining the format
                    f.write(
                        "# One proxy per line, format: http://host:port or https://host:port\n"
                    )
                logger.debug(f"Created default proxy file: {proxy_path}")
            except Exception as e:
                logger.error(f"Failed to create proxy file: {proxy_path}, error: {e}")
                overall_success = False

        return overall_success

    def delete(self, name: str, key: Optional[str] = None) -> bool:
        """
        Delete a specific configuration key or an entire configuration file.

        Args:
            name: The name of the configuration.
            key: The key to delete (dot notation for nested keys). If None, delete the entire file.

        Returns:
            True if deletion was successful, False otherwise.
        """
        if key is None:
            # Delete the entire configuration file
            config_path = self.get_config_path(name)
            if not config_path.exists():
                return True  # File doesn't exist, consider deletion successful

            try:
                config_path.unlink()  # Delete the file
                # Remove from cache if present
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
            # Delete a specific key within the configuration
            config = self.load_config(name)
            if not config:
                return True  # Config is empty or doesn't exist, key effectively deleted

            key_found = False
            # Handle nested keys
            if "." in key:
                parts = key.split(".")
                last_key = parts.pop()

                # Traverse to the parent dictionary
                current = config
                for part in parts:
                    if part not in current or not isinstance(current[part], dict):
                        return (
                            True  # Parent path doesn't exist, key effectively deleted
                        )
                    current = current[part]

                # Delete the key if it exists in the parent
                if last_key in current:
                    del current[last_key]
                    key_found = True
            else:
                # Handle top-level keys
                if key in config:
                    del config[key]
                    key_found = True

            # If the key was found and deleted, save the modified config
            if key_found:
                return self.save_config(name, config)
            else:
                return True  # Key wasn't found, consider deletion successful

    def clear_cache(self, name: Optional[str] = None) -> None:
        """
        Clear the in-memory configuration cache.

        Args:
            name: The name of the specific configuration to clear from the cache.
                  If None, clear the entire cache.
        """
        if name is None:
            self.config_cache.clear()
            logger.debug("Cleared all configuration caches")
        elif name in self.config_cache:
            del self.config_cache[name]
            logger.debug(f"Cleared configuration cache: {name}")
