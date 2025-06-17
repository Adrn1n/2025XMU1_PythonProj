"""
Logging utilities for project-wide standardized logging configuration.
Provides centralized logging setup with module-specific log file routing.
"""

import logging
from typing import Dict, List, Optional, Union
import sys
from pathlib import Path


def setup_logger(
    name: str,
    log_level: int = logging.INFO,
    log_file: Optional[Union[str, Path]] = None,
    log_to_console: bool = True,
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    date_format: str = "%Y-%m-%d %H:%M:%S",
    propagate: bool = True,  # Changed default to True to allow log propagation
) -> logging.Logger:
    """
    Configure and return a logger instance with handlers and formatting.
    Prevents duplicate handler creation for existing loggers.

    Args:
        name: Logger name identifier
        log_level: Minimum logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional path to log file
        log_to_console: Whether to output logs to console
        log_format: Format string for log messages
        date_format: Format string for timestamps
        propagate: Whether to propagate messages to parent loggers

    Returns:
        Configured Logger instance
    """
    logger = logging.getLogger(name)

    if logger.handlers:
        if logger.name == name:
            logger.debug(f"Logger '{name}' already configured, reusing existing")
        return logger

    logger.setLevel(log_level)
    logger.propagate = propagate

    formatter = logging.Formatter(log_format, date_format)

    if log_to_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(log_level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    if log_file:
        if isinstance(log_file, str):
            log_file = Path(log_file)

        try:
            log_file.parent.mkdir(parents=True, exist_ok=True)
            file_handler = logging.FileHandler(log_file, encoding="utf-8")
            file_handler.setLevel(log_level)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
            logger.info(f"Logging to file: {log_file}")
        except (PermissionError, OSError) as e:
            logger.error(f"Failed to setup file handler for {log_file}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error setting up file handler: {e}")
            if not log_to_console:
                temp_handler = logging.StreamHandler(sys.stderr)
                temp_handler.setFormatter(formatter)
                logger.addHandler(temp_handler)
                logger.error(f"File handler setup failed for {log_file}: {e}")
                logger.removeHandler(temp_handler)

    return logger


def get_log_levels() -> List[str]:
    """Return list of standard log level names."""
    return ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


def get_log_level_from_string(level_name: str) -> int:
    """
    Convert log level name to logging constant.

    Args:
        level_name: Log level name (case-insensitive)

    Returns:
        Logging level constant, defaults to INFO if invalid
    """
    level_map = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL,
    }
    return level_map.get(level_name.lower(), logging.INFO)


def get_module_log_file(
    module_name: str, config_files: Optional[Dict[str, Path]] = None
) -> Optional[Path]:
    """
    Get appropriate log file path for a module using direct mapping.

    This function maps module names to log files using a hierarchical approach:
    1. Direct exact match (e.g., "api" -> "api")
    2. Prefix match (e.g., "api.openai" -> "api")
    3. Suffix-based class mapping (e.g., "BaiduScraper" -> "scrapers")
    4. Fallback to main log file

    Args:
        module_name: Module name or class name
        config_files: Dictionary of log file paths (keys should be module prefixes)

    Returns:
        Path to appropriate log file or None if not found
    """
    if not config_files:
        return None

    # Module prefix mappings - maps module patterns to config_files keys
    module_prefixes = [
        "api",  # Matches api.*, api_*, openai, start_server, etc.
        "scrapers",  # Matches scrapers.*, *scraper*, etc.
        "ollama",  # MatchesG:ollama.*, ollama_*, etc.
        "cache",  # Matches cache.*, *Cache, etc.
        "config",  # Matches config.*, config_*, etc.
        "utils",  # Matches utils.*, *_utils, logging_utils, etc.
        "main",  # Matches main, __main__
    ]

    # Normalize module name for matching
    module_lower = module_name.lower()

    # Direct exact match (highest priority)
    for prefix in module_prefixes:
        if module_lower == prefix:
            return config_files.get(prefix)

    # Prefix-based matching (e.g., "api.openai" -> "api")
    for prefix in module_prefixes:
        if module_lower.startswith(f"{prefix}.") or module_lower.startswith(
            f"{prefix}_"
        ):
            return config_files.get(prefix)

    # Substring-based matching for components
    prefix_patterns = {
        "api": ["api", "openai", "start_server"],
        "scrapers": ["scraper", "baidu"],
        "ollama": ["ollama"],
        "cache": ["cache"],
        "config": ["config"],
        "utils": ["utils", "file_", "url_", "logging_"],
        "main": ["main", "__main__"],
    }

    for prefix, patterns in prefix_patterns.items():
        for pattern in patterns:
            if pattern in module_lower:
                log_file = config_files.get(prefix)
                if log_file:
                    return log_file

    # Fallback to main or default log file
    return config_files.get("main") or config_files.get("log_file")


def setup_module_logger(
    name: str,
    log_level: int = logging.INFO,
    config_files: Optional[Dict[str, Path]] = None,
    log_to_console: bool = True,
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    date_format: str = "%Y-%m-%d %H:%M:%S",
    propagate: bool = False,
) -> logging.Logger:
    """
    Setup logger with automatic log file selection based on module name.

    Args:
        name: Logger name (usually module name)
        log_level: Minimum logging level
        config_files: Dictionary of available log file paths
        log_to_console: Whether to output to console
        log_format: Format string for log messages
        date_format: Format string for timestamps
        propagate: Whether to propagate to parent loggers

    Returns:
        Configured Logger instance
    """
    log_file = get_module_log_file(name, config_files)
    return setup_logger(
        name=name,
        log_level=log_level,
        log_file=log_file,
        log_to_console=log_to_console,
        log_format=log_format,
        date_format=date_format,
        propagate=propagate,
    )


def reset_module_logger(
    name: str, config_files: Optional[Dict[str, Path]] = None
) -> logging.Logger:
    """
    Reset and reconfigure existing logger with correct log file.

    Args:
        name: Logger name to reset
        config_files: Dictionary of available log file paths

    Returns:
        Reconfigured Logger instance
    """
    logger = logging.getLogger(name)

    # Remove existing handlers
    for handler in logger.handlers[:]:
        handler.close()
        logger.removeHandler(handler)

    logger.handlers.clear()
    return setup_module_logger(name, config_files=config_files)


def fix_existing_loggers():
    """Fix existing loggers that may be using incorrect log files."""
    try:
        from config import module_log_files

        loggers_to_fix = ["OllamaIntegrate", "BaiduScraper", "BaseScraper", "URLCache"]

        for logger_name in loggers_to_fix:
            if logger_name in logging.Logger.manager.loggerDict:
                reset_module_logger(logger_name, module_log_files)

    except ImportError:
        pass
