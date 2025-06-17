"""
Logging utilities for project-wide standardized logging configuration.
Provides centralized logging setup with module-specific log file routing.
"""

import logging
import sys
from pathlib import Path
from typing import Dict, List, Optional, Union


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
    Get appropriate log file path for a module.

    Args:
        module_name: Module name or class name
        config_files: Dictionary of available log file paths

    Returns:
        Path to appropriate log file or None if not found
    """
    if not config_files:
        return None

    module_mappings = {
        # API modules
        "api": "api_log_file",
        "api.openai": "api_log_file",
        "api.start_server": "api_log_file",
        "api.api_keys": "api_log_file",
        "openai": "api_log_file",
        "start_server": "api_log_file",
        "api_keys": "api_log_file",
        "api_core": "api_log_file",
        "api_services": "api_log_file",
        "api_handlers": "api_log_file",
        "api_utils": "api_log_file",
        # Scraper modules
        "scrapers": "scraper_log_file",
        "baidu_scraper": "scraper_log_file",
        "base_scraper": "scraper_log_file",
        "scraper_test": "scraper_log_file",
        "BaiduScraper": "scraper_log_file",
        "BaseScraper": "scraper_log_file",
        # Ollama modules
        "ollama": "ollama_log_file",
        "ollama_integrate": "ollama_log_file",
        "ollama_utils": "ollama_log_file",
        "OllamaIntegrate": "ollama_log_file",
        "OptimizedOllamaIntegrate": "ollama_log_file",
        # Cache modules
        "cache": "cache_log_file",
        "URLCache": "cache_log_file",
        # Config modules
        "config": "config_log_file",
        "config_manager": "config_log_file",
        "OptimizedConfigManager": "config_log_file",
        # Main application
        "main": "main_log_file",
        "__main__": "main_log_file",
        # Utils modules
        "utils": "utils_log_file",
        "file_utils": "utils_log_file",
        "url_utils": "utils_log_file",
        "logging_utils": "utils_log_file",
    }

    # Try exact match first
    if module_name in module_mappings:
        log_file_key = module_mappings[module_name]
        return config_files.get(log_file_key)

    # Try partial matches
    for key, log_file_key in module_mappings.items():
        if key in module_name:
            return config_files.get(log_file_key)

    return config_files.get("main_log_file") or config_files.get("log_file")


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
        from config import files

        loggers_to_fix = ["OllamaIntegrate", "BaiduScraper", "BaseScraper", "URLCache"]

        for logger_name in loggers_to_fix:
            if logger_name in logging.Logger.manager.loggerDict:
                reset_module_logger(logger_name, files)

    except ImportError:
        pass
