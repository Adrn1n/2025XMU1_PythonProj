"""
Logging utilities for standardized project-wide logging configuration.
Provides centralized setup with module-specific log file routing.
"""

import logging
from typing import Dict, Optional, Union
import sys
from pathlib import Path


def setup_logger(
    name: str,
    log_level: int = logging.INFO,
    log_file: Optional[Union[str, Path]] = None,
    log_to_console: bool = True,
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    date_format: str = "%Y-%m-%d %H:%M:%S",
    propagate: bool = True,
) -> logging.Logger:
    """
    Configure logger instance with handlers and formatting.
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

    return logger


def get_log_level_from_string(level_name: str) -> int:
    """Convert log level name to logging constant."""
    level_map = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL,
    }
    return level_map.get(level_name.lower(), logging.INFO)


def setup_module_logger(
    name: str,
    log_level: int = logging.INFO,
    config_files: Optional[Dict[str, Path]] = None,
    log_to_console: bool = True,
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    date_format: str = "%Y-%m-%d %H:%M:%S",
    propagate: bool = False,
) -> logging.Logger:
    """Setup logger with automatic log file selection based on module name."""
    log_file = None
    if config_files and (name in config_files):
        log_file = config_files[name]

    return setup_logger(
        name=name,
        log_level=log_level,
        log_file=log_file,
        log_to_console=log_to_console,
        log_format=log_format,
        date_format=date_format,
        propagate=propagate,
    )
