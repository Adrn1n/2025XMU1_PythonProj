import logging
from typing import List, Optional, Union
from pathlib import Path
import sys


def setup_logger(
    name: str,
    log_level: int = logging.INFO,
    log_file: Optional[Union[str, Path]] = None,
    log_to_console: bool = True,
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    date_format: str = "%Y-%m-%d %H:%M:%S",
    propagate: bool = False,
) -> logging.Logger:
    """
    Configure and return a logger

    Args:
        name: Logger name
        log_level: Log level
        log_file: Log file path
        log_to_console: Whether to output to console
        log_format: Log format
        date_format: Date format
        propagate: Whether to propagate logs to parent logger

    Returns:
        Configured logger
    """
    # Get logger
    logger = logging.getLogger(name)

    # If handlers already exist, return existing logger (avoid duplicate handlers)
    if logger.handlers:
        return logger

    logger.setLevel(log_level)
    logger.propagate = propagate

    formatter = logging.Formatter(log_format, date_format)

    # Add console handler
    if log_to_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(log_level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    # Add file handler
    if log_file:
        if isinstance(log_file, str):
            log_file = Path(log_file)

        try:
            # Ensure log directory exists
            log_file.parent.mkdir(parents=True, exist_ok=True)

            file_handler = logging.FileHandler(log_file, encoding="utf-8")
            file_handler.setLevel(log_level)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        except Exception as e:
            # If unable to set up file handler, log error to console
            if log_to_console:
                logger.error(f"Unable to set up log file {log_file}: {e}")

    return logger


def get_log_levels() -> List[str]:
    """Return all available log level names"""
    return ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


def get_log_level_from_string(level_name: str) -> int:
    """
    Convert log level name to corresponding integer value, default to INFO level

    Args:
        level_name: Log level name

    Returns:
        Log level integer value
    """
    level_map = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL,
    }

    return level_map.get(level_name.lower(), logging.INFO)
