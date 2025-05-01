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
    propagate: bool = False,  # Default to False to avoid duplicate logs if root logger is configured
) -> logging.Logger:
    """
    Configure and return a logger instance with specified handlers and formatting.

    Avoids adding duplicate handlers if a logger with the same name already exists.

    Args:
        name: The name for the logger.
        log_level: The minimum logging level for this logger (e.g., logging.DEBUG, logging.INFO).
        log_file: Optional path to a file where logs should be written.
        log_to_console: If True, add a handler to output logs to the console (stdout).
        log_format: The format string for log messages.
        date_format: The format string for timestamps in log messages.
        propagate: If True, allow messages to propagate to parent loggers.

    Returns:
        The configured logging.Logger instance.
    """
    # Get the logger instance by name
    logger = logging.getLogger(name)

    # Prevent adding handlers multiple times if the logger is already configured
    if logger.handlers:
        logger.warning(f"Logger '{name}' already has handlers. Skipping setup.")
        return logger

    # Set the logger's level. Handlers can have their own levels >= this level.
    logger.setLevel(log_level)
    # Control propagation to ancestor loggers
    logger.propagate = propagate

    # Create a formatter based on the provided formats
    formatter = logging.Formatter(log_format, date_format)

    # Add console handler if requested
    if log_to_console:
        console_handler = logging.StreamHandler(
            sys.stdout
        )  # Use stdout for console output
        console_handler.setLevel(log_level)  # Set handler level
        console_handler.setFormatter(formatter)  # Apply the formatter
        logger.addHandler(console_handler)

    # Add file handler if a log file path is provided
    if log_file:
        # Ensure log_file is a Path object
        if isinstance(log_file, str):
            log_file = Path(log_file)

        try:
            # Ensure the directory for the log file exists
            log_file.parent.mkdir(parents=True, exist_ok=True)

            # Create a file handler
            file_handler = logging.FileHandler(log_file, encoding="utf-8")
            file_handler.setLevel(log_level)  # Set handler level
            file_handler.setFormatter(formatter)  # Apply the formatter
            logger.addHandler(file_handler)
        except Exception as e:
            # Log an error to the console (if available) if file handler setup fails
            # Avoids crashing the application just because logging to file failed
            logger.error(
                f"Failed to set up log file handler for {log_file}: {e}", exc_info=True
            )
            # If console logging wasn't enabled initially, add a temporary one for this error
            if not log_to_console:
                temp_console_handler = logging.StreamHandler(sys.stderr)
                temp_console_handler.setFormatter(formatter)
                logger.addHandler(temp_console_handler)
                logger.error(
                    f"Failed to set up log file handler for {log_file}: {e}",
                    exc_info=True,
                )
                logger.removeHandler(temp_console_handler)

    return logger


def get_log_levels() -> List[str]:
    """Return a list of standard log level names as strings."""
    return ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


def get_log_level_from_string(level_name: str) -> int:
    """
    Convert a log level name (string) to its corresponding logging constant (integer).
    Defaults to logging.INFO if the name is not recognized. Case-insensitive.

    Args:
        level_name: The string representation of the log level (e.g., "DEBUG", "info").

    Returns:
        The integer value of the log level (e.g., logging.DEBUG, logging.INFO).
    """
    level_map = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL,
    }
    # Use lower case for case-insensitive matching, default to INFO
    return level_map.get(level_name.lower(), logging.INFO)
