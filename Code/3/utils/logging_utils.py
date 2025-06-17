import logging
from typing import List, Optional, Union, Dict
from pathlib import Path
import sys


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
        # Only warn if this is the same logger being reconfigured
        if logger.name == name:
            logger.debug(
                f"Logger '{name}' already has handlers. Reusing existing logger."
            )
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
            logger.info(f"Logging to file: {log_file}")
        except (PermissionError, OSError) as e:
            logger.error(
                f"Failed to set up log file handler for {log_file}: {e}", exc_info=True
            )
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


def get_module_log_file(
    module_name: str, config_files: Optional[Dict[str, Path]] = None
) -> Optional[Path]:
    """
    Get the appropriate log file path for a given module name.

    Args:
        module_name: The name of the module (e.g., 'scrapers.baidu_scraper', 'ollama.ollama_integrate')
        config_files: Dictionary of available log file paths from configuration

    Returns:
        Path to the appropriate log file, or None if no specific mapping found
    """
    if not config_files:
        return None

    # Module to log file mapping
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
        # Utils modules (fallback for other utils)
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

    # Default fallback - use main log file instead of scraper log file
    return config_files.get("main_log_file") or config_files.get("log_file")


def setup_module_logger(
    name: str,
    log_level: int = logging.INFO,
    config_files: Optional[Dict[str, Path]] = None,
    log_to_console: bool = True,
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    date_format: str = "%Y-%m-%d %H:%M:%S",
    propagate: bool = False,  # Changed to False to prevent cross-contamination
) -> logging.Logger:
    """
    Configure and return a logger instance with automatic log file selection based on module name.

    Args:
        name: The name for the logger (usually the module name).
        log_level: The minimum logging level for this logger.
        config_files: Dictionary of available log file paths from configuration.
        log_to_console: If True, add a handler to output logs to the console.
        log_format: The format string for log messages.
        date_format: The format string for timestamps in log messages.
        propagate: If True, allow messages to propagate to parent loggers.

    Returns:
        The configured logging.Logger instance.
    """
    # Determine the appropriate log file for this module
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
    Reset and reconfigure an existing logger with correct module-specific log file.

    Args:
        name: The name of the logger to reset.
        config_files: Dictionary of available log file paths from configuration.

    Returns:
        The reconfigured logging.Logger instance.
    """
    # Get the existing logger
    logger = logging.getLogger(name)

    # Remove all existing handlers
    for handler in logger.handlers[:]:
        handler.close()
        logger.removeHandler(handler)

    # Clear any cached handlers
    logger.handlers.clear()

    # Reconfigure with correct log file
    return setup_module_logger(name, config_files=config_files)


def fix_existing_loggers():
    """Fix any existing loggers that might be using wrong log files."""
    try:
        from config import files

        # List of logger names that might need fixing
        loggers_to_fix = ["OllamaIntegrate", "BaiduScraper", "BaseScraper", "URLCache"]

        for logger_name in loggers_to_fix:
            if logger_name in logging.Logger.manager.loggerDict:
                reset_module_logger(logger_name, files)

    except ImportError:
        pass  # Config not available
