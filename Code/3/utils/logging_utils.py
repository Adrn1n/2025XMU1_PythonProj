"""
Simplified logging utilities with automatic module detection.
No need to pass string parameters - everything is auto-detected.
"""

import inspect
import logging
from typing import Dict, Optional, Union
import sys
from pathlib import Path


# ============================================================================
# Internal helper functions
# ============================================================================

def _get_caller_info():
    """获取调用者信息，内部工具函数"""
    frame = inspect.currentframe()
    try:
        # 回溯两层：_get_caller_info -> 调用的日志函数 -> 实际调用者
        caller_frame = frame.f_back.f_back
        if caller_frame:
            module_name = caller_frame.f_globals.get('__name__', 'unknown')
            func_name = caller_frame.f_code.co_name
            return module_name, func_name
        return 'unknown', 'unknown'
    finally:
        del frame


def _get_auto_module_name():
    """自动获取调用者模块名"""
    frame = inspect.currentframe()
    try:
        caller_frame = frame.f_back.f_back
        if caller_frame:
            return caller_frame.f_globals.get('__name__', 'unknown')
        return 'unknown'
    finally:
        del frame


def _get_module_log_file(module_name: str) -> Optional[Path]:
    """
    根据模块名自动获取对应的日志文件路径
    
    Args:
        module_name: 模块名
    
    Returns:
        对应的日志文件路径或None
    """
    try:
        from config import module_log_files
        config_files = module_log_files
    except ImportError:
        return None
    
    if not config_files:
        return None

    # 模块名规范化
    module_lower = module_name.lower()
    
    # 直接匹配
    module_prefixes = ["api", "scrapers", "ollama", "cache", "config", "utils", "main"]
    
    for prefix in module_prefixes:
        if module_lower == prefix:
            return config_files.get(prefix)
    
    # 前缀匹配
    for prefix in module_prefixes:
        if module_lower.startswith(f"{prefix}.") or module_lower.startswith(f"{prefix}_"):
            return config_files.get(prefix)
    
    # 子串匹配
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
    
    # 回退到主日志文件
    return config_files.get("main") or config_files.get("log_file")


def _setup_logger_internal(
    name: str,
    log_level: int = logging.INFO,
    log_file: Optional[Union[str, Path]] = None,
    log_to_console: bool = True,
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    date_format: str = "%Y-%m-%d %H:%M:%S",
    propagate: bool = True,
) -> logging.Logger:
    """
    内部日志器配置函数
    """
    logger = logging.getLogger(name)

    if logger.handlers:
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
        except Exception as e:
            if log_to_console:
                logger.error(f"Failed to setup file handler for {log_file}: {e}")

    return logger


# ============================================================================
# 全局日志器缓存
# ============================================================================

_logger_cache = {}


# ============================================================================
# 简化的日志函数 - 无需传递任何字符串参数
# ============================================================================

def get_logger(
    log_level: int = logging.INFO,
    log_to_console: bool = True
) -> logging.Logger:
    """
    自动检测调用模块并获取适当的日志器。
    无需传递任何字符串参数，自动从调用栈中检测。
    
    Args:
        log_level: 日志级别，默认INFO
        log_to_console: 是否输出到控制台，默认True
    
    Returns:
        配置好的Logger实例
    
    Usage:
        logger = get_logger()
        logger.info("This is a log message")
    """
    module_name = _get_auto_module_name()
    
    if module_name not in _logger_cache:
        log_file = _get_module_log_file(module_name)
        _logger_cache[module_name] = _setup_logger_internal(
            name=module_name,
            log_level=log_level,
            log_file=log_file,
            log_to_console=log_to_console,
        )
    
    return _logger_cache[module_name]


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
    class_name = cls_instance.__class__.__name__
    
    if class_name not in _logger_cache:
        log_file = _get_module_log_file(class_name)
        _logger_cache[class_name] = _setup_logger_internal(
            name=class_name,
            log_file=log_file,
        )
    
    return _logger_cache[class_name]


def get_current_logger() -> logging.Logger:
    """
    获取当前模块的日志器实例（用于需要直接访问日志器的场合）
    
    Returns:
        当前模块配置好的Logger实例
    
    Usage:
        logger = get_current_logger()
        logger.info("Using logger directly")
    """
    module_name, _ = _get_caller_info()
    if module_name not in _logger_cache:
        _logger_cache[module_name] = get_logger()
    return _logger_cache[module_name]


# ============================================================================
# 直接日志函数 - 最简化的使用方式
# ============================================================================

def log_debug(message: str, *args, **kwargs) -> None:
    """
    直接记录DEBUG级别日志，自动检测模块并使用合适的日志器
    
    Args:
        message: 日志消息
        *args: 格式化参数
        **kwargs: 额外的日志参数
    
    Usage:
        log_debug("Debug message: %s", some_value)
        log_debug("Simple debug message")
    """
    module_name, _ = _get_caller_info()
    if module_name not in _logger_cache:
        _logger_cache[module_name] = get_logger()
    _logger_cache[module_name].debug(message, *args, **kwargs)


def log_info(message: str, *args, **kwargs) -> None:
    """
    直接记录INFO级别日志，自动检测模块并使用合适的日志器
    
    Args:
        message: 日志消息
        *args: 格式化参数
        **kwargs: 额外的日志参数
    
    Usage:
        log_info("Info message: %s", some_value)
        log_info("Simple info message")
    """
    module_name, _ = _get_caller_info()
    if module_name not in _logger_cache:
        _logger_cache[module_name] = get_logger()
    _logger_cache[module_name].info(message, *args, **kwargs)


def log_warning(message: str, *args, **kwargs) -> None:
    """
    直接记录WARNING级别日志，自动检测模块并使用合适的日志器
    
    Args:
        message: 日志消息
        *args: 格式化参数
        **kwargs: 额外的日志参数
    
    Usage:
        log_warning("Warning message: %s", some_value)
        log_warning("Simple warning message")
    """
    module_name, _ = _get_caller_info()
    if module_name not in _logger_cache:
        _logger_cache[module_name] = get_logger()
    _logger_cache[module_name].warning(message, *args, **kwargs)


def log_error(message: str, *args, **kwargs) -> None:
    """
    直接记录ERROR级别日志，自动检测模块并使用合适的日志器
    
    Args:
        message: 日志消息
        *args: 格式化参数
        **kwargs: 额外的日志参数 (支持 exc_info=True)
    
    Usage:
        log_error("Error message: %s", some_value)
        log_error("Error with exception", exc_info=True)
    """
    module_name, _ = _get_caller_info()
    if module_name not in _logger_cache:
        _logger_cache[module_name] = get_logger()
    _logger_cache[module_name].error(message, *args, **kwargs)


def log_critical(message: str, *args, **kwargs) -> None:
    """
    直接记录CRITICAL级别日志，自动检测模块并使用合适的日志器
    
    Args:
        message: 日志消息
        *args: 格式化参数
        **kwargs: 额外的日志参数
    
    Usage:
        log_critical("Critical message: %s", some_value)
        log_critical("Simple critical message")
    """
    module_name, _ = _get_caller_info()
    if module_name not in _logger_cache:
        _logger_cache[module_name] = get_logger()
    _logger_cache[module_name].critical(message, *args, **kwargs)


def log(level: int, message: str, *args, **kwargs) -> None:
    """
    通用日志函数，支持任意日志级别
    
    Args:
        level: 日志级别 (logging.DEBUG, logging.INFO, 等)
        message: 日志消息
        *args: 格式化参数
        **kwargs: 额外的日志参数
    
    Usage:
        log(logging.INFO, "Info message: %s", some_value)
        log(logging.ERROR, "Error occurred", exc_info=True)
    """
    module_name, _ = _get_caller_info()
    if module_name not in _logger_cache:
        _logger_cache[module_name] = get_logger()
    _logger_cache[module_name].log(level, message, *args, **kwargs)


# ============================================================================
# 实用工具函数
# ============================================================================

def clear_logger_cache() -> None:
    """清除日志器缓存（主要用于测试或重新配置）"""
    global _logger_cache
    _logger_cache.clear()


def get_log_levels() -> list:
    """返回可用的日志级别列表"""
    return ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


def get_log_level_from_string(level_name: str) -> int:
    """
    从字符串获取日志级别常量
    
    Args:
        level_name: 日志级别名称（不区分大小写）
    
    Returns:
        日志级别常量，无效时返回INFO级别
    """
    level_map = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL,
    }
    return level_map.get(level_name.lower(), logging.INFO)


# ============================================================================
# 向后兼容的函数（已弃用，建议使用新的简化函数）
# ============================================================================

def setup_logger(*args, **kwargs):
    """
    已弃用：请使用 get_logger() 替代
    保留此函数仅为向后兼容
    """
    import warnings
    warnings.warn(
        "setup_logger() is deprecated. Use get_logger() instead.",
        DeprecationWarning,
        stacklevel=2
    )
    return _setup_logger_internal(*args, **kwargs)


def get_auto_logger(*args, **kwargs):
    """
    已弃用：请使用 get_logger() 替代  
    保留此函数仅为向后兼容
    """
    import warnings
    warnings.warn(
        "get_auto_logger() is deprecated. Use get_logger() instead.",
        DeprecationWarning,
        stacklevel=2
    )
    return get_logger(*args, **kwargs)
