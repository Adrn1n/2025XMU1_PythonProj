import logging
from typing import Optional, Union, List
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
    配置并返回日志记录器

    Args:
        name: 日志记录器名称
        log_level: 日志级别
        log_file: 日志文件路径
        log_to_console: 是否输出到控制台
        log_format: 日志格式
        date_format: 日期格式
        propagate: 是否传播日志到父记录器

    Returns:
        配置好的日志记录器
    """
    # 获取logger
    logger = logging.getLogger(name)

    # 如果已经有handlers，则返回现有logger（避免重复添加handlers）
    if logger.handlers:
        return logger

    logger.setLevel(log_level)
    logger.propagate = propagate

    formatter = logging.Formatter(log_format, date_format)

    # 添加控制台处理器
    if log_to_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(log_level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    # 添加文件处理器
    if log_file:
        if isinstance(log_file, str):
            log_file = Path(log_file)

        try:
            # 确保日志目录存在
            log_file.parent.mkdir(parents=True, exist_ok=True)

            file_handler = logging.FileHandler(log_file, encoding="utf-8")
            file_handler.setLevel(log_level)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        except Exception as e:
            # 如果无法设置文件处理器，则记录错误到控制台
            if log_to_console:
                logger.error(f"无法设置日志文件 {log_file}: {e}")

    return logger


def get_log_levels() -> List[str]:
    """返回所有可用的日志级别名称"""
    return ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


def get_log_level_from_string(level_name: str) -> int:
    """
    将日志级别名称转换为对应的整数值, 默认为INFO级别

    Args:
        level_name: 日志级别名称

    Returns:
        日志级别整数值
    """
    level_map = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL,
    }

    return level_map.get(level_name.lower(), logging.INFO)
