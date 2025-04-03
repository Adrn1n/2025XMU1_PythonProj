import aiofiles
from typing import Any, Dict, List, Optional, Union
from pathlib import Path
import logging
import time
import json


async def write_to_file(
    data: Any,
    file_path: Union[str, Path],
    backup: bool = True,
    log_level: int = logging.INFO,
    logger: Optional[logging.Logger] = None,
) -> bool:
    """
    异步将数据写入文件，并支持备份

    Args:
        data: 要写入的数据
        file_path: 文件路径
        backup: 是否创建备份
        log_level: 日志级别
        logger: 日志记录器

    Returns:
        bool: 写入是否成功
    """
    if isinstance(file_path, str):
        file_path = Path(file_path)

    # 创建或获取日志记录器
    if logger is None:
        logger = logging.getLogger("file_utils: write_to_file")
        logger.setLevel(log_level)
        if not logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(
                logging.Formatter(
                    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
                )
            )
            logger.addHandler(handler)

    if not isinstance(file_path, Path):
        logger.error("文件路径无效")
        return False

    try:
        # 确保目录存在
        file_path.parent.mkdir(parents=True, exist_ok=True)

        # 如果文件已存在且需要备份，则创建备份
        if backup and file_path.exists():
            backup_path = file_path.with_suffix(f".{int(time.time())}.bak")
            file_path.rename(backup_path)
            logger.debug(f"已创建备份: {backup_path}")

        # 写入数据
        async with aiofiles.open(file_path, "w", encoding="utf-8") as file:
            if isinstance(data, (dict, list)):
                json_str = json.dumps(data, ensure_ascii=False, indent=2)
                await file.write(json_str)
            else:
                await file.write(str(data))

        logger.info(f"数据已保存至: {file_path}")
        return True

    except json.JSONEncodeError as e:
        logger.error(f"数据序列化失败: {str(e)}")
        return False
    except PermissionError:
        logger.error(f"无权限写入文件: {file_path}")
        return False
    except OSError as e:
        logger.error(f"文件操作失败: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"未知错误: {str(e)}")
        return False


async def read_from_file(
    file_path: Union[str, Path],
    default: Any = None,
    log_level: int = logging.INFO,
    logger: Optional[logging.Logger] = None,
) -> Any:
    """
    异步从文件读取数据

    Args:
        file_path: 文件路径
        default: 文件不存在或读取失败时的默认值
        log_level: 日志级别
        logger: 日志记录器

    Returns:
        读取的数据，如果读取失败则返回默认值
    """
    if isinstance(file_path, str):
        file_path = Path(file_path)

    # 创建或获取日志记录器
    if logger is None:
        logger = logging.getLogger("file_utils: read_from_file")
        logger.setLevel(log_level)
        if not logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(
                logging.Formatter(
                    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
                )
            )
            logger.addHandler(handler)

    if not isinstance(file_path, Path):
        logger.error("文件路径无效")
        return default

    try:
        if not file_path.exists():
            logger.warning(f"文件不存在: {file_path}")
            return default

        async with aiofiles.open(file_path, "r", encoding="utf-8") as file:
            content = await file.read()

        # 尝试解析为JSON
        try:
            data = json.loads(content)
            logger.debug(f"已从 {file_path} 读取JSON数据")
            return data
        except json.JSONDecodeError:
            # 不是JSON，返回原始内容
            logger.debug(f"已从 {file_path} 读取文本数据")
            return content

    except PermissionError:
        logger.error(f"无权限读取文件: {file_path}")
        return default
    except OSError as e:
        logger.error(f"文件操作失败: {str(e)}")
        return default
    except Exception as e:
        logger.error(f"未知错误: {str(e)}")
        return default


async def save_search_results(
    results: List[Dict[str, Any]],
    file_path: Union[str, Path],
    save_timestamp: bool = True,
    logger: Optional[logging.Logger] = None,
) -> bool:
    """
    保存搜索结果到文件

    Args:
        results: 搜索结果列表
        file_path: 文件路径
        save_timestamp: 是否保存时间戳
        logger: 日志记录器

    Returns:
        保存是否成功
    """
    data_to_save = {
        "results": results,
    }

    if save_timestamp:
        data_to_save["timestamp"] = time.time()
        data_to_save["formatted_time"] = time.strftime("%Y-%m-%d %H:%M:%S")

    return await write_to_file(data_to_save, file_path, logger=logger)
