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
    Asynchronously write data to a file, with backup support

    Args:
        data: Data to write
        file_path: File path
        backup: Whether to create backup
        log_level: Log level
        logger: Logger object

    Returns:
        bool: Whether writing was successful
    """
    if isinstance(file_path, str):
        file_path = Path(file_path)

    # Create or get logger
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
        logger.error("Invalid file path")
        return False

    try:
        # Ensure directory exists
        file_path.parent.mkdir(parents=True, exist_ok=True)

        # Create backup if file exists and backup is needed
        if backup and file_path.exists():
            backup_path = file_path.with_suffix(f".{int(time.time())}.bak")
            file_path.rename(backup_path)
            logger.debug(f"[FILE_UTILS]: Backup created: {backup_path}")

        # Write data
        async with aiofiles.open(file_path, "w", encoding="utf-8") as file:
            if isinstance(data, (dict, list)):
                json_str = json.dumps(data, ensure_ascii=False, indent=2)
                await file.write(json_str)
            else:
                await file.write(str(data))

        logger.info(f"Data saved to: {file_path}")
        return True

    except json.JSONEncodeError as e:
        logger.error(f"Data serialization failed: {str(e)}")
        return False
    except PermissionError:
        logger.error(f"No permission to write file: {file_path}")
        return False
    except OSError as e:
        logger.error(f"File operation failed: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Unknown error: {str(e)}")
        return False


async def read_from_file(
    file_path: Union[str, Path],
    default: Any = None,
    log_level: int = logging.INFO,
    logger: Optional[logging.Logger] = None,
) -> Any:
    """
    Asynchronously read data from file

    Args:
        file_path: File path
        default: Default value if file doesn't exist or reading fails
        log_level: Log level
        logger: Logger object

    Returns:
        Read data or default value if reading fails
    """
    if isinstance(file_path, str):
        file_path = Path(file_path)

    # Create or get logger
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
        logger.error("Invalid file path")
        return default

    try:
        if not file_path.exists():
            logger.warning(f"File does not exist: {file_path}")
            return default

        async with aiofiles.open(file_path, "r", encoding="utf-8") as file:
            content = await file.read()

        # Try to parse as JSON
        try:
            data = json.loads(content)
            logger.debug(f"JSON data read from {file_path}")
            return data
        except json.JSONDecodeError:
            # Not JSON, return original content
            logger.debug(f"Text data read from {file_path}")
            return content

    except PermissionError:
        logger.error(f"No permission to read file: {file_path}")
        return default
    except OSError as e:
        logger.error(f"File operation failed: {str(e)}")
        return default
    except Exception as e:
        logger.error(f"[FILE_UTILS]: Unknown error: {str(e)}")
        return default


async def save_search_results(
    results: List[Dict[str, Any]],
    file_path: Union[str, Path],
    save_timestamp: bool = True,
    logger: Optional[logging.Logger] = None,
) -> bool:
    """
    Save search results to file

    Args:
        results: List of search results
        file_path: File path
        save_timestamp: Whether to save timestamp
        logger: Logger object

    Returns:
        Whether saving was successful
    """
    data_to_save = {
        "results": results,
    }

    if save_timestamp:
        data_to_save: Dict[str, any] = {"results": results, "timestamp": time.time()}

    return await write_to_file(data_to_save, file_path, logger=logger)
