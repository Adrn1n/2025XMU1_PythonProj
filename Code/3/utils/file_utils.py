import aiofiles
import json
import time
import logging
from typing import Any, Dict, List, Optional, Union
from pathlib import Path


async def write_to_file(
    data: Any,
    file_path: Union[str, Path],
    backup: bool = True,
    log_level: int = logging.INFO,
    logger: Optional[logging.Logger] = None,
) -> bool:
    """
    Asynchronously write data to a file. Supports JSON serialization for dicts/lists.
    Optionally creates a timestamped backup of the existing file before writing.

    Args:
        data: The data to write (can be dict, list, or string).
        file_path: The path (string or Path object) to the target file.
        backup: If True and the file exists, rename the existing file with a timestamp before writing.
        log_level: Logging level for the internal logger if none is provided.
        logger: Optional logger instance to use. If None, a default logger is created.

    Returns:
        True if writing was successful, False otherwise.
    """
    # Ensure file_path is a Path object
    if isinstance(file_path, str):
        file_path = Path(file_path)

    # Set up logger if not provided
    if logger is None:
        logger = logging.getLogger("file_utils_write")  # Specific logger name
        logger.setLevel(log_level)
        # Add handler only if none exist to avoid duplicates
        if not logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(
                logging.Formatter(
                    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
                )
            )
            logger.addHandler(handler)

    if not isinstance(file_path, Path):
        logger.error(f"Invalid file path type provided: {type(file_path)}")
        return False

    try:
        # Ensure the parent directory exists
        file_path.parent.mkdir(parents=True, exist_ok=True)

        # Handle backup creation if requested and file exists
        if backup and file_path.exists():
            try:
                # Create a backup filename with a timestamp
                backup_path = file_path.with_suffix(
                    f"{file_path.suffix}.{int(time.time())}.bak"
                )
                file_path.rename(backup_path)
                logger.debug(f"[FILE_UTILS]: Created backup: {backup_path}")
            except OSError as backup_error:
                logger.error(
                    f"[FILE_UTILS]: Failed to create backup for {file_path}: {backup_error}"
                )
                # Decide whether to proceed without backup or return False
                # For now, we'll proceed but log the error.
                # return False # Uncomment this to abort if backup fails

        # Asynchronously open and write to the file
        async with aiofiles.open(file_path, "w", encoding="utf-8") as file:
            # Serialize dicts and lists to JSON with indentation
            if isinstance(data, (dict, list)):
                try:
                    # Use ensure_ascii=False for proper UTF-8 output, indent for readability
                    json_str = json.dumps(data, ensure_ascii=False, indent=2)
                    await file.write(json_str)
                except TypeError as json_error:
                    logger.error(
                        f"Failed to serialize data to JSON for {file_path}: {json_error}"
                    )
                    return False  # Cannot write unserializable data
            else:
                # Write other data types as strings
                await file.write(str(data))

        logger.info(f"Data successfully written to: {file_path}")
        return True

    except PermissionError:
        logger.error(f"Permission denied when trying to write to file: {file_path}")
        return False
    except OSError as e:
        logger.error(f"OS error during file write operation for {file_path}: {str(e)}")
        return False
    except Exception as e:
        # Catch any other unexpected errors
        logger.error(
            f"An unexpected error occurred while writing to {file_path}: {str(e)}",
            exc_info=True,
        )
        return False


async def read_from_file(
    file_path: Union[str, Path],
    default: Any = None,
    log_level: int = logging.INFO,
    logger: Optional[logging.Logger] = None,
) -> Any:
    """
    Asynchronously read data from a file. Attempts to parse as JSON, falls back to text.

    Args:
        file_path: The path (string or Path object) to the file to read.
        default: The value to return if the file doesn't exist or reading/parsing fails.
        log_level: Logging level for the internal logger if none is provided.
        logger: Optional logger instance to use. If None, a default logger is created.

    Returns:
        The parsed data (if JSON) or the raw text content, or the default value on failure.
    """
    # Ensure file_path is a Path object
    if isinstance(file_path, str):
        file_path = Path(file_path)

    # Set up logger if not provided
    if logger is None:
        logger = logging.getLogger("file_utils_read")  # Specific logger name
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
        logger.error(f"Invalid file path type provided: {type(file_path)}")
        return default

    # Check if the file exists before attempting to read
    if not file_path.exists():
        logger.warning(f"File not found: {file_path}")
        return default

    try:
        # Asynchronously open and read the entire file content
        async with aiofiles.open(file_path, "r", encoding="utf-8") as file:
            content = await file.read()

        # Attempt to parse the content as JSON
        try:
            data = json.loads(content)
            logger.debug(f"Successfully read and parsed JSON data from {file_path}")
            return data
        except json.JSONDecodeError:
            # If JSON parsing fails, assume it's plain text and return the raw content
            logger.debug(
                f"Content from {file_path} is not valid JSON, returning as text."
            )
            return content

    except PermissionError:
        logger.error(f"Permission denied when trying to read file: {file_path}")
        return default
    except OSError as e:
        logger.error(f"OS error during file read operation for {file_path}: {str(e)}")
        return default
    except Exception as e:
        # Catch any other unexpected errors
        logger.error(
            f"[FILE_UTILS]: An unexpected error occurred while reading {file_path}: {str(e)}",
            exc_info=True,
        )
        return default


async def save_search_results(
    results: List[Dict[str, Any]],
    file_path: Union[str, Path],
    save_timestamp: bool = True,
    logger: Optional[logging.Logger] = None,
) -> bool:
    """
    Convenience function to save a list of search result dictionaries to a JSON file.
    Optionally includes a timestamp in the saved data.

    Args:
        results: A list where each item is a dictionary representing a search result.
        file_path: The path (string or Path object) to the output JSON file.
        save_timestamp: If True, add a 'timestamp' key with the current time to the saved data structure.
        logger: Optional logger instance to pass to write_to_file.

    Returns:
        True if saving was successful, False otherwise.
    """
    # Structure the data to be saved
    data_to_save: Dict[str, Any] = {
        "results": results,
    }

    # Add timestamp if requested
    if save_timestamp:
        data_to_save["timestamp"] = time.time()

    # Use the generic write_to_file function for saving
    return await write_to_file(data_to_save, file_path, logger=logger)
