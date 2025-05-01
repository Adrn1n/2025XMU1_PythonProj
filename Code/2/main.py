import argparse
from typing import Any, Dict, List, Optional
import time
from pathlib import Path
import logging
import aiohttp
import asyncio
import sys

from scrapers.baidu_scraper import BaiduScraper
from utils.logging_utils import get_log_level_from_string, setup_logger
from utils.file_utils import save_search_results

# Import optimized configuration from config.py
from config import (
    CONFIG,
    HEADERS,
    PROXY_LIST,
    LOG_FILE,
    DEFAULT_CONFIG,
    CACHE_DIR,
)


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Baidu Search Results Scraper")
    parser.add_argument(
        "query", nargs="?", help="Search keyword (if not specified, prompt for input)"
    )
    parser.add_argument(
        "-p",
        "--pages",
        type=int,
        default=None,
        help="Number of pages to scrape (default: 1)",
    )
    parser.add_argument(
        "-o", "--output", type=str, help="Output file path (default: auto-generated)"
    )
    parser.add_argument(
        "--no-save-results",
        action="store_true",
        help="Do not save search results to file",
    )
    parser.add_argument(
        "--cache-dir", type=str, default=str(CACHE_DIR), help="Cache directory path"
    )
    parser.add_argument("--cache-file", type=str, help="URL cache file path")
    parser.add_argument("--no-cache", action="store_true", help="Do not use URL cache")
    parser.add_argument(
        "--clear-cache", action="store_true", help="Clear existing cache"
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Log level",
    )
    parser.add_argument("--log-file", type=str, help="Log file path")
    parser.add_argument(
        "--no-log-console", action="store_true", help="Do not display logs in console"
    )
    parser.add_argument(
        "--no-log-file", action="store_true", help="Do not write logs to file"
    )
    parser.add_argument(
        "--concurrent-pages",
        type=int,
        help=f"Concurrent pages scraping limit (default: {DEFAULT_CONFIG.get('max_concurrent_pages', 5)})",
    )
    parser.add_argument(
        "--concurrent",
        type=int,
        help=f"Concurrent requests limit (default: {DEFAULT_CONFIG['max_semaphore']})",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        help=f"Batch processing size (default: {DEFAULT_CONFIG['batch_size']})",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        help=f"Request timeout in seconds (default: {DEFAULT_CONFIG['timeout']})",
    )
    parser.add_argument(
        "--retries",
        type=int,
        help=f"Number of retry attempts for failed requests (default: {DEFAULT_CONFIG['retries']})",
    )
    parser.add_argument(
        "--proxy", action="store_true", help="Use proxy for all requests"
    )
    parser.add_argument(
        "--no-filter-ads",
        action="store_false",
        dest="filter_ads",
        help="Disable advertisement filtering (default: enabled)",
    )
    return parser.parse_args()


def get_scraper_config(
    args: argparse.Namespace,
    log_to_console: bool,
    log_file_path: Optional[Path],
) -> Dict[str, Any]:
    """
    Generate scraper configuration based on command line arguments and global config.

    Args:
        args: Command-line arguments namespace.
        log_to_console: Flag to enable console logging.
        log_file_path: Path object for the log file.

    Returns:
        A dictionary containing the scraper's configuration settings.
    """
    # Load scraper config from the unified CONFIG object, falling back to DEFAULT_CONFIG
    scraper_config = CONFIG.get("scraper", DEFAULT_CONFIG)

    # Build configuration dictionary, prioritizing command line arguments over loaded config
    config = {
        "filter_ads": args.filter_ads,
        "max_concurrent_pages": args.concurrent_pages
        or scraper_config.get("max_concurrent_pages", 5),
        "max_semaphore": args.concurrent or scraper_config.get("max_semaphore", 25),
        "batch_size": args.batch_size or scraper_config.get("batch_size", 25),
        "timeout": args.timeout or scraper_config.get("timeout", 3),
        "retries": args.retries or scraper_config.get("retries", 0),
        "min_sleep": scraper_config.get("min_sleep", 0.1),
        "max_sleep": scraper_config.get("max_sleep", 0.3),
        "max_redirects": scraper_config.get("max_redirects", 5),
        "cache_size": scraper_config.get("cache_size", 1000),
    }

    # Determine the logging level based on the command line argument
    log_level = get_log_level_from_string(args.log_level)

    # Construct the final configuration dictionary for the scraper instance
    return {
        "headers": HEADERS,
        "proxies": PROXY_LIST,
        "filter_ads": config["filter_ads"],
        "use_proxy": bool(args.proxy),  # Explicitly cast to bool
        "max_concurrent_pages": config["max_concurrent_pages"],
        "max_semaphore": config["max_semaphore"],
        "batch_size": config["batch_size"],
        "timeout": config["timeout"],
        "retries": config["retries"],
        "min_sleep": config["min_sleep"],
        "max_sleep": config["max_sleep"],
        "max_redirects": config["max_redirects"],
        "cache_size": config["cache_size"],
        "enable_logging": True,  # Always enable logging for the scraper instance
        "log_level": log_level,
        "log_file": log_file_path,
        "log_to_console": log_to_console,
    }


def get_output_file(args: argparse.Namespace, query: str) -> Path:
    """Determine the output file path for search results."""
    # Use the path specified in the --output argument if provided
    if args.output:
        output_path = Path(args.output)
    else:
        # Otherwise, construct a path based on the cache directory and query
        # Prioritize the command line argument for cache directory
        cache_dir = args.cache_dir
        if not cache_dir:
            # Fallback to the path defined in the configuration system
            cache_dir = CONFIG["paths"].get("cache_dir", str(CACHE_DIR))

        # Sanitize the query string for use in the filename
        safe_query = "".join(c if c.isalnum() else "_" for c in query)
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        output_path = Path(cache_dir) / f"baidu_search_{safe_query}_{timestamp}.json"

    # Ensure the directory for the output file exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    return output_path


async def run_search(
    scraper: BaiduScraper,
    query: str,
    pages: int,
    cache_file: Optional[Path],
    use_cache: bool,
    logger: logging.Logger,
) -> List[Dict[str, Any]]:
    """Execute the search operation using the scraper instance."""
    try:
        logger.info(f"Starting search for '{query}', pages: {pages}")
        results = await scraper.scrape(
            query=query,
            num_pages=pages,
            cache_to_file=use_cache,
            cache_file=cache_file,
        )
        logger.info(f"Search completed, retrieved {len(results)} results")
        return results
    except KeyboardInterrupt:
        logger.warning("Search interrupted by user")
        return []
    except Exception as e:
        logger.error(f"Error occurred during search: {str(e)}")
        return []


async def main():
    """Main execution function for the Baidu scraper."""
    # Configuration manager is initialized in config.py

    try:
        args = parse_args()

        # Determine runtime flags based on arguments or interactive prompts
        save_results = not args.no_save_results
        log_to_file = not args.no_log_file
        log_to_console = not args.no_log_console

        # Prompt user for missing required arguments if not provided via command line
        if not args.query:
            query = input("Please enter search keyword: ").strip()
            if not query:
                print("Error: No search keyword provided, exiting")
                return None  # Indicate error or abnormal exit

            # Prompt for number of pages if not specified
            if args.pages is None:
                pages_str = input("Number of pages to scrape (default: 1): ").strip()
                if pages_str:
                    try:
                        args.pages = int(pages_str)
                        if args.pages <= 0:
                            print("Warning: Invalid page number, using default (1)")
                            args.pages = 1
                    except ValueError:
                        print("Warning: Invalid page number, using default (1)")
                        args.pages = 1
                else:
                    args.pages = 1  # Default to 1 page if input is empty

            # Prompt for saving results if not disabled and no output path given
            if not args.no_save_results and not args.output:
                save_choice = (
                    input("Save search results to file? (y/[n]): ").strip().lower()
                )
                save_results = save_choice == "y"
            # Prompt for logging to file if not disabled and no log file path given
            if not args.no_log_file and not args.log_file:
                log_file_choice = input("Write logs to file? (y/[n]): ").strip().lower()
                log_to_file = log_file_choice == "y"
            # Prompt for console logging if not disabled
            if not args.no_log_console:
                log_console_choice = (
                    input("Display logs in console? (y/[n]): ").strip().lower()
                )
                log_to_console = log_console_choice == "y"
        else:
            query = args.query
            # Ensure pages defaults to 1 if not provided via CLI or prompt
            if args.pages is None:
                args.pages = 1

        # Set up the main logger for the application
        log_file_path = None
        if log_to_file:
            if args.log_file:
                log_file_path = Path(args.log_file)
            else:
                # Use log file path from the configuration system if not specified
                log_file_path = Path(CONFIG["files"].get("log_file", str(LOG_FILE)))

            # Ensure the log directory exists before setting up the logger
            log_file_path.parent.mkdir(parents=True, exist_ok=True)

        logger = setup_logger(
            "baidu_scraper_main",  # Logger name
            get_log_level_from_string(args.log_level),
            log_file_path,
            log_to_console,
        )

        # Determine the path for the URL cache file
        if args.cache_file:
            cache_file = Path(args.cache_file)
        else:
            # Use cache directory from config, prioritizing command line argument
            cache_dir = Path(
                args.cache_dir or CONFIG["paths"].get("cache_dir", str(CACHE_DIR))
            )
            cache_dir.mkdir(parents=True, exist_ok=True)
            cache_file = cache_dir / "url_cache.json"

        logger.info("Baidu Search Results Scraper started")

        # Determine the output file path if results are to be saved
        output_file = None
        if save_results:
            output_file = get_output_file(args, query)
            logger.info(f"Search results will be saved to: {output_file}")

        # Instantiate and configure the BaiduScraper
        scraper_config = get_scraper_config(args, log_to_console, log_file_path)
        scraper = BaiduScraper(**scraper_config)

        # Clear the URL cache if requested via command line argument
        if args.clear_cache:
            logger.info("[MAIN]: Clearing URL cache")
            scraper.url_cache.clear()

        # Execute the search operation
        try:
            results = await run_search(
                scraper=scraper,
                query=query,
                pages=args.pages,
                cache_file=cache_file if not args.no_cache else None,
                use_cache=not args.no_cache,
                logger=logger,
            )
        except aiohttp.ClientError as e:
            logger.error(f"[MAIN]: Network request error: {e}")
            # Attempt to recover partial results from cache if available
            if not args.no_cache and cache_file.exists():
                logger.warning(
                    "[MAIN]: Attempting to load partial results from cache..."
                )
                # TODO: Implement logic to recover results from cache if needed
            results = []
        except Exception as e:
            logger.error(
                f"[MAIN]: Unknown error occurred during search: {str(e)}", exc_info=True
            )
            results = []

        # Save the search results if applicable
        if results and save_results and output_file:
            success = await save_search_results(
                results=results,
                file_path=output_file,
                save_timestamp=True,  # Include timestamp in the saved file
                logger=logger,
            )
            if success:
                logger.info(f"[MAIN]: Search results saved to: {output_file}")
            else:
                logger.error("[MAIN]: Failed to save search results")
        elif results and not save_results:
            logger.info("[MAIN]: Search results not saved as per user settings")
        else:
            logger.warning("[MAIN]: No search results available to save")

        # Output scraper and cache statistics
        stats = scraper.get_stats()
        logger.info("[MAIN]: Scraper statistics:")
        logger.info(f" - Total requests: {stats['total']}")
        logger.info(f" - Successful requests: {stats['success']}")
        logger.info(f" - Failed requests: {stats['failed']}")
        logger.info(f" - Success rate: {stats['success_rate']*100:.2f}%")
        logger.info(f" - Duration: {stats['duration']:.2f} seconds")

        if "cache" in stats:
            cache_stats = stats["cache"]
            logger.info("[MAIN]: Cache statistics:")
            logger.info(
                f" - Cache size: {cache_stats['size']}/{cache_stats['max_size']}"
            )
            logger.info(f" - Cache hits: {cache_stats['hits']}")
            logger.info(f" - Cache misses: {cache_stats['misses']}")
            # Avoid division by zero if no cache operations occurred
            if cache_stats["hits"] + cache_stats["misses"] > 0:
                logger.info(f" - Cache hit rate: {cache_stats['hit_rate']*100:.2f}%")

    except Exception as e:
        # Catch-all for unexpected errors during setup or execution
        print(f"Error occurred during program execution: {str(e)}")
        # Log the error if the logger was successfully initialized
        if logging.getLogger().hasHandlers():
            logging.getLogger().error(
                f"[MAIN]: Error during execution: {str(e)}", exc_info=True
            )
        return 1  # Return a non-zero exit code to indicate an error

    return 0  # Return 0 for successful execution


if __name__ == "__main__":
    # Set the event loop policy for Windows environments
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        # Run the main asynchronous function
        exit_code = asyncio.run(main())
        sys.exit(exit_code)  # Propagate the exit code
    except KeyboardInterrupt:
        print("\nProgram interrupted by user")
        sys.exit(1)  # Indicate interruption
