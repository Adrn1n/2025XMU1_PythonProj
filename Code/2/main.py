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

# Import optimized configuration
from config import (
    CONFIG,
    HEADERS,
    PROXY_LIST,
    LOG_FILE,
    DEFAULT_CONFIG,
    CACHE_DIR,
)


def parse_args() -> argparse.Namespace:
    """Parse command line arguments"""
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
    return parser.parse_args()


def get_scraper_config(
    args: argparse.Namespace,
    log_to_console: bool,
    log_file_path: Optional[Path],
) -> Dict[str, Any]:
    """
    Generate scraper configuration based on command line arguments

    Args:
        args: Command-line arguments
        log_to_console: Whether to output logs to console
        log_file_path: Path to the log file

    Returns:
        Dictionary of scraper configuration
    """
    # Get scraper config from unified config object, or use default config if not available
    scraper_config = CONFIG.get("scraper", DEFAULT_CONFIG)

    # Build configuration dictionary, prioritizing command line arguments
    config = {
        "max_semaphore": args.concurrent or scraper_config.get("max_semaphore", 25),
        "batch_size": args.batch_size or scraper_config.get("batch_size", 25),
        "timeout": args.timeout or scraper_config.get("timeout", 3),
        "retries": args.retries or scraper_config.get("retries", 0),
        "min_sleep": scraper_config.get("min_sleep", 0.1),
        "max_sleep": scraper_config.get("max_sleep", 0.3),
        "max_redirects": scraper_config.get("max_redirects", 5),
        "cache_size": scraper_config.get("cache_size", 1000),
    }

    # Get log level
    log_level = get_log_level_from_string(args.log_level)

    # Build final configuration
    return {
        "headers": HEADERS,
        "proxies": PROXY_LIST,
        "use_proxy": bool(args.proxy),
        "max_semaphore": config["max_semaphore"],
        "batch_size": config["batch_size"],
        "timeout": config["timeout"],
        "retries": config["retries"],
        "min_sleep": config["min_sleep"],
        "max_sleep": config["max_sleep"],
        "max_redirects": config["max_redirects"],
        "cache_size": config["cache_size"],
        "enable_logging": True,
        "log_level": log_level,
        "log_file": log_file_path,
        "log_to_console": log_to_console,
    }


def get_output_file(args: argparse.Namespace, query: str) -> Path:
    """Determine output file path"""
    # If output argument is provided, use it directly
    if args.output:
        output_path = Path(args.output)
    else:
        # Get cache directory from config, prioritize command line arguments
        cache_dir = args.cache_dir
        if not cache_dir:
            # Use path from config system
            cache_dir = CONFIG["paths"].get("cache_dir", str(CACHE_DIR))

        # Create safe query string and timestamp
        safe_query = "".join(c if c.isalnum() else "_" for c in query)
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        output_path = Path(cache_dir) / f"baidu_search_{safe_query}_{timestamp}.json"

    # Ensure output directory exists
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
    """Execute search and return results"""
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
    """Main function"""
    # Initialization of configuration manager is already done in config.py, use it directly

    try:
        args = parse_args()

        # Interactively prompt user when command line arguments are insufficient
        save_results = not args.no_save_results
        log_to_file = not args.no_log_file
        log_to_console = not args.no_log_console

        if not args.query:
            query = input("Please enter search keyword: ").strip()
            if not query:
                print("Error: No search keyword provided, exiting")
                return None

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
                    args.pages = 1

            if not args.no_save_results and not args.output:
                save_choice = (
                    input("Save search results to file? (y/[n]): ").strip().lower()
                )
                save_results = save_choice == "y"
            if not args.no_log_file and not args.log_file:
                log_file_choice = input("Write logs to file? (y/[n]): ").strip().lower()
                log_to_file = log_file_choice == "y"
            if not args.no_log_console:
                log_console_choice = (
                    input("Display logs in console? (y/[n]): ").strip().lower()
                )
                log_to_console = log_console_choice == "y"
        else:
            query = args.query

        # Set up main logger, ensure configuration is done before any operations
        log_file_path = None
        if log_to_file:
            if args.log_file:
                log_file_path = Path(args.log_file)
            else:
                # Use log file path from config system
                log_file_path = Path(CONFIG["files"].get("log_file", str(LOG_FILE)))

            # Ensure log directory exists
            log_file_path.parent.mkdir(parents=True, exist_ok=True)

        logger = setup_logger(
            "baidu_scraper_main",
            get_log_level_from_string(args.log_level),
            log_file_path,
            log_to_console,
        )

        # Determine cache file path
        if args.cache_file:
            cache_file = Path(args.cache_file)
        else:
            # Use cache directory from config system
            cache_dir = Path(
                args.cache_dir or CONFIG["paths"].get("cache_dir", str(CACHE_DIR))
            )
            cache_dir.mkdir(parents=True, exist_ok=True)
            cache_file = cache_dir / "url_cache.json"

        logger.info("Baidu Search Results Scraper started")

        # Determine output file path
        output_file = None
        if save_results:
            output_file = get_output_file(args, query)
            logger.info(f"Search results will be saved to: {output_file}")

        # Create and configure scraper
        scraper_config = get_scraper_config(args, log_to_console, log_file_path)
        scraper = BaiduScraper(**scraper_config)

        # Clear cache (if needed)
        if args.clear_cache:
            logger.info("[MAIN]: Clearing URL cache")
            scraper.url_cache.clear()

        # Execute search
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
            # Fallback - attempt to use cached results
            if not args.no_cache and cache_file.exists():
                logger.warning(
                    "[MAIN]: Attempting to load partial results from cache..."
                )
                # Add logic to recover from cache if needed
            results = []
        except Exception as e:
            logger.error(
                f"[MAIN]: Unknown error occurred during search: {str(e)}", exc_info=True
            )
            results = []

        # Save search results
        if results and save_results and output_file:
            success = await save_search_results(
                results=results,
                file_path=output_file,
                save_timestamp=True,
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

        # Output statistics
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
            if cache_stats["hits"] + cache_stats["misses"] > 0:
                logger.info(f" - Cache hit rate: {cache_stats['hit_rate']*100:.2f}%")

    except Exception as e:
        print(f"Error occurred during program execution: {str(e)}")
        if logging.getLogger().hasHandlers():
            logging.getLogger().error(
                f"[MAIN]: Error during execution: {str(e)}", exc_info=True
            )
        return 1  # Return error code
    return 0  # Normal exit


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nProgram interrupted by user")
