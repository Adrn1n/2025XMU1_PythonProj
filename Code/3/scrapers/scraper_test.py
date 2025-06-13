"""
Scraper testing module for Baidu search functionality.
This module provides a comprehensive interface for testing the BaiduScraper.
"""

import argparse
import asyncio
import logging
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional
import aiohttp

# Add the parent directory to sys.path to allow importing from sibling packages
sys.path.insert(0, str(Path(__file__).parent.parent))

# Import from project modules
from scrapers.baidu_scraper import BaiduScraper
from utils.logging_utils import get_log_level_from_string, setup_logger
from utils.file_utils import save_search_results

# Import configuration
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
    parser = argparse.ArgumentParser(description="Baidu Scraper Testing Tool")

    # Basic arguments
    parser.add_argument(
        "query", nargs="?", help="Search query (if not specified, you'll be prompted)"
    )
    parser.add_argument(
        "-i", "--interactive", action="store_true", help="Run in interactive mode"
    )

    # Search configuration
    search_group = parser.add_argument_group("Search Options")
    search_group.add_argument(
        "-p",
        "--pages",
        type=int,
        default=None,
        help="Number of pages to scrape (default: 1)",
    )
    search_group.add_argument(
        "--no-filter-ads",
        action="store_false",
        dest="filter_ads",
        help="Disable advertisement filtering (default: enabled)",
    )

    # Output/logging configuration
    output_group = parser.add_argument_group("Output Options")
    output_group.add_argument("-o", "--output", help="Output file for search results")
    output_group.add_argument(
        "--save-results",
        action="store_true",
        help="Save search results (default: False)",
    )
    output_group.add_argument(
        "--no-save-results",
        action="store_true",
        help="Do not save search results to file",
    )
    output_group.add_argument("--debug", action="store_true", help="Enable debug mode")
    output_group.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Log level",
    )
    output_group.add_argument("--log-file", help="Log file path")
    output_group.add_argument(
        "--no-log-console", action="store_true", help="Don't log to console"
    )
    output_group.add_argument(
        "--no-log-file", action="store_true", help="Do not write logs to file"
    )

    # Advanced options
    advanced_group = parser.add_argument_group("Advanced Options")
    advanced_group.add_argument(
        "--cache-dir", type=str, default=str(CACHE_DIR), help="Cache directory path"
    )
    advanced_group.add_argument("--cache-file", type=str, help="URL cache file path")
    advanced_group.add_argument(
        "--no-cache", action="store_true", help="Do not use URL cache"
    )
    advanced_group.add_argument(
        "--clear-cache", action="store_true", help="Clear existing cache"
    )
    advanced_group.add_argument(
        "--concurrent-pages",
        type=int,
        help=f"Concurrent pages scraping limit (default: {DEFAULT_CONFIG.get('max_concurrent_pages', 5)})",
    )
    advanced_group.add_argument(
        "--concurrent",
        type=int,
        help=f"Concurrent requests limit (default: {DEFAULT_CONFIG['max_semaphore']})",
    )
    advanced_group.add_argument(
        "--batch-size",
        type=int,
        help=f"Batch processing size (default: {DEFAULT_CONFIG['batch_size']})",
    )
    advanced_group.add_argument(
        "--timeout",
        type=int,
        help=f"Request timeout in seconds (default: {DEFAULT_CONFIG['timeout']})",
    )
    advanced_group.add_argument(
        "--retries",
        type=int,
        help=f"Number of retry attempts for failed requests (default: {DEFAULT_CONFIG['retries']})",
    )
    advanced_group.add_argument(
        "--proxy", action="store_true", help="Use proxy for all requests"
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

        # Use the scrape method
        results = await scraper.scrape(
            query=query,
            pages=pages,
            cache_file=cache_file,
            use_cache=use_cache,
        )

        logger.info(f"Search completed, retrieved {len(results)} results")
        return results
    except KeyboardInterrupt:
        logger.warning("Search interrupted by user")
        return []
    except Exception as search_error:
        logger.error(f"Error occurred during search: {str(search_error)}")
        return []


def show_usage_examples():
    """Show some usage examples for the script."""
    print("\nUsage Examples:")
    print("  1. Interactive mode:")
    print("     python scraper_test.py -i")
    print("  2. Direct query with default settings:")
    print('     python scraper_test.py "your search query"')
    print("  3. Custom pages and output file:")
    print('     python scraper_test.py "your search query" -p 3 -o results.json')
    print("  4. Debug mode with more verbose output:")
    print('     python scraper_test.py "your search query" --debug')
    print("  5. Advanced configuration:")
    print(
        '     python scraper_test.py "your search query" --concurrent-pages 3 --timeout 5 --retries 2'
    )
    print("\nFor full list of options, use: python scraper_test.py --help")
    print()


async def main():
    """Main function to run the Baidu scraper test."""
    try:
        args = parse_args()

        # If no arguments provided, automatically enable interactive mode
        if len(sys.argv) == 1:
            args.interactive = True
            print("No arguments provided, running in interactive mode...")
            print("Type 'python scraper_test.py --help' for command line options.\n")

        # Determine runtime flags based on arguments or interactive prompts
        save_results = (
            not args.no_save_results
            if hasattr(args, "no_save_results")
            else args.save_results
        )
        log_to_file = not args.no_log_file if hasattr(args, "no_log_file") else True
        log_to_console = (
            not args.no_log_console if hasattr(args, "no_log_console") else True
        )

        # Prompt user for missing required arguments if not provided via command line
        query = args.query
        if not query and args.interactive:
            try:
                query = input("Enter search query: ").strip()
                if not query:
                    print("Error: No search query provided.")
                    return 1

                # Prompt for number of pages if not specified
                if args.pages is None:
                    pages_str = input(
                        "Number of pages to scrape (default: 1): "
                    ).strip()
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

                # Prompt for saving results if not already decided
                if not hasattr(args, "no_save_results") and not args.save_results:
                    save_choice = (
                        input("Save search results to file? (y/[n]): ").strip().lower()
                    )
                    save_results = save_choice == "y"

                # Prompt for logging options if interactive
                log_file_choice = input("Write logs to file? (y/[n]): ").strip().lower()
                log_to_file = log_file_choice == "y"

                log_console_choice = (
                    input("Display logs in console? ([y]/n): ").strip().lower()
                )
                log_to_console = log_console_choice != "n"

            except (EOFError, KeyboardInterrupt):
                print("\nOperation cancelled by user.")
                return 1
        elif not args.pages:
            # Default to 1 page if not specified
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
            "baidu_scraper_test",  # Logger name
            get_log_level_from_string(args.log_level),
            log_file_path,
            log_to_console,
        )

        logger.info("Baidu Scraper Test started")

        # Determine the path for the URL cache file
        cache_file = None
        if not args.no_cache:
            if args.cache_file:
                cache_file = Path(args.cache_file)
            else:
                # Use cache directory from config, prioritizing command line argument
                cache_dir = Path(
                    args.cache_dir or CONFIG["paths"].get("cache_dir", str(CACHE_DIR))
                )
                cache_dir.mkdir(parents=True, exist_ok=True)
                cache_file = cache_dir / "url_cache.json"

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
            logger.info("Clearing URL cache")
            scraper.url_cache.clear()

        # Execute the search operation
        try:
            results = await run_search(
                scraper=scraper,
                query=query,
                pages=args.pages,
                cache_file=cache_file,
                use_cache=not args.no_cache,
                logger=logger,
            )
        except aiohttp.ClientError as client_error:
            logger.error(f"Network request error: {client_error}")
            # Attempt to recover partial results from cache if available
            if not args.no_cache and cache_file and cache_file.exists():
                logger.warning("Attempting to load partial results from cache...")
                # This would be implemented if needed
            results = []
        except Exception as request_error:
            logger.error(
                f"Unknown error occurred during search: {str(request_error)}",
                exc_info=True,
            )
            results = []

        if not results:
            logger.warning("No search results found")
            print("Warning: No search results found.")
            return 1

        logger.info(f"Found {len(results)} search results")
        print(f"Found {len(results)} search results.")

        # Print the first few results
        print("\nFirst 3 results:")
        for i, result in enumerate(results[:3], 1):
            print(f"\n--- Result {i} ---")
            print(f"Title: {result.get('title', 'N/A')}")
            print(f"URL: {result.get('url', 'N/A')}")

            content = result.get("content", "")
            if len(content) > 100:
                content = content[:100] + "..."
            print(f"Content: {content}")

        # Save the search results if applicable
        if save_results and output_file:
            success = await save_search_results(
                results=results,
                file_path=output_file,
                save_timestamp=True,  # Include timestamp in the saved file
                logger=logger,
            )
            if success:
                logger.info(f"Search results saved to: {output_file}")
                print(f"Search results saved to: {output_file}")
            else:
                logger.error("Failed to save search results")
                print("Error: Failed to save search results.")

        # Output scraper and cache statistics
        stats = scraper.get_stats()
        logger.info("Scraper statistics:")
        logger.info(f" - Total requests: {stats.get('total', 0)}")
        logger.info(f" - Successful requests: {stats.get('success', 0)}")
        logger.info(f" - Failed requests: {stats.get('failed', 0)}")

        success_rate = 0
        if stats.get("total", 0) > 0:
            success_rate = stats.get("success", 0) / stats.get("total", 1) * 100
        logger.info(f" - Success rate: {success_rate:.2f}%")

        if "duration" in stats:
            logger.info(f" - Duration: {stats.get('duration', 0):.2f} seconds")

        if "cache" in stats:
            cache_stats = stats["cache"]
            logger.info("Cache statistics:")
            logger.info(
                f" - Cache size: {cache_stats.get('size', 0)}/{cache_stats.get('max_size', 0)}"
            )
            logger.info(f" - Cache hits: {cache_stats.get('hits', 0)}")
            logger.info(f" - Cache misses: {cache_stats.get('misses', 0)}")

            # Avoid division by zero if no cache operations occurred
            total_ops = cache_stats.get("hits", 0) + cache_stats.get("misses", 0)
            if total_ops > 0:
                hit_rate = cache_stats.get("hits", 0) / total_ops * 100
                logger.info(f" - Cache hit rate: {hit_rate:.2f}%")

        # Print stats if debug is enabled
        if args.debug:
            print("\nScraper Statistics:")
            print(f"Total requests: {stats.get('total', 0)}")
            print(f"Successful requests: {stats.get('success', 0)}")
            print(f"Failed requests: {stats.get('failed', 0)}")
            print(f"Success rate: {success_rate:.2f}%")
            print(f"Duration: {stats.get('duration', 0):.2f} seconds")

            if "cache" in stats:
                cache_stats = stats["cache"]
                print("\nCache Statistics:")
                print(
                    f"Cache size: {cache_stats.get('size', 0)}/{cache_stats.get('max_size', 0)}"
                )
                print(f"Cache hits: {cache_stats.get('hits', 0)}")
                print(f"Cache misses: {cache_stats.get('misses', 0)}")
                # Calculate total operations and hit rate for debug output
                debug_total_ops = cache_stats.get("hits", 0) + cache_stats.get(
                    "misses", 0
                )
                if debug_total_ops > 0:
                    debug_hit_rate = cache_stats.get("hits", 0) / debug_total_ops * 100
                    print(f"Cache hit rate: {debug_hit_rate:.2f}%")

        logger.info("Baidu scraper test completed successfully")
        print("\nTest completed successfully!")
        return 0

    except Exception as main_error:
        # Catch-all for unexpected errors during setup or execution
        print(f"Error occurred during program execution: {str(main_error)}")
        # Log the error if the logger was successfully initialized
        if logging.getLogger().hasHandlers():
            logging.getLogger().error(
                f"Error during execution: {str(main_error)}", exc_info=True
            )
        return 1  # Return a non-zero exit code to indicate an error


if __name__ == "__main__":
    # Set event loop policy for Windows
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\nOperation interrupted by user.")
        sys.exit(1)
    except Exception as final_error:
        print(f"Error: {str(final_error)}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
