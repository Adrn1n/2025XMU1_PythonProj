"""
Main application module for Baidu-Ollama integration.
Provides an interactive command-line interface for using Ollama LLMs with Baidu search results.
"""

import argparse
import asyncio
import logging
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

# Import from project modules
from scrapers.baidu_scraper import BaiduScraper
from utils.logging_utils import get_log_level_from_string, setup_logger
from utils.file_utils import save_search_results
from utils.ollama_utils import (
    list_ollama_models,
    interactive_model_selection,
    generate_with_ollama,
    format_search_results_for_ollama,
    create_system_prompt,
    create_full_prompt,
    check_ollama_status,
    get_model_info,
    get_recommended_parameters,
)

# Import configuration
from config import (
    CONFIG,
    HEADERS,
    PROXY_LIST,
    LOG_FILE,
    DEFAULT_CONFIG,
    CACHE_DIR,
    OLLAMA_CONFIG,
    SEARCH_CACHE_FILE,
)


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Baidu Search + Ollama LLM Integration"
    )

    # Basic arguments
    parser.add_argument(
        "query",
        nargs="?",
        help="Search query or question (if not specified, you'll be prompted)",
    )
    parser.add_argument(
        "-i", "--interactive", action="store_true", help="Run in interactive mode"
    )
    parser.add_argument(
        "-m",
        "--model",
        help="Ollama model to use (if not specified, you'll be prompted to select)",
    )

    # Search configuration
    search_group = parser.add_argument_group("Search Options")
    search_group.add_argument(
        "-p",
        "--pages",
        type=int,
        default=1,
        help="Number of pages to scrape (default: 1)",
    )
    search_group.add_argument(
        "--no-filter-ads",
        action="store_false",
        dest="filter_ads",
        help="Disable advertisement filtering (default: enabled)",
    )

    # Ollama configuration
    ollama_group = parser.add_argument_group("Ollama Options")
    ollama_group.add_argument(
        "--ollama-url",
        default=OLLAMA_CONFIG.get("base_url", "http://localhost:11434"),
        help="Ollama API base URL",
    )
    ollama_group.add_argument(
        "--temperature",
        type=float,
        help="Ollama temperature (uses model-specific defaults if not specified)",
    )
    ollama_group.add_argument(
        "--top-p",
        type=float,
        help="Ollama top-p parameter (uses model-specific defaults if not specified)",
    )
    ollama_group.add_argument(
        "--top-k",
        type=int,
        help="Ollama top-k parameter (uses model-specific defaults if not specified)",
    )
    ollama_group.add_argument(
        "--context-size", type=int, help="Context size (window) for the model"
    )
    ollama_group.add_argument(
        "--max-tokens", type=int, help="Maximum tokens to generate"
    )
    ollama_group.add_argument(
        "--no-stream",
        dest="stream",
        action="store_false",
        default=OLLAMA_CONFIG.get("stream", True),
        help="Disable streaming response (wait for complete response)",
    )
    ollama_group.add_argument(
        "--question",
        help="Specific question to ask the LLM (if different from search query)",
    )

    # Output/logging configuration
    output_group = parser.add_argument_group("Output Options")
    output_group.add_argument("-o", "--output", help="Output file for search results")
    output_group.add_argument(
        "--save-results",
        action="store_true",
        help="Save search results to file (always enabled in debug mode unless --no-save-results is used)",
    )
    output_group.add_argument(
        "--no-save-results",
        action="store_true",
        help="Explicitly disable saving search results (overrides debug mode default)",
    )
    output_group.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug mode (shows detailed logging and automatically saves results unless --no-save-results is specified)",
    )
    output_group.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Log level",
    )
    output_group.add_argument("--log-file", help="Log file path")
    output_group.add_argument(
        "--no-log-console",
        action="store_false",
        dest="log_console",
        default=True,
        help="Don't log to console",
    )

    # Advanced options
    advanced_group = parser.add_argument_group("Advanced Options")
    advanced_group.add_argument(
        "--cache-dir", type=str, default=str(CACHE_DIR), help="Cache directory path"
    )
    advanced_group.add_argument(
        "--no-cache", action="store_true", help="Do not use URL cache"
    )
    advanced_group.add_argument(
        "--clear-cache", action="store_true", help="Clear existing cache"
    )
    advanced_group.add_argument(
        "--concurrent-pages",
        type=int,
        default=DEFAULT_CONFIG.get("max_concurrent_pages", 5),
        help="Concurrent pages scraping limit",
    )
    advanced_group.add_argument(
        "--concurrent",
        type=int,
        default=DEFAULT_CONFIG.get("max_semaphore", 25),
        help="Concurrent requests limit",
    )
    advanced_group.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_CONFIG.get("batch_size", 25),
        help="Batch size for requests",
    )
    advanced_group.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_CONFIG.get("timeout", 3),
        help="Request timeout in seconds",
    )
    advanced_group.add_argument(
        "--proxy", action="store_true", help="Use proxy for all requests"
    )

    return parser.parse_args()


def setup_logging(args: argparse.Namespace) -> logging.Logger:
    """
    Set up logging based on command line arguments.

    Args:
        args: Parsed command line arguments.

    Returns:
        Configured logger instance.
    """
    # If debug mode is enabled, set log level to DEBUG
    if args.debug:
        log_level = logging.DEBUG
        print("Debug mode enabled - setting log level to DEBUG")
    else:
        log_level = get_log_level_from_string(args.log_level)

    # Determine log file path
    log_file_path = None
    if args.log_file:
        log_file_path = Path(args.log_file)
    else:
        log_file_path = Path(LOG_FILE)

    # Ensure log directory exists
    log_file_path.parent.mkdir(parents=True, exist_ok=True)

    # Create logger with the correct name
    logger = setup_logger(
        "OllamaIntegrate",  # Changed from BaiduOllama or other name
        log_level,
        log_file_path if not args.debug else log_file_path,
        args.log_console,
    )

    logger.info("Logger initialized")
    return logger


def setup_scraper(args: argparse.Namespace, logger: logging.Logger) -> BaiduScraper:
    """
    Set up the Baidu scraper with appropriate configuration.

    Args:
        args: Parsed command line arguments.
        logger: Logger instance.

    Returns:
        Configured BaiduScraper instance.
    """
    # Create scraper with parameters from args
    # Pass the same log_file and log settings as the main logger to ensure logs go to the same file
    scraper = BaiduScraper(
        headers=HEADERS,
        proxies=PROXY_LIST,
        filter_ads=args.filter_ads,
        use_proxy=args.proxy,
        max_concurrent_pages=args.concurrent_pages,
        max_semaphore=args.concurrent,
        batch_size=args.batch_size,
        timeout=args.timeout,
        enable_logging=True,
        log_to_console=args.log_console,
        log_level=get_log_level_from_string(args.log_level),
        log_file=(
            args.log_file if not args.debug else Path(LOG_FILE)
        ),  # Ensure log_file is set
    )

    # Clear cache if requested
    if args.clear_cache:
        logger.info("Clearing URL cache")
        scraper.url_cache.clear()

    return scraper


async def run_search(
    scraper: BaiduScraper,
    query: str,
    pages: int,
    use_cache: bool,
    logger: logging.Logger,
    debug: bool = False,  # Add debug parameter for detailed logging
) -> List[Dict[str, Any]]:
    """
    Run the search operation using the scraper.

    Args:
        scraper: BaiduScraper instance.
        query: Search query.
        pages: Number of pages to scrape.
        use_cache: Whether to use cache.
        logger: Logger instance.
        debug: Whether debug mode is enabled.

    Returns:
        List of search result dictionaries.
    """
    try:
        logger.info(f"Starting search for '{query}', pages: {pages}")
        logger.debug(
            f"Search cache settings: use_cache={use_cache}, debug={debug}, cache_to_file={use_cache and not debug}"
        )

        # Use the SEARCH_CACHE_FILE for cache
        cache_file_path = Path(SEARCH_CACHE_FILE) if SEARCH_CACHE_FILE else None
        if debug:
            logger.debug(
                f"Debug mode enabled, cache_file_path will be ignored: {cache_file_path}"
            )

        results = await scraper.scrape(
            query=query,
            num_pages=pages,
            cache_to_file=use_cache and not debug,  # Don't cache to file in debug mode
            cache_file=(
                cache_file_path if not debug else None
            ),  # Don't use cache file in debug mode
        )
        logger.info(f"Search completed, retrieved {len(results)} results")
        return results
    except KeyboardInterrupt:
        logger.warning("Search interrupted by user")
        return []
    except Exception as e:
        logger.error(f"Error occurred during search: {str(e)}")
        return []


async def save_results_if_needed(
    search_results: List[Dict[str, Any]],
    save_results: bool,
    output_file: Optional[str],
    query: str,
    logger: logging.Logger,
) -> None:
    """
    Save search results to file if configured to do so.

    Args:
        search_results: List of search result dictionaries.
        save_results: Whether to save results.
        output_file: Output file path.
        query: Search query (used in auto-generated filename).
        logger: Logger instance.
    """
    if not search_results or not save_results:
        return

    # Determine output file path
    if output_file:
        output_path = Path(output_file)
    else:
        # Use default output path
        import time

        cache_dir = Path(CACHE_DIR)
        # Sanitize the query string for use in the filename
        safe_query = "".join(c if c.isalnum() else "_" for c in query)
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        output_path = cache_dir / f"baidu_search_{safe_query}_{timestamp}.json"

    # Ensure the directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Save results
    success = await save_search_results(
        results=search_results,
        file_path=output_path,
        save_timestamp=True,
        logger=logger,
    )

    if success:
        logger.info(f"Search results saved to: {output_path}")
        print(f"Search results saved to: {output_path}")
    else:
        logger.error("Failed to save search results")


def print_stats(scraper: BaiduScraper, logger: logging.Logger) -> None:
    """
    Print scraper statistics.

    Args:
        scraper: BaiduScraper instance.
        logger: Logger instance.
    """
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


async def setup_ollama_model(
    args: argparse.Namespace, logger: logging.Logger
) -> Optional[str]:
    """
    Check Ollama status and set up model selection.

    Args:
        args: Parsed command line arguments.
        logger: Logger instance.

    Returns:
        Selected model name or None if setup failed.
    """
    # Check if Ollama is running
    ollama_running = await check_ollama_status(
        base_url=args.ollama_url, timeout=args.timeout, logger=logger
    )

    if not ollama_running:
        logger.error("Ollama server is not running or not responding")
        print("Error: Ollama server is not running or not responding.")
        print("Please make sure Ollama is installed and running.")
        print("Installation instructions: https://ollama.ai/download")
        return None

    # Get model from args, config, or prompt user
    model = args.model
    if not model:
        # Check if default model is specified in config
        default_model = OLLAMA_CONFIG.get("default_model", "")
        if default_model:
            logger.info(f"Using default model from config: {default_model}")
            print(f"Using default model: {default_model}")
            return default_model

        try:
            print("Fetching available Ollama models...")
            models = await list_ollama_models(
                base_url=args.ollama_url, timeout=args.timeout, logger=logger
            )

            if not models:
                logger.error("No Ollama models available")
                print("Error: No Ollama models available.")
                return None

            model = interactive_model_selection(models, logger)
            if not model:
                logger.error("No model selected")
                print("Error: No model selected.")
                return None

            # Get model info and print some details
            model_info = await get_model_info(
                model=model,
                base_url=args.ollama_url,
                timeout=args.timeout,
                logger=logger,
            )

            if model_info:
                print(f"\nSelected model: {model}")
                if "details" in model_info:
                    details = model_info["details"]
                    if "parameter_size" in details:
                        print(f"Model size: {details.get('parameter_size', 'Unknown')}")
                    if "family" in details:
                        print(f"Model family: {details.get('family', 'Unknown')}")
                    if "quantization_level" in details:
                        print(
                            f"Quantization: {details.get('quantization_level', 'Unknown')}"
                        )

        except Exception as e:
            logger.error(f"Error during model setup: {str(e)}")
            print(f"Error: {str(e)}")
            return None

    return model


def show_usage_examples():
    """Show some usage examples for the script."""
    print("\nUsage Examples:")
    print("  1. Interactive mode:")
    print("     python main.py -i")
    print("  2. Direct query with default settings:")
    print('     python main.py "your search query"')
    print("  3. Specify model and query:")
    print('     python main.py "your search query" -m llama3')
    print("  4. Non-streaming mode (wait for complete response):")
    print('     python main.py "your search query" --no-stream')
    print("  5. Debug mode with more verbose output:")
    print('     python main.py "your search query" --debug')
    print("  6. Custom pages and output file:")
    print('     python main.py "your search query" -p 3 -o results.json')
    print("\nFor full list of options, use: python main.py --help")
    print()


async def main():
    """Main function to run the Baidu-Ollama integration."""
    args = parse_args()

    # If no arguments provided, automatically enable interactive mode
    if len(sys.argv) == 1:
        args.interactive = True
        print("No arguments provided, running in interactive mode...")
        print("Type 'python main.py --help' for command line options.\n")

    # Set up logging
    logger = setup_logging(args)

    # Set up scraper
    scraper = setup_scraper(args, logger)

    # Get query (from args or input)
    query = args.query
    if not query and args.interactive:
        try:
            query = input(
                "Enter search query (or press Enter to skip search): "
            ).strip()
        except (EOFError, KeyboardInterrupt):
            print("\nOperation cancelled by user.")
            return

    # Check Ollama and select model
    model = await setup_ollama_model(args, logger)
    if not model:
        return

    # Get optimal parameters for this model if not specified
    recommended_params = get_recommended_parameters(model, args.context_size)

    # Run search if query is provided
    search_results = []
    if query:
        logger.info(f"Running search for query: {query}")
        print(f"Searching Baidu for: {query}")

        search_results = await run_search(
            scraper=scraper,
            query=query,
            pages=args.pages,
            use_cache=not args.no_cache,
            logger=logger,
            debug=args.debug,  # Pass debug flag
        )

        if not search_results:
            logger.warning("No search results found")
            print("Warning: No search results found.")
        else:
            logger.info(f"Found {len(search_results)} search results")
            print(f"Found {len(search_results)} search results.")

        # Determine whether to save results:
        # - In debug mode: save by default unless --no-save-results is specified
        # - In normal mode: only save if --save-results or -o is specified
        save_needed = False
        if args.debug:
            # In debug mode, save by default unless explicitly disabled
            save_needed = not args.no_save_results
        else:
            # In normal mode, only save if requested
            save_needed = args.save_results or args.output is not None

        await save_results_if_needed(
            search_results=search_results,
            save_results=save_needed,
            output_file=args.output,
            query=query,
            logger=logger,
        )

        # Print stats if debug is enabled
        if args.debug:
            print(
                "Debug mode enabled - detailed logging information will be displayed."
            )
            print("Debug info: search results saved to file:", save_needed)
            print("Debug info: cache usage enabled:", not args.no_cache)
            print_stats(scraper, logger)

    # Determine the question to ask the LLM
    question = args.question if args.question else query
    if not question:
        try:
            question = input("Enter a question for the LLM: ").strip()
            if not question:
                logger.error("No question provided")
                print("Error: No question provided.")
                return
        except (EOFError, KeyboardInterrupt):
            print("\nOperation cancelled by user.")
            return

    # Format the search results for the LLM
    context = format_search_results_for_ollama(search_results, logger)

    # Create the prompt for the LLM
    system_prompt = create_system_prompt()
    full_prompt = create_full_prompt(system_prompt, context, question)

    # Define callback for streaming mode
    async def stream_callback(chunk):
        if "response" in chunk:
            # Print each chunk without newline to create a continuous stream
            print(chunk["response"], end="", flush=True)

    # Send the prompt to the LLM
    logger.info(f"Sending prompt to Ollama model: {model}")
    print(f"\nAsking {model}: {question}")

    # Use recommended parameters if not specified in args
    temperature = (
        args.temperature
        if args.temperature is not None
        else recommended_params["temperature"]
    )
    top_p = args.top_p if args.top_p is not None else recommended_params["top_p"]
    top_k = args.top_k if args.top_k is not None else recommended_params["top_k"]
    context_size = (
        args.context_size
        if args.context_size is not None
        else recommended_params["context_size"]
    )
    max_tokens = (
        args.max_tokens
        if args.max_tokens is not None
        else recommended_params["max_tokens"]
    )

    # Log the generation parameters if in debug mode
    if args.debug:
        logger.debug(
            f"Generation parameters: temperature={temperature}, top_p={top_p}, top_k={top_k}"
        )
        logger.debug(
            f"Context size: {context_size}, Max tokens: {max_tokens if max_tokens else 'Not limited'}"
        )
        print(
            f"Using parameters: temperature={temperature:.2f}, top_p={top_p:.2f}, top_k={top_k}"
        )
        print(
            f"Context size: {context_size}, Max tokens: {max_tokens if max_tokens else 'Not limited'}"
        )

    callback = stream_callback if args.stream else None
    response = await generate_with_ollama(
        prompt=full_prompt,
        model=model,
        base_url=args.ollama_url,
        temperature=temperature,
        top_p=top_p,
        top_k=top_k,
        context_size=context_size,
        max_tokens=max_tokens,
        stream=args.stream,
        stream_callback=callback,
        timeout=args.timeout,
        logger=logger,
    )

    # Print the response if not streaming
    if not args.stream:
        if "response" in response:
            print("\nResponse:")
            print("-" * 40)
            print(response["response"])
            print("-" * 40)
        elif "error" in response:
            print(f"\nError: {response['error']}")
            logger.error(f"Ollama API error: {response['error']}")
            return
    else:
        # Print a newline and divider after streaming completes
        print("\n" + "-" * 40)

    logger.info("Baidu-Ollama integration completed successfully")
    return True


if __name__ == "__main__":
    # Debug output at startup
    print("Starting main.py...")

    # Set event loop policy for Windows
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        print("Calling asyncio.run(main())...")
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nOperation interrupted by user.")
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback

        traceback.print_exc()
