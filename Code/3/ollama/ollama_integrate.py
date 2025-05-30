"""
Ollama integration with Baidu search results.
This module provides a class-based interface for using Ollama with Baidu search results.
"""

import os
import sys
import json
import time
import asyncio
import logging
import argparse
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

# Add the parent directory to sys.path to allow importing from sibling packages
sys.path.insert(0, str(Path(__file__).parent.parent))

# Import from project modules
from scrapers.baidu_scraper import BaiduScraper
from utils.logging_utils import setup_logger, get_log_level_from_string
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
from utils.config_manager import ConfigManager


class OllamaIntegrate:
    """
    Integration class to use Baidu search results as context for Ollama models.
    """

    def __init__(
        self,
        # Question/query related parameters
        query: Optional[str] = None,
        question: Optional[str] = None,
        same_query_as_question: bool = True,
        pages: int = 5,
        # Ollama related parameters
        model: Optional[str] = None,
        ollama_base_url: str = "http://localhost:11434",
        ollama_timeout: int = 60,  # Timeout for initial response in seconds
        ollama_temperature: Optional[float] = None,
        ollama_top_p: Optional[float] = None,
        ollama_top_k: Optional[int] = None,
        ollama_context_size: Optional[int] = None,
        ollama_max_tokens: Optional[int] = None,
        ollama_stream: bool = True,  # Whether to use streaming response, default to True
        # Baidu scraper parameters (matching BaiduScraper)
        headers: Optional[Dict[str, str]] = None,
        proxies: List[str] = None,
        filter_ads: bool = True,
        use_proxy: bool = False,
        max_concurrent_pages: int = 5,
        max_semaphore: int = 25,
        batch_size: int = 25,
        timeout: int = 3,
        retries: int = 0,
        min_sleep: float = 0.1,
        max_sleep: float = 0.3,
        max_redirects: int = 5,
        cache_size: int = 1000,
        cache_ttl: int = 24 * 60 * 60,
        # Logging and debug options
        debug: bool = False,
        enable_logging: bool = True,  # Default to True to enable BaiduScraper logging
        log_level: str = "INFO",
        log_to_console: Optional[bool] = None,
        log_to_file: Optional[bool] = True,
        log_file: Optional[Union[str, Path]] = None,
        # File and cache options
        output_file: Optional[str] = None,
        save_results: Optional[bool] = None,
        cache_dir: Optional[str] = None,
        cache_file: Optional[str] = None,
        use_cache: bool = True,
        clear_cache: bool = False,
    ):
        # Question/query related parameters
        self.query = query
        self.question = question
        self.same_query_as_question = same_query_as_question
        self.pages = pages

        # Ollama related parameters
        self.model = model
        self.ollama_base_url = ollama_base_url
        self.ollama_timeout = ollama_timeout
        self.ollama_stream = ollama_stream
        self.ollama_temperature = ollama_temperature
        self.ollama_top_p = ollama_top_p
        self.ollama_top_k = ollama_top_k
        self.ollama_context_size = ollama_context_size
        self.ollama_max_tokens = ollama_max_tokens

        # Baidu scraper parameters
        self.headers = headers
        self.proxies = proxies
        self.filter_ads = filter_ads
        self.use_proxy = use_proxy
        self.max_concurrent_pages = max_concurrent_pages
        self.max_semaphore = max_semaphore
        self.batch_size = batch_size
        self.timeout = timeout
        self.retries = retries
        self.min_sleep = min_sleep
        self.max_sleep = max_sleep
        self.max_redirects = max_redirects
        self.cache_size = cache_size
        self.cache_ttl = cache_ttl

        # Logging and debug options
        self.debug = debug
        self.enable_logging = enable_logging
        self.log_level = log_level
        self.log_to_console = debug if log_to_console is None else log_to_console
        self.log_to_file = log_to_file
        self.log_file = log_file

        # File and cache options
        self.output_file = output_file
        self.save_results = debug if save_results is None else save_results
        self.cache_dir = cache_dir
        self.cache_file = cache_file
        self.use_cache = use_cache
        self.clear_cache = clear_cache

        # Initialize logger
        self.logger = None
        self.scraper = None
        self.available_models = []

        # Results storage
        self.search_results = []
        self.llm_response = None

        # Config manager
        self.config_manager = ConfigManager()

        # Get defaults from config if available
        try:
            from config import OLLAMA_CONFIG

            # Apply defaults from config if not explicitly provided
            if (
                ollama_base_url == "http://localhost:11434"
                and "base_url" in OLLAMA_CONFIG
            ):
                self.ollama_base_url = OLLAMA_CONFIG["base_url"]
            if ollama_timeout == 60 and "timeout" in OLLAMA_CONFIG:
                self.ollama_timeout = OLLAMA_CONFIG["timeout"]
            if ollama_stream == True and "stream" in OLLAMA_CONFIG:
                self.ollama_stream = OLLAMA_CONFIG["stream"]
            if (
                model is None
                and "default_model" in OLLAMA_CONFIG
                and OLLAMA_CONFIG["default_model"]
            ):
                self.model = OLLAMA_CONFIG["default_model"]
        except (ImportError, KeyError):
            # Use the provided values if config is not available
            pass

    def setup_logger(self):
        """Set up logging based on configuration."""
        log_file_path = None
        if self.log_to_file:
            if self.log_file:
                log_file_path = Path(self.log_file)
            else:
                # Use default log file path
                from config import LOG_FILE

                log_file_path = Path(LOG_FILE)

            # Ensure log directory exists
            log_file_path.parent.mkdir(parents=True, exist_ok=True)

        self.logger = setup_logger(
            "OllamaIntegrate",
            get_log_level_from_string(self.log_level),
            log_file_path,
            self.log_to_console,
        )
        self.logger.info("OllamaIntegrate logger initialized")

    def setup_scraper(self):
        """Set up the Baidu scraper with appropriate configuration."""
        from config import HEADERS, PROXY_LIST

        # Use the instance variables directly
        headers_to_use = self.headers or HEADERS
        proxies_to_use = self.proxies or PROXY_LIST

        # Setup log file path for BaiduScraper
        baidu_log_file = None
        if self.log_to_file and self.log_file:
            baidu_log_file = Path(self.log_file)
        elif self.log_to_file:
            # Use default log file path
            from config import LOG_FILE

            baidu_log_file = Path(LOG_FILE)

        self.scraper = BaiduScraper(
            headers=headers_to_use,
            proxies=proxies_to_use,
            filter_ads=self.filter_ads,
            use_proxy=self.use_proxy,
            max_concurrent_pages=self.max_concurrent_pages,
            max_semaphore=self.max_semaphore,
            batch_size=self.batch_size,
            timeout=self.timeout,
            retries=self.retries,
            min_sleep=self.min_sleep,
            max_sleep=self.max_sleep,
            max_redirects=self.max_redirects,
            cache_size=self.cache_size,
            cache_ttl=self.cache_ttl,
            enable_logging=self.enable_logging,
            log_to_console=self.log_to_console,
            log_level=get_log_level_from_string(self.log_level),
            log_file=baidu_log_file,
        )

        # Clear cache if requested
        if self.clear_cache:
            self.logger.info("Clearing URL cache")
            self.scraper.url_cache.clear()

    def get_cache_file_path(self) -> Optional[Path]:
        """Determine the cache file path."""
        if not self.use_cache:
            return None

        if self.cache_file:
            return Path(self.cache_file)

        # Use default cache directory and file
        from config import CACHE_DIR

        cache_dir = Path(self.cache_dir or CACHE_DIR)
        cache_dir.mkdir(parents=True, exist_ok=True)
        return cache_dir / "url_cache.json"

    def get_output_file_path(self) -> Optional[Path]:
        """Determine the output file path for search results."""
        if not self.save_results:
            return None

        if self.output_file:
            output_path = Path(self.output_file)
        else:
            # Use default output path
            from config import CACHE_DIR
            import time

            cache_dir = Path(self.cache_dir or CACHE_DIR)
            # Sanitize the query string for use in the filename
            safe_query = "".join(c if c.isalnum() else "_" for c in self.query)
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            output_path = cache_dir / f"baidu_search_{safe_query}_{timestamp}.json"

        # Ensure the directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        return output_path

    async def run_search(self) -> List[Dict[str, Any]]:
        """Execute the search operation using the scraper instance."""
        if not self.query:
            self.query = input("Please enter search keyword: ").strip()
            if not self.query:
                self.logger.error("No search keyword provided")
                return []

        cache_file = self.get_cache_file_path()

        try:
            self.logger.info(f"Starting search for '{self.query}', pages: {self.pages}")
            results = await self.scraper.scrape(
                query=self.query,
                num_pages=self.pages,
                cache_to_file=self.use_cache,
                cache_file=cache_file,
            )
            self.logger.info(f"Search completed, retrieved {len(results)} results")
            return results
        except KeyboardInterrupt:
            self.logger.warning("Search interrupted by user")
            return []
        except Exception as e:
            self.logger.error(f"Error occurred during search: {str(e)}")
            return []

    async def save_results_if_needed(self):
        """Save search results to file if configured to do so."""
        if not self.search_results or not self.save_results:
            return

        output_path = self.get_output_file_path()
        if not output_path:
            return

        success = await save_search_results(
            results=self.search_results,
            file_path=output_path,
            save_timestamp=True,
            logger=self.logger,
        )

        if success:
            self.logger.info(f"Search results saved to: {output_path}")
            print(f"Search results saved to: {output_path}")
        else:
            self.logger.error("Failed to save search results")

    def print_stats(self):
        """Print scraper statistics."""
        if not self.scraper:
            return

        stats = self.scraper.get_stats()
        self.logger.info("Scraper statistics:")
        self.logger.info(f" - Total requests: {stats['total']}")
        self.logger.info(f" - Successful requests: {stats['success']}")
        self.logger.info(f" - Failed requests: {stats['failed']}")

        success_rate = 0
        if stats.get("total", 0) > 0:
            success_rate = stats.get("success", 0) / stats.get("total", 1) * 100
        self.logger.info(f" - Success rate: {success_rate:.2f}%")

        if "duration" in stats:
            self.logger.info(f" - Duration: {stats.get('duration', 0):.2f} seconds")

        if "cache" in stats:
            cache_stats = stats["cache"]
            self.logger.info("Cache statistics:")
            self.logger.info(
                f" - Cache size: {cache_stats.get('size', 0)}/{cache_stats.get('max_size', 0)}"
            )
            self.logger.info(f" - Cache hits: {cache_stats.get('hits', 0)}")
            self.logger.info(f" - Cache misses: {cache_stats.get('misses', 0)}")

            # Avoid division by zero if no cache operations occurred
            total_ops = cache_stats.get("hits", 0) + cache_stats.get("misses", 0)
            if total_ops > 0:
                hit_rate = cache_stats.get("hits", 0) / total_ops * 100
                self.logger.info(f" - Cache hit rate: {hit_rate:.2f}%")

    async def setup_ollama(self) -> bool:
        """Check Ollama server status and set up model."""
        # Check if Ollama is running
        ollama_running = await check_ollama_status(
            base_url=self.ollama_base_url, timeout=self.timeout, logger=self.logger
        )

        if not ollama_running:
            self.logger.error("Ollama server is not running or not responding")
            print("Error: Ollama server is not running or not responding.")
            print("Please make sure Ollama is installed and running.")
            print("Installation instructions: https://ollama.ai/download")
            return False

        # List models and select one if not already specified
        if not self.model:
            try:
                print("Fetching available Ollama models...")
                models = await list_ollama_models(
                    base_url=self.ollama_base_url,
                    timeout=self.timeout,
                    logger=self.logger,
                )

                if not models:
                    self.logger.error("No Ollama models available")
                    print("Error: No Ollama models available.")
                    return False

                self.model = interactive_model_selection(models, self.logger)
                if not self.model:
                    self.logger.error("No model selected")
                    print("Error: No model selected.")
                    return False

                # Store available models for later use
                self.available_models = models

            except Exception as e:
                self.logger.error(f"Error listing Ollama models: {str(e)}")
                print(f"Error: {str(e)}")
                return False

        # Get model info and print some details
        model_info = await get_model_info(
            model=self.model,
            base_url=self.ollama_base_url,
            timeout=self.timeout,
            logger=self.logger,
        )

        if model_info:
            print(f"\nSelected model: {self.model}")
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

        return True

    async def run(self):
        """Run the complete integration process."""
        # Set up logger
        self.setup_logger()
        self.logger.info("Starting OllamaIntegrate")

        # Set up scraper
        self.setup_scraper()

        # Check Ollama server and setup model
        if not await self.setup_ollama():
            return False

        # Ask for query if in interactive mode or if needed
        if not self.query:
            try:
                self.query = input(
                    "Enter search query (or press Enter to skip search): "
                ).strip()
            except EOFError:
                print("\nNo input provided.")
                if not self.model:
                    return False

        # Run search if query is provided
        if self.query:
            self.logger.info(f"Running search for query: {self.query}")
            print(f"Searching Baidu for: {self.query}")

            self.search_results = await self.run_search()

            if not self.search_results:
                self.logger.warning("No search results found")
                print("Warning: No search results found.")
            else:
                self.logger.info(f"Found {len(self.search_results)} search results")
                print(f"Found {len(self.search_results)} search results.")

            # Save results if needed
            await self.save_results_if_needed()

            # Print stats if debug is enabled
            if self.debug:
                self.print_stats()

        # Determine the question to ask the LLM
        question = self.question
        if self.same_query_as_question and self.query:
            question = self.query

        if not question:
            try:
                question = input("Enter a question for the LLM: ").strip()
                if not question:
                    self.logger.error("No question provided")
                    print("Error: No question provided.")
                    return False
            except (EOFError, KeyboardInterrupt):
                print("\nOperation cancelled by user.")
                return False

        # Format the search results for the LLM
        context = format_search_results_for_ollama(self.search_results, self.logger)

        # Create the prompt for the LLM
        system_prompt = create_system_prompt()
        full_prompt = create_full_prompt(system_prompt, context, question)

        self.logger.info(f"Sending prompt to Ollama model: {self.model}")
        print(f"\nAsking {self.model}: {question}")

        # Get recommended parameters for this model
        recommended_params = get_recommended_parameters(
            model=self.model, context_size=self.ollama_context_size
        )

        # Use recommended parameters if not specified
        temperature = (
            self.ollama_temperature
            if self.ollama_temperature is not None
            else recommended_params["temperature"]
        )
        top_p = (
            self.ollama_top_p
            if self.ollama_top_p is not None
            else recommended_params["top_p"]
        )
        top_k = (
            self.ollama_top_k
            if self.ollama_top_k is not None
            else recommended_params["top_k"]
        )
        context_size = (
            self.ollama_context_size
            if self.ollama_context_size is not None
            else recommended_params["context_size"]
        )
        max_tokens = (
            self.ollama_max_tokens
            if self.ollama_max_tokens is not None
            else recommended_params["max_tokens"]
        )

        # Log the generation parameters if in debug mode
        if self.debug:
            self.logger.debug(
                f"Generation parameters: temperature={temperature}, top_p={top_p}, top_k={top_k}"
            )
            self.logger.debug(
                f"Context size: {context_size}, Max tokens: {max_tokens if max_tokens else 'Not limited'}"
            )
            print(
                f"Using parameters: temperature={temperature:.2f}, top_p={top_p:.2f}, top_k={top_k}"
            )
            print(
                f"Context size: {context_size}, Max tokens: {max_tokens if max_tokens else 'Not limited'}"
            )

        # Define streaming callback if needed
        async def stream_callback(chunk):
            if "response" in chunk:
                # Print each chunk without newline to create a continuous stream
                print(chunk["response"], end="", flush=True)

        # Send prompt to Ollama
        callback = stream_callback if self.ollama_stream else None
        response = await generate_with_ollama(
            prompt=full_prompt,
            model=self.model,
            base_url=self.ollama_base_url,
            temperature=temperature,
            top_p=top_p,
            top_k=top_k,
            context_size=context_size,
            max_tokens=max_tokens,
            stream=self.ollama_stream,
            stream_callback=callback,
            timeout=self.ollama_timeout,
            logger=self.logger,
        )

        # Handle non-streaming response
        if not self.ollama_stream:
            if "response" in response:
                print("\nResponse:")
                print("-" * 40)
                print(response["response"])
                print("-" * 40)
            elif "error" in response:
                print(f"\nError: {response['error']}")
                self.logger.error(f"Ollama API error: {response['error']}")
                return False
        else:
            # Print a newline and divider after streaming completes
            print("\n" + "-" * 40)

        # Store the response
        self.llm_response = response

        self.logger.info("OllamaIntegrate completed successfully")
        return True


def show_usage_examples():
    """Show some usage examples for the script."""
    print("\nUsage Examples:")
    print("  1. Interactive mode:")
    print("     python ollama_integrate.py -i")
    print("  2. Direct query with default settings:")
    print('     python ollama_integrate.py "your search query"')
    print("  3. Specify model and query:")
    print('     python ollama_integrate.py "your search query" -m llama3')
    print("  4. Non-streaming mode (wait for complete response):")
    print('     python ollama_integrate.py "your search query" --no-stream')
    print("  5. Debug mode with more verbose output:")
    print('     python ollama_integrate.py "your search query" --debug')
    print("  6. Custom pages and output file:")
    print('     python ollama_integrate.py "your search query" -p 3 -o results.json')
    print("\nFor full list of options, use: python ollama_integrate.py --help")
    print()


async def main():
    """Main function to demonstrate OllamaIntegrate usage."""
    parser = argparse.ArgumentParser(description="Ollama integration with Baidu search")

    # Create argument groups for better organization
    basic_group = parser.add_argument_group("Basic Options")
    search_group = parser.add_argument_group("Search Options")
    ollama_group = parser.add_argument_group("Ollama Options")
    output_group = parser.add_argument_group("Output Options")
    advanced_group = parser.add_argument_group("Advanced Options")

    # Basic options
    basic_group.add_argument("query", nargs="?", help="Search query")
    basic_group.add_argument(
        "-i", "--interactive", action="store_true", help="Run in interactive mode"
    )
    basic_group.add_argument("-m", "--model", help="Ollama model to use")

    # Search options
    search_group.add_argument(
        "-p", "--pages", type=int, default=1, help="Number of pages to scrape"
    )
    search_group.add_argument(
        "--question", help="Question for the LLM (if different from search query)"
    )
    search_group.add_argument(
        "--no-same-query",
        dest="same_query",
        action="store_false",
        help="Don't use search query as the question",
    )
    search_group.add_argument(
        "--no-filter-ads",
        action="store_false",
        dest="filter_ads",
        help="Don't filter ads",
    )
    search_group.add_argument("--proxy", action="store_true", help="Use proxy")

    # Ollama options
    ollama_group.add_argument(
        "--ollama-url", default="http://localhost:11434", help="Ollama API base URL"
    )
    ollama_group.add_argument(
        "--ollama-timeout",
        type=int,
        default=30,
        help="Timeout for initial Ollama response in seconds",
    )
    ollama_group.add_argument(
        "--temperature",
        type=float,
        help="Ollama temperature (uses model-specific defaults if not specified)",
    )
    ollama_group.add_argument(
        "--top-p",
        type=float,
        help="Ollama top-p (uses model-specific defaults if not specified)",
    )
    ollama_group.add_argument(
        "--top-k",
        type=int,
        help="Ollama top-k (uses model-specific defaults if not specified)",
    )
    ollama_group.add_argument("--max-tokens", type=int, help="Ollama max tokens")
    ollama_group.add_argument(
        "--context-size",
        type=int,
        help="Ollama context size (None for maximum/unlimited)",
    )
    ollama_group.add_argument(
        "--unlimited-context",
        action="store_true",
        help="Use maximum/unlimited context size",
    )
    ollama_group.add_argument(
        "--no-stream",
        dest="stream",
        action="store_false",
        default=True,
        help="Disable streaming response (wait for complete response)",
    )

    # Output options
    output_group.add_argument("--debug", action="store_true", help="Enable debug mode")
    output_group.add_argument("-o", "--output", help="Output file for search results")
    output_group.add_argument("--log-file", help="Log file path")
    output_group.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Log level",
    )
    output_group.add_argument(
        "--log-console", action="store_true", help="Log to console"
    )
    output_group.add_argument(
        "--no-log-console",
        action="store_false",
        dest="log_console",
        help="Don't log to console",
    )
    output_group.add_argument(
        "--no-log-file",
        action="store_false",
        dest="log_file_enabled",
        help="Don't write logs to file",
    )
    output_group.add_argument(
        "--enable-logging",
        action="store_true",
        dest="enable_logging",
        default=True,
        help="Enable logging for OllamaIntegrate and BaiduScraper",
    )
    output_group.add_argument(
        "--disable-logging",
        action="store_false",
        dest="enable_logging",
        help="Disable logging for OllamaIntegrate and BaiduScraper",
    )
    output_group.add_argument(
        "--save-results", action="store_true", help="Save search results"
    )
    output_group.add_argument(
        "--no-save-results",
        action="store_false",
        dest="save_results",
        help="Don't save search results",
    )

    # Advanced options
    advanced_group.add_argument("--cache-dir", help="Cache directory")
    advanced_group.add_argument("--cache-file", help="Cache file")
    advanced_group.add_argument(
        "--no-cache", action="store_false", dest="use_cache", help="Don't use cache"
    )
    advanced_group.add_argument(
        "--clear-cache", action="store_true", help="Clear cache"
    )
    advanced_group.add_argument(
        "--concurrent-pages",
        type=int,
        default=5,
        help="Max concurrent pages (default: 5)",
    )
    advanced_group.add_argument(
        "--concurrent",
        type=int,
        default=25,
        help="Max concurrent requests (default: 25)",
    )
    advanced_group.add_argument(
        "--batch-size", type=int, default=25, help="Batch size (default: 25)"
    )
    advanced_group.add_argument(
        "--timeout", type=int, default=3, help="Request timeout in seconds (default: 3)"
    )
    advanced_group.add_argument(
        "--retries", type=int, default=0, help="Number of retries (default: 0)"
    )

    # If no arguments provided, show examples and help
    if len(sys.argv) == 1:
        parser.print_help()
        show_usage_examples()
        return False

    args = parser.parse_args()

    # If no query and no model and not in interactive mode, display help and exit
    if not args.query and not args.model and not args.interactive:
        parser.print_help()
        show_usage_examples()
        print(
            "\nError: You must provide a search query or a model to use, or use interactive mode (-i)"
        )
        return False

    # Create OllamaIntegrate instance with parsed arguments
    integrator = OllamaIntegrate(
        query=args.query,
        pages=args.pages,
        model=args.model,
        same_query_as_question=args.same_query,
        question=args.question,
        debug=args.debug,
        output_file=args.output,
        log_file=args.log_file,
        log_level=args.log_level,
        log_to_console=args.log_console,
        log_to_file=args.log_file_enabled,
        enable_logging=args.enable_logging,
        save_results=args.save_results,
        cache_dir=args.cache_dir,
        cache_file=args.cache_file,
        use_cache=args.use_cache,
        clear_cache=args.clear_cache,
        filter_ads=args.filter_ads,
        use_proxy=args.proxy,
        max_concurrent_pages=args.concurrent_pages,
        max_semaphore=args.concurrent,
        batch_size=args.batch_size,
        timeout=args.timeout,
        retries=args.retries,
        ollama_base_url=args.ollama_url,
        ollama_timeout=args.ollama_timeout,
        ollama_temperature=args.temperature,
        ollama_top_p=args.top_p,
        ollama_top_k=args.top_k,
        ollama_max_tokens=args.max_tokens,
        ollama_context_size=None if args.unlimited_context else args.context_size,
        ollama_stream=args.stream,
    )

    # Run the integration
    return await integrator.run()


if __name__ == "__main__":
    # Set event loop policy for Windows
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nOperation interrupted by user.")
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback

        traceback.print_exc()
