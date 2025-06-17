"""
Ollama integration with Baidu search results.
Provides streamlined interface for search-augmented LLM interactions.
"""

import argparse
import asyncio
import logging
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import DEFAULT_CONFIG, LOG_FILE, OLLAMA_CONFIG, HEADERS, PROXY_LIST
from scrapers.baidu_scraper import BaiduScraper
from utils.file_utils import save_search_results
from utils.logging_utils import (
    get_log_level_from_string,
    setup_logger,
    setup_module_logger,
)
from utils.ollama_utils import (
    check_ollama_status,
    create_full_prompt,
    create_system_prompt,
    format_search_results_for_ollama,
    generate_with_ollama,
    get_recommended_parameters,
    interactive_model_selection,
    list_ollama_models,
)


@dataclass
class SearchConfig:
    """Configuration for search operations."""

    query: Optional[str] = None
    question: Optional[str] = None
    pages: int = 1
    filter_ads: bool = True
    use_cache: bool = True
    clear_cache: bool = False
    cache_file: Optional[str] = None


@dataclass
class OllamaConfig:
    """Configuration for Ollama operations."""

    model: Optional[str] = None
    base_url: str = field(
        default_factory=lambda: OLLAMA_CONFIG.get("base_url", "http://localhost:11434")
    )
    timeout: int = field(default_factory=lambda: OLLAMA_CONFIG.get("timeout", 60))
    temperature: Optional[float] = None
    top_p: Optional[float] = None
    top_k: Optional[int] = None
    context_size: Optional[int] = None
    max_tokens: Optional[int] = None
    stream: bool = field(default_factory=lambda: OLLAMA_CONFIG.get("stream", True))


@dataclass
class ScraperConfig:
    """Configuration for scraper operations."""

    headers: Optional[Dict[str, str]] = field(default_factory=lambda: HEADERS)
    proxies: Optional[List[str]] = field(default_factory=lambda: PROXY_LIST)
    use_proxy: bool = False
    max_concurrent_pages: int = field(
        default_factory=lambda: DEFAULT_CONFIG.get("max_concurrent_pages", 5)
    )
    max_semaphore: int = field(
        default_factory=lambda: DEFAULT_CONFIG.get("max_semaphore", 25)
    )
    batch_size: int = field(
        default_factory=lambda: DEFAULT_CONFIG.get("batch_size", 25)
    )
    timeout: int = field(default_factory=lambda: DEFAULT_CONFIG.get("timeout", 3))
    retries: int = field(default_factory=lambda: DEFAULT_CONFIG.get("retries", 0))
    min_sleep: float = field(
        default_factory=lambda: DEFAULT_CONFIG.get("min_sleep", 0.1)
    )
    max_sleep: float = field(
        default_factory=lambda: DEFAULT_CONFIG.get("max_sleep", 0.3)
    )
    max_redirects: int = field(
        default_factory=lambda: DEFAULT_CONFIG.get("max_redirects", 5)
    )
    cache_size: int = field(
        default_factory=lambda: DEFAULT_CONFIG.get("cache_size", 1000)
    )
    cache_ttl: int = 24 * 60 * 60


@dataclass
class OutputConfig:
    """Output and logging configuration."""
    output_file: Optional[str] = None
    save_results: bool = False
    debug: bool = False
    log_level: str = "INFO"
    log_to_console: bool = True
    log_to_file: bool = True
    log_file: Optional[Union[str, Path]] = None


class OptimizedOllamaIntegrate:
    """
    Integration class for Baidu search with Ollama LLM.
    Provides search-augmented generation capabilities.
    """

    def __init__(
        self,
        search_config: Optional[SearchConfig] = None,
        ollama_config: Optional[OllamaConfig] = None,
        scraper_config: Optional[ScraperConfig] = None,
        output_config: Optional[OutputConfig] = None,
    ):
        self.search_config = search_config or SearchConfig()
        self.ollama_config = ollama_config or OllamaConfig()
        self.scraper_config = scraper_config or ScraperConfig()
        self.output_config = output_config or OutputConfig()

        self.logger = None
        self.scraper = None
        self.available_models = []
        self.search_results = []
        self.llm_response = None

        # Performance metrics
        self.metrics = {
            "search_time": 0.0,
            "ollama_time": 0.0,
            "total_time": 0.0,
            "results_count": 0,
            "cache_hits": 0,
        }

        # Setup
        self._setup_logger()
        self._setup_scraper()

    def _setup_logger(self):
        """Set up logging based on configuration."""
        try:
            # Try to use module-specific logging
            from config import files

            # Use the module path instead of class name for better mapping
            module_name = f"{__name__}.{self.__class__.__name__}"
            self.logger = setup_module_logger(
                name=module_name,
                log_level=get_log_level_from_string(self.output_config.log_level),
                config_files=files,
                log_to_console=self.output_config.log_to_console,
                propagate=False,  # Prevent logging to parent loggers
            )
        except ImportError as e:
            print(f"Warning: Module-specific logging setup failed: {e}")
            # Fallback to standard logging
            log_file_path = None
            if self.output_config.log_to_file:
                if self.output_config.log_file:
                    log_file_path = Path(self.output_config.log_file)
                else:
                    log_file_path = Path(LOG_FILE)
                log_file_path.parent.mkdir(parents=True, exist_ok=True)
                print(f"Setting up log file at: {log_file_path}")

            self.logger = setup_logger(
                name="OllamaIntegrate",
                log_level=get_log_level_from_string(self.output_config.log_level),
                log_file=log_file_path,
                log_to_console=self.output_config.log_to_console,
                propagate=True,
            )
        self.logger.info("OllamaIntegrate initialized")

    def _setup_scraper(self):
        """Set up Baidu scraper with optimized configuration."""
        try:
            scraper_args = {
                "headers": self.scraper_config.headers,
                "proxies": (
                    self.scraper_config.proxies if self.scraper_config.use_proxy else []
                ),
                "max_semaphore": self.scraper_config.max_semaphore,
                "batch_size": self.scraper_config.batch_size,
                "timeout": self.scraper_config.timeout,
                "retries": self.scraper_config.retries,
                "min_sleep": self.scraper_config.min_sleep,
                "max_sleep": self.scraper_config.max_sleep,
                "max_redirects": self.scraper_config.max_redirects,
                "cache_size": self.scraper_config.cache_size,
                "enable_logging": True,
                "log_level": get_log_level_from_string(self.output_config.log_level),
                "log_to_console": self.output_config.log_to_console,
            }

            # Set log file if configured
            if self.output_config.log_to_file:
                if self.output_config.log_file:
                    scraper_args["log_file"] = Path(self.output_config.log_file)
                else:
                    scraper_args["log_file"] = Path(LOG_FILE)

            self.scraper = BaiduScraper(**scraper_args)

            # Ensure scraper logger exists and configure it properly
            if self.scraper.logger:
                self.scraper.logger.propagate = True
                self.logger.info(
                    f"BaiduScraper initialized with logger: {self.scraper.logger.name}"
                )
            else:
                self.logger.warning("BaiduScraper was initialized without a logger")

        except Exception as e:
            if self.logger:
                self.logger.error(f"Failed to initialize scraper: {str(e)}")
            raise

    def get_cache_file_path(self) -> Optional[Path]:
        """Get the cache file path."""
        if self.search_config.cache_file:
            return Path(self.search_config.cache_file)
        return None

    def get_output_file_path(self) -> Optional[Path]:
        """Get the output file path."""
        if self.output_config.output_file:
            return Path(self.output_config.output_file)
        return None

    async def run_search(self) -> List[Dict[str, Any]]:
        """Execute search and return results."""
        if not self.search_config.query:
            if self.logger:
                self.logger.error("No search query provided")
            return []

        start_time = time.time()

        try:
            self.logger.info(f"Starting search for: {self.search_config.query}")

            results = await self.scraper.scrape(
                query=self.search_config.query,
                pages=self.search_config.pages,
                filter_ads=self.search_config.filter_ads,
                max_concurrent_pages=self.scraper_config.max_concurrent_pages,
                cache_file=self.get_cache_file_path(),
                use_cache=self.search_config.use_cache,
                clear_cache=self.search_config.clear_cache,
            )

            self.search_results = results
            self.metrics["results_count"] = len(results)
            self.metrics["search_time"] = time.time() - start_time

            if hasattr(self.scraper, "url_cache") and self.scraper.url_cache:
                cache_stats = self.scraper.url_cache.stats()
                self.metrics["cache_hits"] = cache_stats.get("hits", 0)

            self.logger.info(
                f"Search completed: {len(results)} results in {self.metrics['search_time']:.2f}s"
            )
            return results

        except Exception as e:
            self.logger.error(f"Search failed: {str(e)}")
            return []

    async def save_results_if_needed(self):
        """Save search results if configured to do so."""
        if not self.output_config.save_results or not self.search_results:
            return

        output_path = self.get_output_file_path()
        if not output_path:
            # Generate default filename and save to cache directory
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            query_safe = "".join(
                c
                for c in self.search_config.query[:20]
                if c.isalnum() or c in (" ", "-", "_")
            ).strip()

            # Ensure cache directory exists
            cache_dir = Path("cache")
            cache_dir.mkdir(exist_ok=True)

            # Save to cache directory
            output_path = cache_dir / f"search_results_{query_safe}_{timestamp}.json"

        try:
            await save_search_results(
                self.search_results, output_path, logger=self.logger
            )
            self.logger.info(f"Results saved to: {output_path}")
        except Exception as e:
            self.logger.error(f"Failed to save results: {str(e)}")

    def print_stats(self):
        """Print performance statistics."""
        if self.logger:
            stats_msg = (
                f"Performance Stats - "
                f"Search: {self.metrics['search_time']:.2f}s, "
                f"LLM: {self.metrics['ollama_time']:.2f}s, "
                f"Total: {self.metrics['total_time']:.2f}s, "
                f"Results: {self.metrics['results_count']}, "
                f"Cache hits: {self.metrics['cache_hits']}"
            )
            self.logger.info(stats_msg)

    async def setup_ollama(self) -> bool:
        """Set up Ollama connection and model selection."""
        # Check if Ollama is running
        if not await check_ollama_status(
            self.ollama_config.base_url, logger=self.logger
        ):
            self.logger.error("Ollama server is not running or not accessible")
            return False

        # Get available models
        self.available_models = await list_ollama_models(
            self.ollama_config.base_url, logger=self.logger
        )

        if not self.available_models:
            self.logger.error("No Ollama models available")
            return False

        # Select model if not already specified
        if not self.ollama_config.model:
            self.ollama_config.model = interactive_model_selection(
                self.available_models, logger=self.logger
            )
            if not self.ollama_config.model:
                self.logger.error("No model selected")
                return False

        # Validate model exists
        if self.ollama_config.model not in self.available_models:
            self.logger.error(f"Model '{self.ollama_config.model}' not available")
            return False

        self.logger.info(f"Using Ollama model: {self.ollama_config.model}")
        return True

    async def generate_response(
        self,
        question: Optional[str] = None,
        stream_callback: Optional[Callable[[Dict[str, Any]], Any]] = None,
    ) -> Dict[str, Any]:
        """Generate response using Ollama."""
        question = question or self.search_config.question or self.search_config.query
        if not question:
            return {"error": "No question provided"}

        start_time = time.time()

        try:
            # Format search results (even if empty or minimal)
            context = format_search_results_for_ollama(
                self.search_results, logger=self.logger
            )

            # Create prompts
            system_prompt = create_system_prompt()
            full_prompt = create_full_prompt(system_prompt, context, question)

            # Get recommended parameters for the model
            recommended_params = get_recommended_parameters(self.ollama_config.model)

            # Use specified parameters or fall back to recommended ones
            temperature = (
                self.ollama_config.temperature or recommended_params["temperature"]
            )
            top_p = self.ollama_config.top_p or recommended_params["top_p"]
            top_k = self.ollama_config.top_k or recommended_params["top_k"]
            context_size = (
                self.ollama_config.context_size or recommended_params["context_size"]
            )

            # Generate response
            response = await generate_with_ollama(
                prompt=full_prompt,
                model=self.ollama_config.model,
                base_url=self.ollama_config.base_url,
                temperature=temperature,
                top_p=top_p,
                top_k=top_k,
                context_size=context_size,
                max_tokens=self.ollama_config.max_tokens,
                stream=self.ollama_config.stream,
                stream_callback=stream_callback,
                timeout=self.ollama_config.timeout,
                logger=self.logger,
            )

            self.llm_response = response
            self.metrics["ollama_time"] = time.time() - start_time

            return response

        except Exception as e:
            self.logger.error(f"LLM generation failed: {str(e)}")
            return {"error": str(e)}

    async def run(
        self,
        question: Optional[str] = None,
        stream_callback: Optional[Callable[[Dict[str, Any]], Any]] = None,
    ) -> Dict[str, Any]:
        """Run the complete workflow: search + LLM generation."""
        total_start = time.time()

        try:
            # Setup Ollama
            if not await self.setup_ollama():
                return {"error": "Failed to setup Ollama"}

            # Run search
            search_results = await self.run_search()

            # Save results if needed
            await self.save_results_if_needed()

            # Check if we have results to work with
            if not search_results:
                self.logger.warning(
                    "No search results found, but continuing with LLM generation"
                )
                # Still try to generate a response, but with a note about no search results
                no_results_message = f"No search results were found for the query '{self.search_config.query}'. The search may have failed or returned no results."

                # Create a minimal search result to provide context
                self.search_results = [
                    {
                        "title": "No search results",
                        "content": no_results_message,
                        "url": "",
                        "source": "System message",
                    }
                ]

            # Generate response
            response = await self.generate_response(question, stream_callback)

            # Calculate total time
            self.metrics["total_time"] = time.time() - total_start

            # Print stats
            self.print_stats()

            return response

        except Exception as e:
            self.logger.error(f"Workflow failed: {str(e)}")
            return {"error": str(e)}


# For backward compatibility
OllamaIntegrate = OptimizedOllamaIntegrate


def create_from_args(args: argparse.Namespace) -> OptimizedOllamaIntegrate:
    """Create OptimizedOllamaIntegrate instance from command line arguments."""
    search_config = SearchConfig(
        query=args.query,
        question=getattr(args, "question", None),
        pages=getattr(args, "pages", 1),
        filter_ads=getattr(args, "filter_ads", True),
        use_cache=not getattr(args, "no_cache", False),
        clear_cache=getattr(args, "clear_cache", False),
        cache_file=getattr(args, "cache_file", None),
    )

    ollama_config = OllamaConfig(
        model=getattr(args, "model", None),
        base_url=getattr(args, "ollama_url", "http://localhost:11434"),
        temperature=getattr(args, "temperature", None),
        top_p=getattr(args, "top_p", None),
        top_k=getattr(args, "top_k", None),
        context_size=getattr(args, "context_size", None),
        max_tokens=getattr(args, "max_tokens", None),
        stream=getattr(args, "stream", True),
    )

    scraper_config = ScraperConfig(
        use_proxy=getattr(args, "proxy", False),
        max_concurrent_pages=getattr(args, "concurrent_pages", 5),
        max_semaphore=getattr(args, "concurrent", 25),
        batch_size=getattr(args, "batch_size", 25),
        timeout=getattr(args, "timeout", 3),
        retries=getattr(args, "retries", 0),
    )

    # Set log level to DEBUG if debug flag is set
    log_level = (
        "DEBUG" if getattr(args, "debug", False) else getattr(args, "log_level", "INFO")
    )

    output_config = OutputConfig(
        output_file=getattr(args, "output", None),
        save_results=getattr(args, "save_results", False)
        and not getattr(args, "no_save_results", False),
        debug=getattr(args, "debug", False),
        log_level=log_level,
        log_to_console=getattr(args, "log_console", True),
        log_to_file=True,  # Enable logging to file by default
        log_file=getattr(args, "log_file", None),
    )

    return OptimizedOllamaIntegrate(
        search_config, ollama_config, scraper_config, output_config
    )


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


async def main():
    """Main function to demonstrate OptimizedOllamaIntegrate usage."""
    parser = argparse.ArgumentParser(
        description="Optimized Ollama integration with Baidu search"
    )

    # Basic options
    basic_group = parser.add_argument_group("Basic Options")
    basic_group.add_argument("query", nargs="?", help="Search query")
    basic_group.add_argument(
        "-i", "--interactive", action="store_true", help="Run in interactive mode"
    )
    basic_group.add_argument("-m", "--model", help="Ollama model to use")
    basic_group.add_argument(
        "--question", help="Specific question (if different from query)"
    )

    # Search options
    search_group = parser.add_argument_group("Search Options")
    search_group.add_argument(
        "-p", "--pages", type=int, default=1, help="Number of pages to scrape"
    )
    search_group.add_argument(
        "--no-filter-ads",
        action="store_false",
        dest="filter_ads",
        help="Disable ad filtering",
    )
    search_group.add_argument("--no-cache", action="store_true", help="Disable cache")
    search_group.add_argument(
        "--clear-cache", action="store_true", help="Clear cache before search"
    )

    # Ollama options
    ollama_group = parser.add_argument_group("Ollama Options")
    ollama_group.add_argument(
        "--ollama-url", default="http://localhost:11434", help="Ollama API base URL"
    )
    ollama_group.add_argument("--temperature", type=float, help="Temperature parameter")
    ollama_group.add_argument("--top-p", type=float, help="Top-p parameter")
    ollama_group.add_argument("--top-k", type=int, help="Top-k parameter")
    ollama_group.add_argument("--context-size", type=int, help="Context size")
    ollama_group.add_argument("--max-tokens", type=int, help="Maximum tokens")
    ollama_group.add_argument(
        "--no-stream", action="store_false", dest="stream", help="Disable streaming"
    )

    # Output options
    output_group = parser.add_argument_group("Output Options")
    output_group.add_argument("-o", "--output", help="Output file")
    output_group.add_argument(
        "--save-results", action="store_true", help="Save search results"
    )
    output_group.add_argument("--debug", action="store_true", help="Enable debug mode")
    output_group.add_argument("--log-level", default="INFO", help="Log level")
    output_group.add_argument("--log-file", help="Log file path")

    args = parser.parse_args()

    # Configure root logger based on args
    log_level = get_log_level_from_string(args.log_level)
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,  # Force reconfiguration
    )

    if args.interactive:
        # Interactive mode implementation would go here
        print("Interactive mode not implemented in this example")
        return

    if not args.query:
        parser.print_help()
        return

    # Create and run integration
    integration = create_from_args(args)

    def stream_callback(chunk):
        """Simple streaming callback."""
        if "response" in chunk:
            print(chunk["response"], end="", flush=True)

    response = await integration.run(
        stream_callback=stream_callback if args.stream else None
    )

    if "error" in response:
        print(f"Error: {response['error']}")
    elif "response" in response and not args.stream:
        print(response["response"])


if __name__ == "__main__":
    asyncio.run(main())
