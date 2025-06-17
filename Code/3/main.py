"""
Main application module for Baidu-Ollama integration.
Provides command-line interface for search and LLM integration.
"""

import argparse
import asyncio
import logging
import sys
from pathlib import Path
from typing import Any, Dict

sys.path.insert(0, str(Path(__file__).parent))

from config import OLLAMA_CONFIG, get_module_logger
from ollama.ollama_integrate import create_from_args, show_usage_examples
from utils.logging_utils import get_log_level_from_string

logger = get_module_logger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse and validate command line arguments."""
    parser = argparse.ArgumentParser(
        description="Baidu Search + Ollama LLM Integration",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s "artificial intelligence trends"
  %(prog)s "machine learning" -m llama3 -p 2 --debug
  %(prog)s -i  # Interactive mode
        """,
    )

    basic_group = parser.add_argument_group("Basic Options")
    basic_group.add_argument(
        "query",
        nargs="?",
        help="Search query or question (if not specified, you'll be prompted)",
    )
    basic_group.add_argument(
        "-i", "--interactive", action="store_true", help="Run in interactive mode"
    )
    basic_group.add_argument(
        "-m",
        "--model",
        help="Ollama model to use (if not specified, you'll be prompted to select)",
    )
    basic_group.add_argument(
        "--question",
        help="Specific question to ask the LLM (if different from search query)",
    )

    search_group = parser.add_argument_group("Search Options")
    search_group.add_argument(
        "-p",
        "--pages",
        type=int,
        default=5,
        help="Number of pages to scrape (default: 5)",
    )
    search_group.add_argument(
        "--no-filter-ads",
        action="store_false",
        dest="filter_ads",
        help="Disable advertisement filtering (default: enabled)",
    )
    search_group.add_argument(
        "--no-cache",
        action="store_true",
        help="Disable URL caching",
    )
    search_group.add_argument(
        "--clear-cache",
        action="store_true",
        help="Clear existing cache before search",
    )
    search_group.add_argument(
        "--cache-file",
        help="Custom cache file path",
    )

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
    output_group.add_argument(
        "--no-log-file",
        action="store_true",
        help="Don't log to file",
    )

    # Advanced options
    advanced_group = parser.add_argument_group("Advanced Options")
    advanced_group.add_argument(
        "--concurrent-pages",
        type=int,
        default=5,
        help="Concurrent pages scraping limit (default: 5)",
    )
    advanced_group.add_argument(
        "--concurrent",
        type=int,
        default=25,
        help="Concurrent requests limit (default: 25)",
    )
    advanced_group.add_argument(
        "--batch-size",
        type=int,
        default=25,
        help="Batch processing size (default: 25)",
    )
    advanced_group.add_argument(
        "--timeout",
        type=int,
        default=3,
        help="Request timeout in seconds (default: 3)",
    )
    advanced_group.add_argument(
        "--retries",
        type=int,
        default=0,
        help="Number of retry attempts for failed requests (default: 0)",
    )
    advanced_group.add_argument(
        "--proxy", action="store_true", help="Use proxy for all requests"
    )

    return parser.parse_args()


async def interactive_mode():
    """Run the application in interactive mode."""
    print("=== Baidu Search + Ollama LLM Integration ===")
    print("Welcome to Baidu Search + Ollama LLM integration tool!")
    print("Please follow the prompts to enter information for search and Q&A.")
    print("Enter 'quit', 'exit' or 'q' to exit the program\n")

    while True:
        try:
            query = input("Enter search content: ").strip()
            if query.lower() in ["quit", "exit", "q"]:
                print("Goodbye!")
                break

            if not query:
                print("Search content cannot be empty, please re-enter")
                continue

            pages = 1
            while True:
                pages_input = input(
                    "Enter number of pages to search (default: 1): "
                ).strip()
                if not pages_input:
                    pages = 1
                    break
                try:
                    pages = int(pages_input)
                    if pages <= 0:
                        print("Page count must be greater than 0, please re-enter")
                        continue
                    break
                except ValueError:
                    print("Please enter a valid number")
                    continue

            # 3. 询问问题是否与搜索一致
            question_same = (
                input("Is the question the same as search content? (y/[n]): ")
                .strip()
                .lower()
            )
            question = None
            if question_same != "y":
                question = input("Enter specific question for AI: ").strip()
                if not question:
                    print("Will use search content as question")
                    question = None

            args = argparse.Namespace(
                query=query,
                question=question,
                pages=pages,
                interactive=False,
                model=None,  # 让模型选择逻辑处理
                filter_ads=True,
                no_cache=False,
                clear_cache=False,
                cache_file=None,
                ollama_url=OLLAMA_CONFIG.get("base_url", "http://localhost:11434"),
                temperature=None,
                top_p=None,
                top_k=None,
                context_size=None,
                max_tokens=None,
                stream=OLLAMA_CONFIG.get("stream", True),
                output=None,
                save_results=True,  # 默认保存结果
                no_save_results=False,
                debug=True,  # 开启debug模式
                log_level="DEBUG",
                log_file=None,
                log_console=True,
                no_log_file=False,
                concurrent_pages=5,
                concurrent=25,
                batch_size=25,
                timeout=3,
                retries=0,
                proxy=False,
            )

            print(f"\nStarting search: {query}")
            print(f"Pages to search: {pages}")
            if question:
                print(f"Question: {question}")
            else:
                print(f"Question: {query} (using search content)")
            print("Debug mode: enabled")
            print("Save results: enabled\n")

            # 创建integrator实例并运行
            integrator = create_from_args(args)

            # 设置流式输出回调（如果启用）
            async def stream_callback(chunk: Dict[str, Any]):
                if "response" in chunk and chunk["response"]:
                    print(chunk["response"], end="", flush=True)

            # 运行整合程序
            response = await integrator.run(
                question=question,
                stream_callback=stream_callback if args.stream else None,
            )

            # 如果不是流式输出，打印完整响应
            if not args.stream and response and "response" in response:
                print(f"\nAI Response:\n{response['response']}")

            print("\n" + "=" * 60 + "\n")

        except (EOFError, KeyboardInterrupt):
            print("\n\nUser cancelled operation, goodbye!")
            break
        except Exception as e:
            logger.error(f"Interactive mode error: {e}")
            print(f"\nError occurred: {e}")
            print("Please re-enter search content\n")
            continue


async def single_run_mode(args: argparse.Namespace):
    """Run the application for a single query."""
    if not args.query:
        print("No query provided. Use -i for interactive mode or provide a query.")
        show_usage_examples()
        return 1

    try:
        integrator = create_from_args(args)

        print(f"Searching for: {args.query}")
        if args.question:
            print(f"Question: {args.question}")

        async def stream_callback(chunk: Dict[str, Any]):
            if "response" in chunk and chunk["response"]:
                print(chunk["response"], end="", flush=True)

        response = await integrator.run(
            question=args.question,
            stream_callback=stream_callback if args.stream else None,
        )

        if not args.stream and response and "response" in response:
            print(f"\nAI Response:\n{response['response']}")

        print("\nCompleted successfully!")
        return 0

    except Exception as e:
        logger.error(f"Single run mode error: {e}")
        print(f"Error: {e}")
        return 1


async def main():
    """Main application entry point."""
    if len(sys.argv) == 1:
        await interactive_mode()
        return 0

    args = parse_args()

    if args.interactive:
        await interactive_mode()
        return 0

    if args.debug:
        args.log_level = "DEBUG"
        if not args.no_save_results:
            args.save_results = True

    log_level = get_log_level_from_string(args.log_level)
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )

    try:
        return await single_run_mode(args)
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user.")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        print(f"Unexpected error: {e}")
        if args.debug:
            import traceback

            traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
