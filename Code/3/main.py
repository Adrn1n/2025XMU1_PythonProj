"""
Optimized main application module for Baidu-Ollama integration.
Provides a streamlined command-line interface using the refactored OllamaIntegrate class.
"""

import argparse
import asyncio
import logging
import sys
from pathlib import Path
from typing import Any, Dict

# Add parent directory for imports
sys.path.insert(0, str(Path(__file__).parent))

# Import the optimized integration class
from ollama.ollama_integrate import (
    create_from_args,
    show_usage_examples,
)

# Import configuration and logging utilities
from config import OLLAMA_CONFIG, get_module_logger
from utils.logging_utils import get_log_level_from_string

# Setup module-specific logger
logger = get_module_logger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Baidu Search + Ollama LLM Integration (Optimized)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s "artificial intelligence trends"
  %(prog)s "machine learning" -m llama3 -p 2 --debug
  %(prog)s -i  # Interactive mode
        """,
    )

    # Basic arguments
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

    # Search configuration
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
    print("欢迎使用百度搜索 + Ollama LLM 整合工具！")
    print("请按照提示输入相关信息进行搜索和问答。")
    print("输入 'quit', 'exit' 或 'q' 退出程序\n")

    while True:
        try:
            # 1. 询问搜索内容
            query = input("请输入搜索内容: ").strip()
            if query.lower() in ["quit", "exit", "q"]:
                print("👋 再见！")
                break

            if not query:
                print("❌ 搜索内容不能为空，请重新输入")
                continue

            # 2. 询问搜索页数
            pages = 1  # 默认值，防止异常时未定义
            while True:
                pages_input = input("请输入搜索页数 (默认: 1): ").strip()
                if not pages_input:
                    pages = 1
                    break
                try:
                    pages = int(pages_input)
                    if pages <= 0:
                        print("⚠️ 页数必须大于0，请重新输入")
                        continue
                    break
                except ValueError:
                    print("⚠️ 请输入有效的数字")
                    continue

            # 3. 询问问题是否与搜索一致
            question_same = (
                input("提问的问题是否与搜索内容一致？(y/[n]): ").strip().lower()
            )
            question = None
            if question_same != "y":
                # 4. 如果不一致，询问提问问题
                question = input("请输入要向AI提问的具体问题: ").strip()
                if not question:
                    print("⚠️ 将使用搜索内容作为提问问题")
                    question = None

            # 创建参数对象，开启debug模式
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

            print(f"\n🔍 开始搜索: {query}")
            print(f"📄 搜索页数: {pages}")
            if question:
                print(f"❓ 提问问题: {question}")
            else:
                print(f"❓ 提问问题: {query} (使用搜索内容)")
            print("🐛 调试模式: 已开启")
            print("💾 保存结果: 已启用\n")

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
                print(f"\n🤖 AI 回答:\n{response['response']}")

            print("\n" + "=" * 60 + "\n")

        except (EOFError, KeyboardInterrupt):
            print("\n\n👋 用户取消操作，再见！")
            break
        except Exception as e:
            print(f"\n❌ 运行出错: {e}")
            print("请重新输入搜索内容\n")
            continue


async def single_run_mode(args: argparse.Namespace):
    """Run the application for a single query."""
    if not args.query:
        print("❌ No query provided. Use -i for interactive mode or provide a query.")
        show_usage_examples()
        return 1

    try:
        # Create integrator
        integrator = create_from_args(args)

        print(f"🔍 Searching for: {args.query}")
        if args.question:
            print(f"❓ Question: {args.question}")

        # Setup streaming callback if enabled
        async def stream_callback(chunk: Dict[str, Any]):
            if "response" in chunk and chunk["response"]:
                print(chunk["response"], end="", flush=True)

        # Run the integration
        response = await integrator.run(
            question=args.question,
            stream_callback=stream_callback if args.stream else None,
        )

        # Print response if not streaming
        if not args.stream and response and "response" in response:
            print(f"\n🤖 AI Response:\n{response['response']}")

        print("\n✅ Completed successfully!")
        return 0

    except Exception as e:
        print(f"❌ Error: {e}")
        return 1


async def main():
    """Main function."""
    # 检查是否没有提供任何参数（直接运行）或使用了-i参数
    if len(sys.argv) == 1:
        await interactive_mode()
        return 0

    args = parse_args()

    # 如果使用了-i参数，也进入交互模式
    if args.interactive:
        await interactive_mode()
        return 0

    # Debug mode adjustments
    if args.debug:
        args.log_level = "DEBUG"
        if not args.no_save_results:
            args.save_results = True

    # Configure root logger to ensure all logs are displayed
    log_level = get_log_level_from_string(args.log_level)
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,  # Force reconfiguration
    )

    try:
        return await single_run_mode(args)
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user.")
        return 1
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        if args.debug:
            import traceback

            traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
