import asyncio
import argparse
import aiohttp
import logging
import sys
import time
from pathlib import Path
from typing import Dict, Any, Optional, List

from scrapers.baidu_scraper import BaiduScraper
from utils.file_utils import save_search_results
from utils.logging_utils import setup_logger, get_log_level_from_string
from utils.config_manager import ConfigManager
from config import (
    HEADERS,
    PROXY_LIST,
    SEARCH_CACHE_FILE,
    LOG_FILE,
    DEFAULT_CONFIG,
    CACHE_DIR,
    LOG_DIR,
)


def parse_args() -> argparse.Namespace:
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description="百度搜索结果抓取工具")
    parser.add_argument(
        "query", nargs="?", help="搜索关键词（如果未指定，将在运行时提示输入）"
    )
    parser.add_argument(
        "-p", "--pages", type=int, default=1, help="抓取的页数（默认：1）"
    )
    parser.add_argument(
        "-o", "--output", type=str, help="输出文件路径（默认：自动生成）"
    )
    parser.add_argument(
        "--no-save-results", action="store_true", help="不保存搜索结果到文件"
    )
    parser.add_argument(
        "--cache-dir", type=str, default=str(CACHE_DIR), help="缓存目录路径"
    )
    parser.add_argument("--cache-file", type=str, help="URL缓存文件路径")
    parser.add_argument("--no-cache", action="store_true", help="不使用URL缓存")
    parser.add_argument("--clear-cache", action="store_true", help="清除现有缓存")
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="日志级别",
    )
    parser.add_argument("--log-file", type=str, help="日志文件路径")
    parser.add_argument(
        "--no-log-console", action="store_true", help="不在控制台显示日志"
    )
    parser.add_argument("--no-log-file", action="store_true", help="不将日志写入文件")
    parser.add_argument(
        "--concurrent",
        type=int,
        help=f"并发请求数（默认：{DEFAULT_CONFIG['max_semaphore']}）",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        help=f"请求超时时间（秒）（默认：{DEFAULT_CONFIG['timeout']}）",
    )
    parser.add_argument(
        "--retries",
        type=int,
        help=f"请求失败重试次数（默认：{DEFAULT_CONFIG['retries']}）",
    )
    parser.add_argument("--proxy", action="store_true", help="为所有请求使用代理")
    return parser.parse_args()


def get_scraper_config(
    args: argparse.Namespace,
    config_manager: ConfigManager,
    log_to_console: bool,
    log_file_path: Optional[Path],
) -> Dict[str, Any]:
    """根据命令行参数生成爬虫配置"""
    # 尝试从配置文件加载配置，如果不存在则使用默认配置
    scraper_config = config_manager.load_config("scraper")
    if not scraper_config:
        scraper_config = {}

    # 使用默认配置填充缺失的值
    config = DEFAULT_CONFIG.copy()

    # 优先使用命令行参数
    if args.concurrent:
        config["max_semaphore"] = args.concurrent
    if args.timeout:
        config["timeout"] = args.timeout
    if args.retries:
        config["retries"] = args.retries

    # 获取日志级别
    log_level = get_log_level_from_string(args.log_level)

    # 构建最终配置
    return {
        "headers": HEADERS,
        "proxies": PROXY_LIST,
        "use_proxy": bool(args.proxy),
        "max_semaphore": config["max_semaphore"],
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


def get_output_file(
    args: argparse.Namespace, config_manager: ConfigManager, query: str
) -> Path:
    """确定输出文件路径"""
    if args.output:
        output_path = Path(args.output)
    else:
        # 获取缓存目录，如果配置中没有则使用命令行参数或默认值
        cache_dir = args.cache_dir
        if not cache_dir:
            paths_config = config_manager.load_config("paths")
            cache_dir = paths_config.get("cache_dir", str(CACHE_DIR))

        safe_query = "".join(c if c.isalnum() else "_" for c in query)
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        output_path = Path(cache_dir) / f"baidu_search_{safe_query}_{timestamp}.json"

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
    """执行搜索并返回结果"""
    try:
        logger.info(f"开始搜索 '{query}'，页数：{pages}")
        results = await scraper.scrape(
            query=query,
            num_pages=pages,
            no_a_title_tag_strip_n=50,
            cache_to_file=use_cache,
            cache_file=cache_file,
        )
        logger.info(f"搜索完成，获取到 {len(results)} 个结果")
        return results
    except KeyboardInterrupt:
        logger.warning("搜索被用户中断")
        return []
    except Exception as e:
        logger.error(f"搜索过程中发生错误：{str(e)}")
        return []


async def main():
    """主函数"""
    # 初始化配置管理器
    config_manager = ConfigManager()

    try:
        args = parse_args()

        # 在命令行参数不足时交互式询问用户
        save_results = not args.no_save_results
        log_to_file = not args.no_log_file
        log_to_console = not args.no_log_console

        if not args.query:
            query = input("请输入搜索关键词: ").strip()
            if not query:
                print("错误: 未提供搜索关键词，程序退出")
                return
            if not args.no_save_results and not args.output:
                save_choice = input("是否保存搜索结果到文件? (y/[n]): ").strip().lower()
                save_results = save_choice == "y"
            if not args.no_log_file and not args.log_file:
                log_file_choice = input("是否将日志写入文件? (y/[n]): ").strip().lower()
                log_to_file = log_file_choice == "y"
            if not args.no_log_console:
                log_console_choice = (
                    input("是否在控制台显示日志? (y/[n]): ").strip().lower()
                )
                log_to_console = log_console_choice == "y"
        else:
            query = args.query

        # 设置主日志记录器，确保在所有操作之前完成配置
        log_file_path = None
        if log_to_file:
            # 尝试从配置获取日志文件路径，如果没有则使用命令行参数或默认值
            if args.log_file:
                log_file_path = Path(args.log_file)
            else:
                paths_config = config_manager.load_config("paths")
                log_file_path = Path(paths_config.get("log_file", str(LOG_FILE)))

            log_file_path.parent.mkdir(parents=True, exist_ok=True)  # 确保日志目录存在

        logger = setup_logger(
            "baidu_scraper_main",
            get_log_level_from_string(args.log_level),
            log_file_path,
            log_to_console,
        )

        logger.info("百度搜索结果抓取工具启动")

        # 确定缓存文件路径
        if args.cache_file:
            cache_file = Path(args.cache_file)
        else:
            paths_config = config_manager.load_config("paths")
            cache_dir = Path(
                args.cache_dir or paths_config.get("cache_dir", str(CACHE_DIR))
            )
            cache_dir.mkdir(parents=True, exist_ok=True)
            cache_file = cache_dir / "url_cache.json"

        # 确定输出文件路径
        output_file = None
        if save_results:
            output_file = get_output_file(args, config_manager, query)
            logger.info(f"搜索结果将保存到: {output_file}")

        # 创建和配置爬虫
        scraper_config = get_scraper_config(
            args, config_manager, log_to_console, log_file_path
        )
        scraper = BaiduScraper(**scraper_config)

        # 清除缓存（如果需要）
        if args.clear_cache:
            logger.info("清除URL缓存")
            scraper.url_cache.clear()

        # 执行搜索
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
            logger.error(f"网络请求错误: {e}")
            # 降级处理 - 尝试使用缓存中的结果
            if not args.no_cache and cache_file.exists():
                logger.warning("尝试从缓存加载部分结果...")
                # 可以添加从缓存恢复的逻辑
            results = []
        except Exception as e:
            logger.error(f"搜索过程中发生未知错误: {str(e)}", exc_info=True)
            results = []

        # 保存搜索结果
        if results and save_results and output_file:
            success = await save_search_results(
                results=results,
                file_path=output_file,
                save_timestamp=True,
                logger=logger,
            )
            if success:
                logger.info(f"搜索结果已保存到: {output_file}")
            else:
                logger.error("保存搜索结果失败")
        elif results and not save_results:
            logger.info("根据用户设置，搜索结果未保存到文件")
        else:
            logger.warning("无搜索结果可保存")

        # 输出统计信息
        stats = scraper.get_stats()
        logger.info("爬虫统计信息:")
        logger.info(f"- 总请求数: {stats['total']}")
        logger.info(f"- 成功请求数: {stats['success']}")
        logger.info(f"- 失败请求数: {stats['failed']}")
        logger.info(f"- 成功率: {stats['success_rate']*100:.2f}%")
        logger.info(f"- 运行时间: {stats['duration']:.2f}秒")

        if "cache" in stats:
            cache_stats = stats["cache"]
            logger.info("缓存统计信息:")
            logger.info(f"- 缓存大小: {cache_stats['size']}/{cache_stats['max_size']}")
            logger.info(f"- 缓存命中: {cache_stats['hits']}")
            logger.info(f"- 缓存未命中: {cache_stats['misses']}")
            if cache_stats["hits"] + cache_stats["misses"] > 0:
                logger.info(f"- 缓存命中率: {cache_stats['hit_rate']*100:.2f}%")

    except Exception as e:
        print(f"程序执行过程中发生错误: {str(e)}")
        if logging.getLogger().hasHandlers():
            logging.getLogger().error(
                f"程序执行过程中发生错误: {str(e)}", exc_info=True
            )
        return 1  # 返回错误代码
    return 0  # 正常退出


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n程序被用户中断")
