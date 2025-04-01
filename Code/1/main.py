import asyncio
import argparse
import logging
import sys
import time
from pathlib import Path
from typing import Dict, Any, Optional, List

from scrapers.baidu_scraper import BaiduScraper
from utils.file_utils import save_search_results
from utils.logging_utils import setup_logger, get_log_level_from_string
from config import (
    HEADERS,
    COOKIES,
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

    # 基本参数
    parser.add_argument(
        "query", nargs="?", help="搜索关键词（如果未指定，将在运行时提示输入）"
    )
    parser.add_argument(
        "-p", "--pages", type=int, default=1, help="抓取的页数（默认：1）"
    )
    parser.add_argument(
        "-o", "--output", type=str, help="输出文件路径（默认：自动生成）"
    )

    # 缓存相关
    parser.add_argument(
        "--cache-dir", type=str, default=str(CACHE_DIR), help="缓存目录路径"
    )
    parser.add_argument("--cache-file", type=str, help="URL缓存文件路径")
    parser.add_argument("--no-cache", action="store_true", help="不使用URL缓存")
    parser.add_argument("--clear-cache", action="store_true", help="清除现有缓存")

    # 日志相关
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

    # 性能调优
    parser.add_argument(
        "--concurrent",
        type=int,
        help=f"并发请求数（默认：{DEFAULT_CONFIG['semaphore_limit']}）",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        help=f"请求超时时间（秒）（默认：{DEFAULT_CONFIG['fetch_timeout']}）",
    )
    parser.add_argument(
        "--retries",
        type=int,
        help=f"请求失败重试次数（默认：{DEFAULT_CONFIG['fetch_retries']}）",
    )

    # 代理相关
    parser.add_argument("--proxy", action="store_true", help="为所有请求使用代理")

    return parser.parse_args()


def get_scraper_config(args: argparse.Namespace) -> Dict[str, Any]:
    """根据命令行参数生成爬虫配置"""
    config = DEFAULT_CONFIG.copy()

    # 更新并发数
    if args.concurrent:
        config["semaphore_limit"] = args.concurrent

    # 更新超时时间
    if args.timeout:
        config["fetch_timeout"] = args.timeout

    # 更新重试次数
    if args.retries:
        config["fetch_retries"] = args.retries

    # 处理日志配置
    log_level = get_log_level_from_string(args.log_level)
    log_file = args.log_file or LOG_FILE

    return {
        "headers": HEADERS,
        "cookies": COOKIES,
        "proxies": PROXY_LIST,
        "use_proxy_for_search": bool(args.proxy),
        "semaphore_limit": config["semaphore_limit"],
        "min_delay_between_requests": config["min_delay_between_requests"],
        "max_delay_between_requests": config["max_delay_between_requests"],
        "fetch_timeout": config["fetch_timeout"],
        "fetch_retries": config["fetch_retries"],
        "min_retries_sleep": config["min_retries_sleep"],
        "max_retries_sleep": config["max_retries_sleep"],
        "max_redirects": config["max_redirects"],
        "cache_size": config["cache_size"],
        "enable_logging": True,
        "log_level": log_level,
        "log_file": log_file,
        "log_to_console": not args.no_log_console,
    }


def get_output_file(args: argparse.Namespace, query: str) -> Path:
    """确定输出文件路径"""
    if args.output:
        output_path = Path(args.output)
    else:
        # 自动生成输出文件名
        safe_query = "".join(c if c.isalnum() else "_" for c in query)
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        output_path = (
            Path(args.cache_dir) / f"baidu_search_{safe_query}_{timestamp}.json"
        )

    # 确保输出目录存在
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
        # 执行搜索
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
    # 解析命令行参数
    args = parse_args()

    # 设置主日志记录器
    logger = setup_logger(
        "baidu_scraper_main",
        get_log_level_from_string(args.log_level),
        args.log_file or LOG_FILE,
        not args.no_log_console,
    )

    logger.info("百度搜索结果抓取工具启动")

    # 获取搜索关键词
    query = args.query
    if not query:
        query = input("请输入搜索关键词: ").strip()
        if not query:
            logger.error("未提供搜索关键词，程序退出")
            return

    # 确定缓存文件路径
    if args.cache_file:
        cache_file = Path(args.cache_file)
    else:
        cache_dir = Path(args.cache_dir)
        cache_dir.mkdir(parents=True, exist_ok=True)
        cache_file = cache_dir / "url_cache.json"

    # 确定输出文件路径
    output_file = get_output_file(args, query)
    logger.info(f"搜索结果将保存到: {output_file}")

    # 创建和配置爬虫
    scraper_config = get_scraper_config(args)
    scraper = BaiduScraper(**scraper_config)

    # 清除缓存（如果需要）
    if args.clear_cache:
        logger.info("清除URL缓存")
        scraper.url_cache.clear()

    # 执行搜索
    results = await run_search(
        scraper=scraper,
        query=query,
        pages=args.pages,
        cache_file=cache_file if not args.no_cache else None,
        use_cache=not args.no_cache,
        logger=logger,
    )

    # 保存搜索结果
    if results:
        success = await save_search_results(
            results=results, file_path=output_file, save_timestamp=True, logger=logger
        )

        if success:
            logger.info(f"搜索结果已保存到: {output_file}")
        else:
            logger.error("保存搜索结果失败")
    else:
        logger.warning("无搜索结果可保存")

    # 输出统计信息
    stats = scraper.get_stats()
    logger.info("爬虫统计信息:")
    logger.info(f"- 总请求数: {stats['requests_total']}")
    logger.info(f"- 成功请求数: {stats['requests_success']}")
    logger.info(f"- 失败请求数: {stats['requests_failed']}")
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


if __name__ == "__main__":
    # 处理Windows事件循环策略
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n程序被用户中断")
