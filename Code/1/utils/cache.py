from typing import Dict, Optional, Any, Tuple
import logging
import time
from pathlib import Path
import json


class URLCache:
    """高效的URL解析结果缓存类"""

    def __init__(
        self,
        max_size: int = 1000,
        ttl: int = 24 * 60 * 60,
        cleanup_threshold: int = 100,
    ):
        """
        初始化缓存

        Args:
            max_size: 最大缓存项数量
            ttl: 缓存项生存时间（秒）
            cleanup_threshold: 多少次操作后触发清理
        """
        self.cache: Dict[str, Tuple[str, float]] = {}  # (url, timestamp)
        self.max_size = max_size
        self.ttl = ttl
        self.cleanup_threshold = cleanup_threshold
        self.hits = 0
        self.misses = 0
        self.operations_count = 0
        self.logger = logging.getLogger("URLCache")

    def stats(self) -> Dict[str, Any]:
        """返回缓存统计信息"""
        total_reqs = self.hits + self.misses
        hit_rate = self.hits / total_reqs if total_reqs > 0 else 0

        return {
            "size": len(self.cache),
            "max_size": self.max_size,
            "ttl": self.ttl,
            "hits": self.hits,
            "misses": self.misses,
            "hit_rate": hit_rate,
        }

    def clean_expired(self) -> None:
        """清理过期的缓存项"""
        now = time.time()
        expired_keys = []

        for k, cache_entry in self.cache.items():
            try:
                if isinstance(cache_entry, tuple) and len(cache_entry) == 2:
                    _, timestamp = cache_entry
                    if now - timestamp > self.ttl:
                        expired_keys.append(k)
                else:
                    # 对于格式不正确的项，将其视为过期项
                    if self.logger:
                        self.logger.debug(
                            f"发现格式不正确的缓存项: {k} -> {cache_entry}, 将其标记为过期"
                        )
                    expired_keys.append(k)
            except Exception as e:
                if self.logger:
                    self.logger.warning(f"处理缓存项时出错: {e}, 将其标记为过期: {k}")
                expired_keys.append(k)

        for key in expired_keys:
            del self.cache[key]

        if expired_keys and self.logger:
            self.logger.debug(f"清理了 {len(expired_keys)} 个过期缓存项")

    def maybe_clean_expired(self) -> None:
        """按需清理过期缓存项"""
        # 每隔cleanup_threshold次操作清理一次
        if self.operations_count >= self.cleanup_threshold:
            self.clean_expired()
            self.operations_count = 0

    def evict_entries(self, max_percent=20) -> None:
        """删除部分缓存项以腾出空间"""
        if not self.cache:
            return

        items = list(self.cache.items())
        items.sort(key=lambda x: x[1][1])  # 按时间戳排序

        to_remove = items[: max(1, len(items) * max_percent // 100)]

        for key, _ in to_remove:
            del self.cache[key]

        if self.logger:
            self.logger.debug(f"缓存空间不足，删除了 {len(to_remove)} 个最旧的缓存项")

    def set(self, org_url: str, real_url: str) -> None:
        """设置缓存"""
        self.operations_count += 1
        self.maybe_clean_expired()

        # 检查缓存是否已满
        if len(self.cache) >= self.max_size:
            self.evict_entries()

        self.cache[org_url] = (real_url, time.time())

    def get(self, url: str) -> Optional[str]:
        """获取缓存的URL"""
        self.operations_count += 1
        self.maybe_clean_expired()

        if url in self.cache:
            cached_url, timestamp = self.cache[url]
            # 检查是否过期
            if time.time() - timestamp <= self.ttl:
                self.hits += 1
                return cached_url
            else:
                # 过期了，删除并返回None
                del self.cache[url]

        self.misses += 1
        return None

    def clear(self) -> None:
        """清空缓存"""
        self.cache.clear()
        self.hits = 0
        self.misses = 0
        if self.logger:
            self.logger.debug("缓存已清空")

    def save_to_file(self, file_path: Path) -> bool:
        """保存缓存到文件"""
        try:
            # 只保存未过期的缓存项
            self.clean_expired()

            # 将缓存转换为可序列化的格式
            serializable_cache = {}
            for url, cache_entry in self.cache.items():
                # 处理缓存项可能是元组或者直接是URL字符串的情况
                if isinstance(cache_entry, tuple) and len(cache_entry) == 2:
                    real_url, timestamp = cache_entry
                    serializable_cache[url] = {
                        "real_url": real_url,
                        "timestamp": timestamp,
                    }
                elif isinstance(cache_entry, str):
                    # 如果缓存项是字符串，则直接将其作为real_url，并使用当前时间作为timestamp
                    serializable_cache[url] = {
                        "real_url": cache_entry,
                        "timestamp": time.time(),
                    }
                else:
                    # 跳过无法识别的格式
                    if self.logger:
                        self.logger.warning(
                            f"跳过无法识别的缓存项格式: {url} -> {cache_entry}"
                        )
                    continue

            with file_path.open("w", encoding="utf-8") as f:
                json.dump(serializable_cache, f, ensure_ascii=False, indent=2)

            if self.logger:
                self.logger.info(
                    f"已将 {len(serializable_cache)} 个缓存项保存到 {file_path}"
                )
            return True

        except Exception as e:
            if self.logger:
                self.logger.error(f"保存缓存到文件失败: {e}")
            return False

    def load_from_file(self, file_path: Path) -> bool:
        """从文件加载缓存"""
        try:
            if not file_path.exists():
                if self.logger:
                    self.logger.warning(f"缓存文件不存在: {file_path}")
                return False

            with file_path.open("r", encoding="utf-8") as f:
                data = json.load(f)

            # 转换回内部格式并过滤过期项
            now = time.time()
            loaded_count = 0

            for url, item in data.items():
                real_url = item["real_url"]
                timestamp = item["timestamp"]

                # 只加载未过期的项
                if now - timestamp <= self.ttl:
                    self.cache[url] = (real_url, timestamp)
                    loaded_count += 1

            if self.logger:
                self.logger.info(
                    f"从 {file_path} 加载了 {loaded_count} 个缓存项（跳过 {len(data) - loaded_count} 个过期项）"
                )
            return True

        except json.JSONDecodeError as e:
            if self.logger:
                self.logger.error(f"解析缓存文件失败: {e}")
            return False
        except Exception as e:
            if self.logger:
                self.logger.error(f"加载缓存文件失败: {e}")
            return False
