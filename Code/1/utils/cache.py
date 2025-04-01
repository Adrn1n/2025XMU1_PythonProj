import time
from typing import Dict, Optional, Any, List, Tuple
import json
from pathlib import Path
import logging


class URLCache:
    """高效的URL解析结果缓存类"""

    def __init__(self, max_size: int = 1000, ttl: int = 86400):
        """
        初始化缓存

        Args:
            max_size: 最大缓存项数量
            ttl: 缓存项生存时间（秒）
        """
        self.cache: Dict[str, Tuple[str, float]] = {}  # (url, timestamp)
        self.max_size = max_size
        self.ttl = ttl
        self.hits = 0
        self.misses = 0
        self.logger = logging.getLogger("URLCache")

    def get(self, url: str) -> Optional[str]:
        """获取缓存的URL"""
        self._clean_expired()

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

    def set(self, org_url: str, real_url: str) -> None:
        """设置缓存"""
        self._clean_expired()

        # 检查缓存是否已满
        if len(self.cache) >= self.max_size:
            self._evict_entries()

        self.cache[org_url] = (real_url, time.time())

    def _clean_expired(self) -> None:
        """清理过期的缓存项"""
        now = time.time()
        expired_keys = [
            k for k, (_, timestamp) in self.cache.items() if now - timestamp > self.ttl
        ]

        for key in expired_keys:
            del self.cache[key]

        if expired_keys and self.logger:
            self.logger.debug(f"清理了 {len(expired_keys)} 个过期缓存项")

    def _evict_entries(self) -> None:
        """删除部分缓存项以腾出空间"""
        if not self.cache:
            return

        # 按访问时间排序，删除最旧的20%
        items = list(self.cache.items())
        items.sort(key=lambda x: x[1][1])  # 按时间戳排序

        to_remove = items[: max(1, len(items) // 5)]  # 至少删除1个，最多删除20%

        for key, _ in to_remove:
            del self.cache[key]

        if self.logger:
            self.logger.debug(f"缓存空间不足，删除了 {len(to_remove)} 个最旧的缓存项")

    def clear(self) -> None:
        """清空缓存"""
        self.cache.clear()
        self.hits = 0
        self.misses = 0
        if self.logger:
            self.logger.debug("缓存已清空")

    def stats(self) -> Dict[str, Any]:
        """返回缓存统计信息"""
        total_requests = self.hits + self.misses
        hit_rate = self.hits / total_requests if total_requests > 0 else 0

        return {
            "size": len(self.cache),
            "max_size": self.max_size,
            "ttl": self.ttl,
            "hits": self.hits,
            "misses": self.misses,
            "hit_rate": hit_rate,
        }

    def save_to_file(self, file_path: Path) -> bool:
        """保存缓存到文件"""
        try:
            # 只保存未过期的缓存项
            self._clean_expired()

            # 将缓存转换为可序列化的格式
            serializable_cache = {
                url: {"real_url": real_url, "timestamp": timestamp}
                for url, (real_url, timestamp) in self.cache.items()
            }

            with file_path.open("w", encoding="utf-8") as f:
                json.dump(serializable_cache, f, ensure_ascii=False, indent=2)

            if self.logger:
                self.logger.info(f"已将 {len(self.cache)} 个缓存项保存到 {file_path}")
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
