import logging
from typing import Any, Dict, List, Optional, Union
from pathlib import Path
import json

# 创建日志记录器
logger = logging.getLogger(__name__)

# 默认配置模板
DEFAULT_CONFIG_TEMPLATES = {
    "paths": {
        "config_dir": "config",
        "cache_dir": "cache",
        "log_dir": "logs",
        "data_dir": "data",
    },
    "scraper": {
        "max_semaphore": 25,
        "batch_size": 25,
        "timeout": 3,
        "retries": 0,
        "min_sleep": 0.1,
        "max_sleep": 0.3,
        "max_redirects": 5,
        "cache_size": 1000,
    },
    "logging": {
        "console_level": "INFO",
        "file_level": "DEBUG",
        "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        "date_format": "%Y-%m-%d %H:%M:%S",
    },
    "files": {
        "headers_file": "config/headers.txt",
        "proxy_file": "config/proxy.txt",
        "search_cache_file": "cache/baidu_search_res.json",
        "log_file": "logs/scraper.log",
    },
}

# 默认HTTP请求头模板
HEADERS_TEMPLATE = """
GET / HTTP/1.1
Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7
Accept-Encoding: gzip, deflate, br, zstd
Accept-Language: en-US,en;q=0.9
Cache-Control: max-age=0
Connection: keep-alive
Cookie:
DNT: 1
Host: www.baidu.com
Sec-Fetch-Dest: document
Sec-Fetch-Mode: navigate
Sec-Fetch-Site: none
Sec-Fetch-User: ?1
Upgrade-Insecure-Requests: 1
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36
sec-ch-ua: "Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"
sec-ch-ua-mobile: ?0
sec-ch-ua-platform: "Windows"

"""


class ConfigManager:
    """配置管理器，用于统一管理项目配置"""

    def __init__(
        self, config_dir: Union[str, Path] = "config", create_if_missing: bool = True
    ):
        """
        初始化配置管理器

        Args:
            config_dir: 配置目录路径
            create_if_missing: 配置目录不存在时是否创建
        """
        self.config_dir = Path(config_dir)
        self.config_cache: Dict[str, Dict[str, Any]] = {}

        # 如果需要且目录不存在，则创建配置目录
        if create_if_missing and not self.config_dir.exists():
            try:
                self.config_dir.mkdir(parents=True, exist_ok=True)
                logger.info(f"创建配置目录: {self.config_dir}")
            except Exception as e:
                logger.error(f"创建配置目录失败: {e}")

    def get_config_path(self, name: str) -> Path:
        """
        获取配置文件路径

        Args:
            name: 配置名称（不含.json后缀）

        Returns:
            配置文件路径
        """
        return self.config_dir / f"{name}.json"

    def config_exists(self, name: str) -> bool:
        """
        检查配置文件是否存在

        Args:
            name: 配置名称

        Returns:
            配置文件是否存在
        """
        return self.get_config_path(name).exists()

    def load_config(
        self, name: str, default: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        加载配置文件

        Args:
            name: 配置名称（不含.json后缀）
            default: 配置不存在时的默认值

        Returns:
            配置字典
        """
        # 如果配置已缓存，直接返回
        if name in self.config_cache:
            return self.config_cache[name]

        # 获取配置文件路径
        config_path = self.get_config_path(name)

        # 如果配置文件不存在，返回默认值
        if not config_path.exists():
            logger.debug(f"配置文件不存在: {config_path}")
            return {} if default is None else default

        # 尝试加载配置
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                config = json.load(f)

            # 缓存并返回配置
            self.config_cache[name] = config
            logger.debug(f"成功加载配置: {name}")
            return config

        except json.JSONDecodeError as e:
            logger.error(f"解析配置文件失败: {config_path}, 错误: {e}")
            return {} if default is None else default
        except Exception as e:
            logger.error(f"加载配置文件失败: {config_path}, 错误: {e}")
            return {} if default is None else default

    def get(self, name: str, key: str, default: Any = None) -> Any:
        """
        获取配置项

        Args:
            name: 配置名称
            key: 配置项键或路径（使用.分隔，如'server.host'）
            default: 默认值

        Returns:
            配置值
        """
        config = self.load_config(name)

        # 处理嵌套键
        if "." in key:
            parts = key.split(".")
            # 递归获取嵌套值
            value = config
            for part in parts:
                if not isinstance(value, dict) or part not in value:
                    return default
                value = value[part]
            return value

        return config.get(key, default)

    def get_all_configs(self) -> List[str]:
        """
        获取所有可用的配置名称

        Returns:
            配置名称列表
        """
        configs = []
        for file in self.config_dir.glob("*.json"):
            configs.append(file.stem)
        return configs

    def deep_merge(
        self, base: Dict[str, Any], override: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        深度合并两个配置字典

        Args:
            base: 基础配置
            override: 要覆盖的配置

        Returns:
            合并后的配置
        """
        result = base.copy()

        for key, value in override.items():
            # 如果两边都是字典，则递归合并
            if (
                key in result
                and isinstance(result[key], dict)
                and isinstance(value, dict)
            ):
                result[key] = self.deep_merge(result[key], value)
            else:
                # 否则直接覆盖
                result[key] = value

        return result

    def save_config(
        self, name: str, config: Dict[str, Any], merge: bool = False
    ) -> bool:
        """
        保存配置到文件

        Args:
            name: 配置名称
            config: 配置字典
            merge: 是否合并现有配置

        Returns:
            是否保存成功
        """
        # 确保配置目录存在
        self.config_dir.mkdir(parents=True, exist_ok=True)

        # 获取配置文件路径
        config_path = self.get_config_path(name)

        # 如果需要合并，先加载现有配置
        if merge and config_path.exists():
            try:
                with open(config_path, "r", encoding="utf-8") as f:
                    existing_config = json.load(f)
                # 深度合并配置
                merged_config = self.deep_merge(existing_config, config)
                config = merged_config
            except Exception as e:
                logger.warning(f"合并配置失败: {e}, 将直接覆盖")

        # 保存配置
        try:
            with open(config_path, "w", encoding="utf-8") as f:
                json.dump(config, f, ensure_ascii=False, indent=2)

            # 更新缓存
            self.config_cache[name] = config
            logger.debug(f"成功保存配置: {name}")
            return True

        except Exception as e:
            logger.error(f"保存配置文件失败: {config_path}, 错误: {e}")
            return False

    def set(self, name: str, key: str, value: Any, create_parents: bool = True) -> bool:
        """
        设置配置项

        Args:
            name: 配置名称
            key: 配置项键或路径（使用.分隔，如'server.host'）
            value: 配置值
            create_parents: 是否创建不存在的父级配置

        Returns:
            是否保存成功
        """
        config = self.load_config(name)

        # 处理嵌套键
        if "." in key:
            parts = key.split(".")
            last_key = parts.pop()

            # 递归查找或创建父级配置
            current = config
            for part in parts:
                if part not in current or not isinstance(current[part], dict):
                    if create_parents:
                        current[part] = {}
                    else:
                        logger.error(f"父级配置不存在: {part}")
                        return False
                current = current[part]

            current[last_key] = value
        else:
            config[key] = value

        return self.save_config(name, config)

    def ensure_default_configs(self) -> bool:
        """
        确保所有默认配置文件存在，不存在则创建

        Returns:
            是否成功创建所有配置
        """
        success = True

        # 创建基础目录
        paths_config = DEFAULT_CONFIG_TEMPLATES["paths"]
        for dir_name in paths_config.values():
            path = Path(dir_name)
            if not path.exists():
                try:
                    path.mkdir(parents=True, exist_ok=True)
                    logger.debug(f"创建目录: {path}")
                except Exception as e:
                    logger.error(f"创建目录失败: {path}, 错误: {e}")
                    success = False

        # 写入配置文件
        for name, config in DEFAULT_CONFIG_TEMPLATES.items():
            if not self.config_exists(name):
                if not self.save_config(name, config):
                    logger.error(f"创建默认配置失败: {name}")
                    success = False
                else:
                    logger.debug(f"已创建默认配置: {name}")
            else:
                logger.debug(f"配置已存在，跳过创建: {name}")

        # 创建默认的headers.txt
        headers_path = Path(paths_config["config_dir"]) / "headers.txt"
        if not headers_path.exists():
            try:
                with open(headers_path, "w", encoding="utf-8") as f:
                    f.write(HEADERS_TEMPLATE)
                logger.debug(f"已创建默认headers文件: {headers_path}")
            except Exception as e:
                logger.error(f"创建headers文件失败: {headers_path}, 错误: {e}")
                success = False

        # 创建空的代理文件
        proxy_path = Path(paths_config["config_dir"]) / "proxy.txt"
        if not proxy_path.exists():
            try:
                with open(proxy_path, "w", encoding="utf-8") as f:
                    f.write(
                        "# 每行一个代理，格式: http://host:port 或 https://host:port\n"
                    )
                logger.debug(f"已创建默认proxy文件: {proxy_path}")
            except Exception as e:
                logger.error(f"创建proxy文件失败: {proxy_path}, 错误: {e}")
                success = False

        return success

    def delete(self, name: str, key: Optional[str] = None) -> bool:
        """
        删除配置项或整个配置文件

        Args:
            name: 配置名称
            key: 要删除的配置项键（None表示删除整个配置文件）

        Returns:
            是否删除成功
        """
        if key is None:
            # 删除整个配置文件
            config_path = self.get_config_path(name)
            if not config_path.exists():
                return True

            try:
                config_path.unlink()
                if name in self.config_cache:
                    del self.config_cache[name]
                logger.debug(f"删除配置文件: {config_path}")
                return True
            except Exception as e:
                logger.error(f"删除配置文件失败: {config_path}, 错误: {e}")
                return False
        else:
            # 删除特定配置项
            config = self.load_config(name)
            if not config:
                return True

            if "." in key:
                # 处理嵌套键
                parts = key.split(".")
                last_key = parts.pop()

                # 递归查找父级配置
                current = config
                for part in parts:
                    if part not in current or not isinstance(current[part], dict):
                        return True  # 父级不存在，视为删除成功
                    current = current[part]

                if last_key in current:
                    del current[last_key]
            else:
                if key in config:
                    del config[key]

            return self.save_config(name, config)

    def clear_cache(self, name: Optional[str] = None) -> None:
        """
        清除配置缓存

        Args:
            name: 要清除的配置名称（None表示清除所有）
        """
        if name is None:
            self.config_cache.clear()
            logger.debug("清除所有配置缓存")
        elif name in self.config_cache:
            del self.config_cache[name]
            logger.debug(f"清除配置缓存: {name}")
