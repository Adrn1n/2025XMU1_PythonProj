import json
import os
from pathlib import Path
from typing import Dict, Any, Optional, Union, List
import logging

logger = logging.getLogger(__name__)


class ConfigManager:
    """配置管理器，用于统一管理项目配置"""

    def __init__(self, config_dir: Union[str, Path] = "config"):
        self.config_dir = Path(config_dir)
        self.config_cache: Dict[str, Any] = {}

    def load_config(self, name: str) -> Dict[str, Any]:
        """
        加载配置文件

        Args:
            name: 配置名称（不含.json后缀）

        Returns:
            配置字典
        """
        if name in self.config_cache:
            return self.config_cache[name]

        config_path = self.config_dir / f"{name}.json"
        if not config_path.exists():
            logger.warning(f"配置文件不存在: {config_path}")
            return {}

        try:
            with open(config_path, "r", encoding="utf-8") as f:
                config = json.load(f)
            self.config_cache[name] = config
            return config
        except Exception as e:
            logger.error(f"加载配置文件失败: {e}")
            return {}

    def save_config(self, name: str, config: Dict[str, Any]) -> bool:
        """
        保存配置到文件

        Args:
            name: 配置名称
            config: 配置字典

        Returns:
            是否保存成功
        """
        self.config_dir.mkdir(parents=True, exist_ok=True)
        config_path = self.config_dir / f"{name}.json"

        try:
            with open(config_path, "w", encoding="utf-8") as f:
                json.dump(config, f, ensure_ascii=False, indent=2)
            self.config_cache[name] = config
            return True
        except Exception as e:
            logger.error(f"保存配置文件失败: {e}")
            return False

    def get(self, name: str, key: str, default: Any = None) -> Any:
        """
        获取配置项

        Args:
            name: 配置名称
            key: 配置项键
            default: 默认值

        Returns:
            配置值
        """
        config = self.load_config(name)
        return config.get(key, default)

    def set(self, name: str, key: str, value: Any) -> bool:
        """
        设置配置项

        Args:
            name: 配置名称
            key: 配置项键
            value: 配置值

        Returns:
            是否保存成功
        """
        config = self.load_config(name)
        config[key] = value
        return self.save_config(name, config)
