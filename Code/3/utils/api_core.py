"""
API utilities for key management, validation, and OpenAI compatibility.
Provides centralized API functionality with caching and validation.
"""

import json
import logging
import secrets
import string
import time
from pathlib import Path
from typing import Any, Dict, Optional, Set, Tuple, Union

from utils.config_manager import OptimizedConfigManager


class APIKeyManager:
    """API key management with caching and configuration integration."""

    def __init__(self, config_manager: Optional[OptimizedConfigManager] = None):
        self.config_manager = config_manager or OptimizedConfigManager()
        self.logger = self._setup_logger()
        self._cache: Optional[Set[str]] = None
        self._cache_time: Optional[float] = None
        self._cache_ttl = 300

    @staticmethod
    def _setup_logger() -> logging.Logger:
        """Setup module-specific logger."""
        try:
            from config import get_module_logger

            return get_module_logger("api_core")
        except ImportError:
            return logging.getLogger("APIKeyManager")

    @property
    def api_config(self) -> Dict[str, Any]:
        """Get API configuration from config manager."""
        return self.config_manager.load_config("api", {})

    @property
    def api_keys_file(self) -> Path:
        """Get API keys file path."""
        api_dir = Path(self.config_manager.get_config_path("api").parent)
        api_dir.mkdir(exist_ok=True)
        return api_dir / self.api_config.get("api_keys_file", "api_keys.txt")

    @staticmethod
    def generate_api_key(prefix: str = "sk-ollama", length: int = 32) -> str:
        """Generate new API key with specified prefix and length."""
        alphabet = string.ascii_letters + string.digits
        random_part = "".join(secrets.choice(alphabet) for _ in range(length))
        return f"{prefix}-{random_part}"

    def _is_cache_valid(self) -> bool:
        """Check if cached API keys are still valid."""
        if self._cache is None or self._cache_time is None:
            return False
        return time.time() - self._cache_time < self._cache_ttl

    def _update_cache(self, keys: Set[str]) -> None:
        """Update the cache with new keys."""
        self._cache = keys.copy()
        self._cache_time = time.time()

    def ensure_api_keys_file(self) -> bool:
        """Create API keys file if it doesn't exist."""
        try:
            if self.api_keys_file.exists():
                return True

            self.api_keys_file.parent.mkdir(parents=True, exist_ok=True)

            with open(self.api_keys_file, "w", encoding="utf-8") as f:
                f.write("# API Keys Configuration\n")
                f.write("# One key per line, lines starting with # are comments\n")
                f.write("# Add your API keys below:\n\n")
                f.write("# Example:\n")
                f.write("# sk-your-api-key-here\n")
                f.write("# ollama-local-key-example\n\n")
                f.write("# Add your actual API keys here\n")

            self.logger.info(f"Created API keys file template: {self.api_keys_file}")
            return True

        except (IOError, OSError, PermissionError) as e:
            self.logger.error(f"Failed to create API keys file: {e}")
            return False

    def load_api_keys(self, use_cache: bool = True) -> Set[str]:
        """Load API keys from file with caching support."""
        if use_cache and self._is_cache_valid():
            self.logger.debug("Using cached API keys")
            return self._cache.copy()

        self.ensure_api_keys_file()
        keys = self.load_api_keys_from_file(self.api_keys_file)

        if self.api_keys_file.exists():
            if keys:
                self.logger.info(f"Loaded {len(keys)} API keys from file")
            else:
                self.logger.warning(f"No valid API keys found in {self.api_keys_file}")
        else:
            self.logger.error(f"API keys file not found: {self.api_keys_file}")

        self._update_cache(keys)
        return keys

    @staticmethod
    def load_api_keys_from_file(file_path: Path) -> Set[str]:
        """Load API keys from text file, filtering comments and empty lines."""
        try:
            if not file_path.exists():
                return set()

            lines = [
                line.strip()
                for line in file_path.read_text(encoding="utf-8").splitlines()
                if line.strip() and not line.strip().startswith("#")
            ]

            return set(lines)
        except (IOError, OSError, UnicodeDecodeError):
            # Return empty set on file read errors
            return set()

    def save_api_keys(self, keys: Set[str]) -> bool:
        """Save API keys to file and update cache."""
        try:
            self.api_keys_file.parent.mkdir(parents=True, exist_ok=True)

            with open(self.api_keys_file, "w", encoding="utf-8") as f:
                f.write("# API Keys Configuration\n")
                f.write("# One key per line, lines starting with # are comments\n\n")

                # Write all keys
                if keys:
                    f.write("# API Keys\n")
                    for key in sorted(keys):
                        f.write(f"{key}\n")
                else:
                    f.write("# No API keys currently configured\n")
                    f.write("# Add your API keys here\n")

            self._update_cache(keys)
            self.logger.info(f"Saved {len(keys)} API keys to {self.api_keys_file}")
            return True

        except (IOError, OSError, PermissionError) as e:
            self.logger.error(f"Failed to save API keys: {e}")
            return False

    def add_api_key(self, new_key: str) -> Tuple[bool, str]:
        """Add a new API key. Returns (success, message)."""
        if not new_key or not new_key.strip():
            return False, "API key cannot be empty"

        new_key = new_key.strip()
        keys = self.load_api_keys()

        if new_key in keys:
            return False, f"API key already exists: {new_key}"

        keys.add(new_key)
        if self.save_api_keys(keys):
            return True, f"Successfully added API key: {new_key}"
        return False, "Failed to save API key"

    def remove_api_key(self, key_to_remove: str) -> Tuple[bool, str]:
        """Remove an API key. Returns (success, message)."""
        keys = self.load_api_keys()

        if key_to_remove not in keys:
            return False, f"API key not found: {key_to_remove}"

        keys.remove(key_to_remove)
        if self.save_api_keys(keys):
            return True, f"Successfully removed API key: {key_to_remove}"
        return False, "Failed to save changes"

    def validate_api_key(self, key: str) -> bool:
        """Validate if an API key is valid."""
        if not key:
            return False
        return key in self.load_api_keys()

    def get_stats(self) -> Dict[str, Any]:
        """Get API key statistics."""
        keys = self.load_api_keys()

        return {
            "total_keys": len(keys),
            "cache_valid": self._is_cache_valid(),
            "file_exists": self.api_keys_file.exists(),
        }

    def clear_cache(self) -> None:
        """Clear the API key cache."""
        self._cache = None
        self._cache_time = None


class OpenAICompatibilityUtils:
    """Utilities for OpenAI API compatibility."""

    @staticmethod
    def create_chat_completion_response(
        model: str,
        content: str,
        message_id: Optional[str] = None,
        finish_reason: str = "stop",
        usage_tokens: Optional[Dict[str, int]] = None,
    ) -> Dict[str, Any]:
        """Create a complete chat completion response."""
        response_id = message_id or f"chatcmpl-{secrets.token_hex(8)}"
        usage = usage_tokens or {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
        }

        return {
            "id": response_id,
            "object": "chat.completion",
            "created": int(time.time()),
            "model": model,
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": content},
                    "finish_reason": finish_reason,
                }
            ],
            "usage": usage,
        }

    @staticmethod
    def create_chat_completion_chunk(
        model: str,
        delta_content: str = "",
        message_id: Optional[str] = None,
        finish_reason: Optional[str] = None,
    ) -> str:
        """Create a streaming chat completion chunk as JSON string."""
        response_id = message_id or f"chatcmpl-{secrets.token_hex(8)}"

        delta = {}
        if delta_content:
            delta["content"] = delta_content

        chunk_data = {
            "id": response_id,
            "object": "chat.completion.chunk",
            "created": int(time.time()),
            "model": model,
            "choices": [{"index": 0, "delta": delta, "finish_reason": finish_reason}],
        }

        return json.dumps(chunk_data, ensure_ascii=False)

    @staticmethod
    def create_error_response(
        error_message: str, error_code: str = "internal_error"
    ) -> Dict[str, Any]:
        """Create a standardized error response."""
        return {
            "error": {
                "message": error_message,
                "type": error_code,
            }
        }


# Global API key manager instance
_api_key_manager: Optional[APIKeyManager] = None


def get_api_key_manager() -> APIKeyManager:
    """Get the global API key manager instance."""
    global _api_key_manager
    if _api_key_manager is None:
        _api_key_manager = APIKeyManager()
    return _api_key_manager


# Convenience functions
def validate_api_key(key: str) -> bool:
    """Validate an API key using the global manager."""
    return get_api_key_manager().validate_api_key(key)


def load_api_keys() -> Set[str]:
    """Load API keys using the global manager."""
    return get_api_key_manager().load_api_keys()


def generate_api_key(prefix: str = "sk-ollama", length: int = 32) -> str:
    """Generate an API key using the global manager."""
    return get_api_key_manager().generate_api_key(prefix, length)


def create_openai_error_response(
    message: str, error_type: str = "internal_error"
) -> Dict[str, Any]:
    """Create OpenAI-compatible error response."""
    return OpenAICompatibilityUtils.create_error_response(message, error_type)


def load_api_keys_from_file(file_path: Union[str, Path]) -> Set[str]:
    """Load API keys from a file, similar to how headers are loaded."""
    return APIKeyManager.load_api_keys_from_file(Path(file_path))
