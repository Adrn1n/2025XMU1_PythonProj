"""
API utilities for key management, validation, and OpenAI compatibility.
This module provides centralized API-related functionality.
"""

import secrets
import string
import time
import json
import logging
from typing import Set, Dict, Any, Optional, Tuple
from pathlib import Path

from utils.config_manager import OptimizedConfigManager


class APIKeyManager:
    """Optimized API key management with caching and configuration integration."""
    
    def __init__(self, config_manager: Optional[OptimizedConfigManager] = None):
        self.config_manager = config_manager or OptimizedConfigManager()
        self.logger = logging.getLogger("APIKeyManager")
        self._cache: Optional[Set[str]] = None
        self._cache_time: Optional[float] = None
        self._cache_ttl = 300  # 5 minutes
        
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
    
    @property
    def default_api_keys(self) -> Set[str]:
        """Get default API keys from configuration."""
        return set(self.api_config.get("api_keys", []))

    @staticmethod
    def generate_api_key(prefix: str = "sk-ollama", length: int = 32) -> str:
        """Generate a new API key with given prefix and length."""
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
    
    def load_api_keys(self, use_cache: bool = True) -> Set[str]:
        """Load API keys from file with caching support."""
        if use_cache and self._is_cache_valid():
            return self._cache.copy()
        
        keys = self.default_api_keys.copy()
        
        if self.api_keys_file.exists():
            try:
                with open(self.api_keys_file, "r", encoding="utf-8") as f:
                    for line in f:
                        key = line.strip()
                        if key and not key.startswith("#"):
                            keys.add(key)
                self.logger.debug(f"Loaded {len(keys)} API keys from {self.api_keys_file}")
            except Exception as e:
                self.logger.error(f"Failed to load API keys: {e}")
        
        self._update_cache(keys)
        return keys
    
    def save_api_keys(self, keys: Set[str]) -> bool:
        """Save API keys to file and update cache."""
        try:
            self.api_keys_file.parent.mkdir(parents=True, exist_ok=True)
            
            with open(self.api_keys_file, "w", encoding="utf-8") as f:
                f.write("# API Keys Configuration\n")
                f.write("# One key per line, lines starting with # are comments\n\n")
                
                # Write default keys
                f.write("# Default API Keys\n")
                for key in self.default_api_keys:
                    f.write(f"{key}\n")
                
                # Write custom keys
                custom_keys = keys - self.default_api_keys
                if custom_keys:
                    f.write("\n# Custom API Keys\n")
                    for key in sorted(custom_keys):
                        f.write(f"{key}\n")
            
            self._update_cache(keys)
            self.logger.info(f"Saved {len(keys)} API keys to {self.api_keys_file}")
            return True
            
        except Exception as e:
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
        
        if key_to_remove in self.default_api_keys:
            return False, f"Cannot remove default API key: {key_to_remove}"
        
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
        default_keys = self.default_api_keys
        custom_keys = keys - default_keys
        
        return {
            "total_keys": len(keys),
            "default_keys": len(default_keys),
            "custom_keys": len(custom_keys),
            "cache_valid": self._is_cache_valid(),
            "file_exists": self.api_keys_file.exists()
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
        usage_tokens: Optional[Dict[str, int]] = None
    ) -> Dict[str, Any]:
        """Create a complete chat completion response."""
        response_id = message_id or f"chatcmpl-{secrets.token_hex(8)}"
        usage = usage_tokens or {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
        
        return {
            "id": response_id,
            "object": "chat.completion",
            "created": int(time.time()),
            "model": model,
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": content},
                "finish_reason": finish_reason
            }],
            "usage": usage
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
            "choices": [{
                "index": 0,
                "delta": delta,
                "finish_reason": finish_reason
            }]
        }
        
        return json.dumps(chunk_data, ensure_ascii=False)
    
    @staticmethod
    def create_error_response(error_message: str, error_code: str = "internal_error") -> Dict[str, Any]:
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

def create_openai_error_response(message: str, error_type: str = "internal_error") -> Dict[str, Any]:
    """Create OpenAI-compatible error response."""
    return OpenAICompatibilityUtils.create_error_response(message, error_type)
