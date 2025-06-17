"""
API handlers for OpenAI-compatible endpoints.
"""

import time
import uuid
import asyncio
import re
from typing import Any, Dict, List, Optional, AsyncGenerator

from utils.ollama_utils import (
    list_ollama_models,
    check_ollama_status,
    generate_with_ollama,
)
from utils.api_core import OpenAICompatibilityUtils
from config import get_logger, API_CONFIG, OLLAMA_CONFIG

logger = get_logger()

# Simple configuration constants
PERFORMANCE_CONFIG = {
    "max_concurrent_pages": 3,
    "chunk_size_words": 3,
    "streaming_delay": 0.05,
}

SEARCH_CONFIG = {
    "use_search_for_api": True,
    "api_search_pages": 5,
    "api_search_timeout": 5,
    "search_command_enabled": True,
}


class ModelsHandler:
    """Handler for models-related operations."""

    def __init__(self):
        self.cache_ttl = API_CONFIG.get("models_cache_ttl", 300)
        self.ollama_base_url = OLLAMA_CONFIG.get("base_url", "http://localhost:11434")
        self._cached_models: Optional[List[str]] = None
        self._cache_time: Optional[float] = None
        self.compatibility_utils = OpenAICompatibilityUtils()

    def _is_cache_valid(self) -> bool:
        """Check if the models cache is still valid."""
        if self._cached_models is None or self._cache_time is None:
            return False
        return time.time() - self._cache_time < self.cache_ttl

    async def get_available_models(self, force_refresh: bool = False) -> List[str]:
        """Get list of available Ollama models with caching."""
        if not force_refresh and self._is_cache_valid():
            return self._cached_models.copy()

        try:
            if not await check_ollama_status(self.ollama_base_url, logger=logger):
                return self._cached_models or []

            models = await list_ollama_models(self.ollama_base_url, logger=logger)
            self._cached_models = models
            self._cache_time = time.time()
            return models

        except Exception as e:
            logger.error(f"Failed to get available models: {e}")
            return self._cached_models or []

    async def create_models_response(self) -> Dict[str, Any]:
        """Create OpenAI-compatible models response."""
        models = await self.get_available_models()

        model_objects = []
        created_time = int(time.time())

        for model in models:
            model_objects.append(
                {
                    "id": model,
                    "object": "model",
                    "created": created_time,
                    "owned_by": "ollama",
                }
            )

        return {"object": "list", "data": model_objects}


class SearchHandler:
    """Handler for search functionality."""

    def __init__(self):
        self.config = SEARCH_CONFIG
        self.performance_config = PERFORMANCE_CONFIG

    @staticmethod
    def extract_search_query(messages: List[Dict[str, Any]]) -> str:
        """Extract search query from messages."""
        return messages[-1].get("content", "") if messages else ""

    @staticmethod
    def parse_search_command(query: str) -> tuple[str, str, bool]:
        """Parse search commands from query."""
        # Check for \search{content} command
        search_match = re.search(r"\\search\{([^}]+)}", query)
        if search_match:
            search_content = search_match.group(1)
            actual_question = re.sub(r"\\search\{[^}]+}", "", query).strip()
            return actual_question, search_content, True

        # Check for \no_search command
        if "\\no_search" in query:
            actual_question = query.replace("\\no_search", "").strip()
            return actual_question, "", False

        # Default: use query for both
        return query, query, True

    @staticmethod
    def should_use_search(query: str) -> bool:
        """Determine if the query should trigger a search."""
        if not SEARCH_CONFIG["use_search_for_api"]:
            return False

        if "\\no_search" in query:
            return False

        return True

    @staticmethod
    async def perform_search_and_generate(
        model: str,
        query: str,
        question: Optional[str] = None,
        temperature: Optional[float] = None,
        top_p: Optional[float] = None,
        top_k: Optional[int] = None,
        max_tokens: Optional[int] = None,
        stream: bool = False,
    ) -> Dict[str, Any]:
        """Perform search and generate response."""
        try:
            actual_question, search_query, _ = SearchHandler.parse_search_command(query)
            final_question = question or actual_question

            logger.info(f"Search: '{search_query}', Question: '{final_question}'")

            # Import here to avoid circular imports
            from ollama.ollama_integrate import (
                OptimizedOllamaIntegrate,
                SearchConfig as IntegrateSearchConfig,
                OllamaConfig,
            )

            search_config = IntegrateSearchConfig(
                query=search_query,
                question=final_question,
                pages=SEARCH_CONFIG["api_search_pages"],
                filter_ads=True,
                use_cache=True,
            )

            ollama_config = OllamaConfig(
                model=model,
                base_url=OLLAMA_CONFIG.get("base_url", "http://localhost:11434"),
                temperature=temperature,
                top_p=top_p,
                top_k=top_k,
                max_tokens=max_tokens,
                stream=stream,
            )

            integrator = OptimizedOllamaIntegrate(
                search_config=search_config,
                ollama_config=ollama_config,
            )

            result = await integrator.run(question=final_question)

            if result and "response" in result:
                result["response"] = SearchHandler.clean_response(result["response"])

            return result

        except Exception as e:
            logger.error(f"Search and generate error: {e}")
            raise

    @staticmethod
    def clean_response(response: str) -> str:
        """Clean the model response by removing thinking tags."""
        if not response:
            return response

        # Remove <think>...</think> tags
        cleaned = re.sub(r"<think>.*?</think>", "", response, flags=re.DOTALL)

        # Remove other reasoning artifacts
        cleaned = re.sub(r"<reasoning>.*?</reasoning>", "", cleaned, flags=re.DOTALL)

        return cleaned.strip()


class ChatHandler:
    """Handler for chat completion operations."""

    def __init__(self):
        self.models_handler = ModelsHandler()
        self.search_handler = SearchHandler()
        self.performance_config = PERFORMANCE_CONFIG
        self.compatibility_utils = OpenAICompatibilityUtils()
        self.ollama_base_url = OLLAMA_CONFIG.get("base_url", "http://localhost:11434")

    async def _stream_text_chunks(
        self, text: str, model: str, completion_id: str, start_with_space: bool = True
    ) -> AsyncGenerator[str, None]:
        """Helper method to stream text in chunks."""
        words = text.split()
        chunk_size = self.performance_config["chunk_size_words"]

        for i in range(0, len(words), chunk_size):
            chunk_words = words[i : i + chunk_size]
            content = (" " if i > 0 and start_with_space else "") + " ".join(
                chunk_words
            )

            chunk = self.compatibility_utils.create_chat_completion_chunk(
                model=model, delta_content=content, message_id=completion_id
            )

            if chunk and chunk.startswith('{"id":'):
                yield f"data: {chunk}\n\n"
            else:
                logger.error(f"Invalid chunk: {repr(chunk)}")

            await asyncio.sleep(self.performance_config["streaming_delay"])

    @staticmethod
    def _create_chat_completion_response(
        completion_id: str,
        created_time: int,
        model: str,
        response_text: str,
        usage: Dict[str, int],
    ) -> Dict[str, Any]:
        """Helper method to create chat completion response structure."""
        return {
            "id": completion_id,
            "object": "chat.completion",
            "created": created_time,
            "model": model,
            "choices": [
                {
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": response_text,
                    },
                    "finish_reason": "stop",
                }
            ],
            "usage": usage,
        }

    async def validate_model(self, model: str) -> bool:
        """Validate if the requested model is available."""
        available_models = await self.models_handler.get_available_models()
        return model in available_models

    @staticmethod
    def calculate_token_usage(prompt: str, response: str) -> Dict[str, int]:
        """Calculate approximate token usage."""
        prompt_tokens = int(len(prompt.split()) * 1.3)
        completion_tokens = int(len(response.split()) * 1.3)
        total_tokens = prompt_tokens + completion_tokens

        return {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": total_tokens,
        }

    async def generate_direct_response(
        self,
        model: str,
        query: str,
        temperature: Optional[float] = None,
        top_p: Optional[float] = None,
        top_k: Optional[int] = None,
        max_tokens: Optional[int] = None,
        clean_response: bool = False,
    ) -> str:
        """Generate response directly from Ollama without search."""
        result = await generate_with_ollama(
            prompt=query,
            model=model,
            base_url=self.ollama_base_url,
            temperature=temperature,
            top_p=top_p,
            top_k=top_k,
            max_tokens=max_tokens,
            stream=False,
            logger=logger,
        )

        raw_response = result.get("response", "")

        if clean_response:
            return SearchHandler.clean_response(raw_response)
        else:
            return raw_response

    async def create_streaming_chunks(
        self,
        response_text: str,
        completion_id: str,
        model: str,
        clean_response: bool = True,
    ) -> AsyncGenerator[str, None]:
        """Create streaming response chunks."""
        try:
            if clean_response:
                cleaned_text = SearchHandler.clean_response(response_text)
            else:
                cleaned_text = response_text

            async for chunk in self._stream_text_chunks(
                cleaned_text, model, completion_id, start_with_space=False
            ):
                yield chunk

            # Final chunk
            final_chunk = self.compatibility_utils.create_chat_completion_chunk(
                model=model,
                delta_content="",
                message_id=completion_id,
                finish_reason="stop",
            )

            # Validate final chunk
            if final_chunk and final_chunk.startswith('{"id":'):
                yield f"data: {final_chunk}\n\n"
            else:
                logger.error(f"Invalid final chunk: {repr(final_chunk)}")

            yield "data: [DONE]\n\n"

        except Exception as e:
            logger.error(f"Error in create_streaming_chunks: {e}")
            # Send error chunk
            error_chunk = self.compatibility_utils.create_chat_completion_chunk(
                model=model,
                delta_content="Error occurred during streaming",
                message_id=completion_id,
            )
            yield f"data: {error_chunk}\n\n"
            yield "data: [DONE]\n\n"

    async def create_streaming_chunks_with_search(
        self,
        model: str,
        query: str,
        completion_id: str,
        temperature: Optional[float] = None,
        top_p: Optional[float] = None,
        top_k: Optional[int] = None,
        max_tokens: Optional[int] = None,
    ) -> AsyncGenerator[str, None]:
        """Create streaming chunks with search."""

        try:
            # Send searching indicator
            searching_chunk = self.compatibility_utils.create_chat_completion_chunk(
                model=model, delta_content="Searching...", message_id=completion_id
            )

            # Validate chunk before yielding
            if searching_chunk and searching_chunk.startswith('{"id":'):
                yield f"data: {searching_chunk}\n\n"
            else:
                logger.error(f"Invalid searching chunk: {repr(searching_chunk)}")

            await asyncio.sleep(0.3)

            try:
                result = await self.search_handler.perform_search_and_generate(
                    model=model,
                    query=query,
                    temperature=temperature,
                    top_p=top_p,
                    top_k=top_k,
                    max_tokens=max_tokens,
                )

                response_text = result.get("response", "")

                # Send completion indicator
                completion_chunk = (
                    self.compatibility_utils.create_chat_completion_chunk(
                        model=model,
                        delta_content="\n\nGenerating response...\n\n",
                        message_id=completion_id,
                    )
                )

                if completion_chunk and completion_chunk.startswith('{"id":'):
                    yield f"data: {completion_chunk}\n\n"

                # Stream response
                async for chunk in self._stream_text_chunks(
                    response_text, model, completion_id
                ):
                    yield chunk

            except Exception as e:
                logger.error(f"Search failed: {e}")
                # Send error and fallback
                error_chunk = self.compatibility_utils.create_chat_completion_chunk(
                    model=model,
                    delta_content=f"\n\nSearch failed: {e}. Using direct response...\n\n",
                    message_id=completion_id,
                )

                if error_chunk and error_chunk.startswith('{"id":'):
                    yield f"data: {error_chunk}\n\n"

                try:
                    fallback = await self.generate_direct_response(
                        model, query, temperature, top_p, top_k, max_tokens, True
                    )
                    async for chunk in self._stream_text_chunks(
                        fallback, model, completion_id
                    ):
                        yield chunk

                except (
                    RuntimeError,
                    ValueError,
                    ConnectionError,
                    TimeoutError,
                ) as fallback_error:
                    logger.error(f"Fallback also failed: {fallback_error}")
                    error_chunk = self.compatibility_utils.create_chat_completion_chunk(
                        model=model,
                        delta_content="\n\nUnable to generate response",
                        message_id=completion_id,
                    )
                    if error_chunk and error_chunk.startswith('{"id":'):
                        yield f"data: {error_chunk}\n\n"

            # Final chunk
            final_chunk = self.compatibility_utils.create_chat_completion_chunk(
                model=model,
                delta_content="",
                message_id=completion_id,
                finish_reason="stop",
            )

            if final_chunk and final_chunk.startswith('{"id":'):
                yield f"data: {final_chunk}\n\n"
            else:
                logger.error(
                    f"Invalid final chunk in search streaming: {repr(final_chunk)}"
                )

            yield "data: [DONE]\n\n"

        except Exception as e:
            logger.error(f"Critical error in create_streaming_chunks_with_search: {e}")
            # Send emergency final chunk
            try:
                emergency_chunk = self.compatibility_utils.create_chat_completion_chunk(
                    model=model,
                    delta_content="Critical streaming error occurred",
                    message_id=completion_id,
                    finish_reason="stop",
                )
                yield f"data: {emergency_chunk}\n\n"
            except (ValueError, TypeError, AttributeError):
                pass
            yield "data: [DONE]\n\n"

    async def process_chat_completion(
        self,
        model: str,
        messages: List[Dict[str, Any]],
        stream: bool = False,
        temperature: Optional[float] = None,
        top_p: Optional[float] = None,
        top_k: Optional[int] = None,
        max_tokens: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Process chat completion request."""
        query = SearchHandler.extract_search_query(messages)
        completion_id = f"chatcmpl-{uuid.uuid4().hex[:8]}"
        created_time = int(time.time())

        # Check if search should be used
        use_search = SearchHandler.should_use_search(query)

        if use_search:
            # Parse search command
            actual_question, search_query, _ = SearchHandler.parse_search_command(query)

            if stream:
                return {
                    "type": "stream_with_search",
                    "model": model,
                    "query": query,
                    "actual_question": actual_question,
                    "search_query": search_query,
                    "completion_id": completion_id,
                    "created_time": created_time,
                    "temperature": temperature,
                    "top_p": top_p,
                    "top_k": top_k,
                    "max_tokens": max_tokens,
                }
            else:
                # Non-streaming search
                result = await self.search_handler.perform_search_and_generate(
                    model=model,
                    query=query,
                    temperature=temperature,
                    top_p=top_p,
                    top_k=top_k,
                    max_tokens=max_tokens,
                )

                response_text = result.get("response", "")
                usage = ChatHandler.calculate_token_usage(query, response_text)

                return self._create_chat_completion_response(
                    completion_id, created_time, model, response_text, usage
                )
        else:
            # Direct response without search
            actual_question, _, _ = SearchHandler.parse_search_command(query)

            if stream:
                response_text = await self.generate_direct_response(
                    model=model,
                    query=actual_question,
                    temperature=temperature,
                    top_p=top_p,
                    top_k=top_k,
                    max_tokens=max_tokens,
                    clean_response=False,
                )

                return {
                    "type": "stream",
                    "response_text": response_text,
                    "completion_id": completion_id,
                    "created_time": created_time,
                }
            else:
                response_text = await self.generate_direct_response(
                    model=model,
                    query=actual_question,
                    temperature=temperature,
                    top_p=top_p,
                    top_k=top_k,
                    max_tokens=max_tokens,
                    clean_response=False,
                )

                usage = ChatHandler.calculate_token_usage(
                    actual_question, response_text
                )

                return self._create_chat_completion_response(
                    completion_id, created_time, model, response_text, usage
                )


# Global handler instances
_models_handler: Optional[ModelsHandler] = None
_chat_handler: Optional[ChatHandler] = None


def get_models_handler() -> ModelsHandler:
    """Get the global models handler instance."""
    global _models_handler
    if _models_handler is None:
        _models_handler = ModelsHandler()
    return _models_handler


def get_chat_handler() -> ChatHandler:
    """Get the global chat handler instance."""
    global _chat_handler
    if _chat_handler is None:
        _chat_handler = ChatHandler()
    return _chat_handler
