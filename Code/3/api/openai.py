"""
Simplified OpenAI-compatible API for Ollama with Baidu search integration.
"""

import time
import sys
from pathlib import Path
from typing import List, Optional, Dict, Any
from datetime import datetime

from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, Field

# Add parent directory for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

# Import with simple fallbacks
try:
    from config import API_CONFIG, OLLAMA_CONFIG, get_logger
    from utils.api_core import get_api_key_manager
    from utils.ollama_utils import check_ollama_status
    from utils.api_services import get_models_handler, get_chat_handler

    logger = get_logger()
    api_key_manager = get_api_key_manager()
    models_handler = get_models_handler()
    chat_handler = get_chat_handler()

except ImportError as e:
    import logging
    
    # Define fallback functions
    def get_fallback_logger() -> logging.Logger:
        # 使用简单的logger作为回退
        return logging.getLogger("api.openai")

    def get_api_key_manager() -> Optional[Any]:
        return None

    def get_models_handler() -> Optional[Any]:
        return None

    def get_chat_handler() -> Optional[Any]:
        return None

    logger = get_fallback_logger()
    logger.error(f"Import failed: {e}")
    API_CONFIG = {"auth_required": False}
    OLLAMA_CONFIG = {"base_url": "http://localhost:11434"}
    api_key_manager = get_api_key_manager()
    models_handler = get_models_handler()
    chat_handler = get_chat_handler()

    async def check_ollama_status(url, **kwargs):
        """Fallback function for checking Ollama status when imports fail."""
        _ = url, kwargs  # Suppress unused parameter warnings
        return False


OLLAMA_BASE_URL = OLLAMA_CONFIG.get("base_url", "http://localhost:11434")
AUTH_REQUIRED = API_CONFIG.get("auth_required", False)
security = HTTPBearer(auto_error=False)

app = FastAPI(
    title="Ollama-Baidu Search API",
    description="OpenAI-compatible API with Baidu search integration",
    version="1.0.0",
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Pydantic models
class ChatMessage(BaseModel):
    role: str
    content: str


class ChatCompletionRequest(BaseModel):
    model: str
    messages: List[ChatMessage]
    temperature: Optional[float] = None
    top_p: Optional[float] = None
    top_k: Optional[int] = None
    max_tokens: Optional[int] = None
    stream: bool = True


class ModelInfo(BaseModel):
    id: str
    object: str = "model"
    created: int = Field(default_factory=lambda: int(time.time()))
    owned_by: str = "ollama"


class ModelsResponse(BaseModel):
    object: str = "list"
    data: List[ModelInfo]


def verify_api_key(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
) -> str:
    """Verify API key."""
    if not AUTH_REQUIRED:
        return "no-auth"

    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API key required",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if api_key_manager and not api_key_manager.validate_api_key(
        credentials.credentials
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return credentials.credentials


# Routes
@app.get("/v1/models", response_model=ModelsResponse)
async def list_models(_: str = Depends(verify_api_key)):
    """List available models."""
    if models_handler:
        return await models_handler.create_models_response()

    # Fallback
    return ModelsResponse(
        data=[
            ModelInfo(id="llama3"),
            ModelInfo(id="qwen2"),
        ]
    )


@app.post("/v1/chat/completions")
async def create_chat_completion(
    request: ChatCompletionRequest, _: str = Depends(verify_api_key)
):
    """Create chat completion."""
    if not chat_handler:
        # Fallback response
        return {
            "id": f"chatcmpl-{int(time.time())}",
            "object": "chat.completion",
            "created": int(time.time()),
            "model": request.model,
            "choices": [
                {
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": "Fallback response - full functionality unavailable.",
                    },
                    "finish_reason": "stop",
                }
            ],
            "usage": {"prompt_tokens": 10, "completion_tokens": 15, "total_tokens": 25},
        }

    # Validate model
    if not await chat_handler.validate_model(request.model):
        available_models = (
            await models_handler.get_available_models()
            if models_handler
            else ["llama3"]
        )
        raise HTTPException(
            status_code=400,
            detail=f"Model '{request.model}' not found. Available: {', '.join(available_models)}",
        )

    # Process request
    messages = [{"role": msg.role, "content": msg.content} for msg in request.messages]
    result = await chat_handler.process_chat_completion(
        model=request.model,
        messages=messages,
        stream=request.stream,
        temperature=request.temperature,
        top_p=request.top_p,
        top_k=request.top_k,
        max_tokens=request.max_tokens,
    )

    # Handle streaming responses
    if request.stream and result.get("type") in ["stream", "stream_with_search"]:
        if result.get("type") == "stream_with_search":
            return StreamingResponse(
                chat_handler.create_streaming_chunks_with_search(
                    result["model"],
                    result["query"],
                    result["completion_id"],
                    result.get("temperature"),
                    result.get("top_p"),
                    result.get("top_k"),
                    result.get("max_tokens"),
                ),
                media_type="text/event-stream",
                headers={
                    "Cache-Control": "no-cache",
                    "Connection": "keep-alive",
                },
            )
        else:
            return StreamingResponse(
                chat_handler.create_streaming_chunks(
                    result["response_text"], result["completion_id"], request.model
                ),
                media_type="text/event-stream",
                headers={
                    "Cache-Control": "no-cache",
                    "Connection": "keep-alive",
                },
            )

    return result


@app.get("/health")
async def health_check() -> Dict[str, Any]:
    """Health check."""
    ollama_status = await check_ollama_status(OLLAMA_BASE_URL)

    health_info: Dict[str, Any] = {
        "status": "healthy" if ollama_status else "degraded",
        "ollama_available": ollama_status,
        "ollama_url": OLLAMA_BASE_URL,
        "auth_required": AUTH_REQUIRED,
        "components": {
            "api_key_manager": api_key_manager is not None,
            "models_handler": models_handler is not None,
            "chat_handler": chat_handler is not None,
        },
        "timestamp": datetime.now().isoformat(),
    }

    if models_handler:
        try:
            models = await models_handler.get_available_models()
            health_info["models_count"] = len(models)
        except (AttributeError, ConnectionError, TimeoutError) as ex:
            logger.warning(f"Failed to get models count: {ex}")
            health_info["models_count"] = 0

    return health_info


@app.get("/")
async def root():
    """API information."""
    return {
        "name": "Ollama-Baidu Search API",
        "version": "1.0.0",
        "description": "OpenAI-compatible API with Baidu search integration",
        "endpoints": ["/v1/models", "/v1/chat/completions", "/health"],
        "features": {
            "ollama_integration": models_handler is not None,
            "search_integration": chat_handler is not None,
            "authentication": AUTH_REQUIRED,
            "streaming": True,
        },
        "status": "ready",
    }


# Error handlers
@app.exception_handler(HTTPException)
async def http_exception_handler(_, exc):
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": {"message": exc.detail, "type": "http_error"}},
    )


@app.exception_handler(Exception)
async def general_exception_handler(_, exc):
    logger.error(f"Unhandled error: {exc}")
    return JSONResponse(
        status_code=500,
        content={
            "error": {"message": "Internal server error", "type": "internal_error"}
        },
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
