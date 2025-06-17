"""
Utility functions for interacting with Ollama API.
Provides consistent interface for Ollama model operations.
"""

import aiohttp
import json
import logging
import random
from typing import Any, Callable, Dict, List, Optional


async def list_ollama_models(
    base_url: str = "http://localhost:11434",
    timeout: int = 5,
    logger: Optional[logging.Logger] = None,
) -> List[str]:
    """
    List available Ollama models by querying the API.

    Args:
        base_url: Base URL for Ollama API
        timeout: Timeout for API request in seconds
        logger: Optional logger for error reporting

    Returns:
        List of model names available on Ollama server
    """
    if logger is None:
        # 使用简化的导入方式
        try:
            from config import get_logger
            logger = get_logger()
        except ImportError:
            logger = logging.getLogger("ollama_utils")

    url = f"{base_url}/api/tags"
    logger.debug(f"Fetching models from {url}")

    try:
        client_timeout = aiohttp.ClientTimeout(
            total=timeout,
            connect=timeout,
            sock_connect=timeout,
            sock_read=timeout,
        )

        async with aiohttp.ClientSession(timeout=client_timeout) as session:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    model_names = [model["name"] for model in data.get("models", [])]
                    logger.info(f"Found {len(model_names)} Ollama models")
                    return model_names
                else:
                    error_text = await response.text()
                    logger.error(
                        f"Failed to list Ollama models: HTTP {response.status} - {error_text}"
                    )
                    return []
    except Exception as e:
        logger.error(f"Error connecting to Ollama API: {e}")
        return []


def interactive_model_selection(
    models: List[str], logger: Optional[logging.Logger] = None
) -> Optional[str]:
    """
    Prompt user to select an Ollama model from available options.

    Args:
        models: List of available model names
        logger: Optional logger for tracking selection

    Returns:
        Selected model name or None if no selection was made
    """
    if logger is None:
        # 使用简化的导入方式
        try:
            from config import get_logger
            logger = get_logger()
        except ImportError:
            logger = logging.getLogger("ollama_utils")

    if not models:
        logger.warning("No Ollama models available")
        print("No Ollama models available. Is Ollama running?")
        return None

    print("\nAvailable Ollama models:")
    for i, model in enumerate(models, 1):
        print(f"{i}. {model}")

    selected_model = None
    while True:
        try:
            choice = input("\nSelect a model (number or name): ").strip()

            if choice.isdigit():
                index = int(choice) - 1
                if 0 <= index < len(models):
                    selected_model = models[index]
                    break
                else:
                    print(f"Please enter a number between 1 and {len(models)}")
            elif choice in models:
                selected_model = choice
                break
            else:
                print("Invalid selection. Please enter a valid model number or name.")
        except Exception as e:
            print(f"Error: {str(e)}")

    if logger and selected_model:
        logger.info(f"Selected model: {selected_model}")
    return selected_model


async def generate_with_ollama(
    prompt: str,
    model: str,
    base_url: str = "http://localhost:11434",
    temperature: Optional[float] = None,
    top_p: Optional[float] = None,
    top_k: Optional[int] = None,
    context_size: Optional[int] = None,
    max_tokens: Optional[int] = None,
    stream: bool = True,
    stream_callback: Optional[Callable[[Dict[str, Any]], Any]] = None,
    timeout: int = 60,
    logger: Optional[logging.Logger] = None,
) -> Dict[str, Any]:
    """
    Send a prompt to the Ollama API and get a response.

    The Ollama API returns a streaming response in NDJSON format.
    We read the entire stream and extract the complete response.

    Args:
        prompt: The text prompt to send to the model.
        model: The name of the Ollama model to use.
        base_url: Base URL for Ollama API.
        temperature: Temperature for model generation (randomness).
        top_p: Top-p value for nucleus sampling.
        top_k: Top-k value for sampling.
        context_size: Context size for model.
        max_tokens: Maximum tokens to generate.
        stream: Whether to use streaming response.
        stream_callback: Optional callback function for streaming mode.
        timeout: Timeout for initial response in seconds.
        logger: Optional logger for error reporting.

    Returns:
        Dictionary containing the response or error.
    """
    url = f"{base_url}/api/generate"

    # Use random values for temperature, top_p, top_k if not provided
    final_temperature = (
        temperature if temperature is not None else random.uniform(0.5, 0.9)
    )
    final_top_p = top_p if top_p is not None else random.uniform(0.8, 1.0)
    final_top_k = top_k if top_k is not None else random.randint(30, 50)

    # Enable streaming if a callback is provided, otherwise use the stream parameter
    use_streaming = True if stream_callback else stream

    payload = {
        "model": model,
        "prompt": prompt,
        "temperature": final_temperature,
        "top_p": final_top_p,
        "top_k": final_top_k,
        "stream": use_streaming,
    }

    # Only include optional parameters if they're explicitly set
    if context_size is not None:
        payload["context_size"] = context_size

    if max_tokens is not None:
        payload["max_tokens"] = max_tokens

    if logger:
        logger.debug(
            f"Sending prompt to Ollama API (model: {model}, streaming: {use_streaming})"
        )

    try:
        # Create a ClientTimeout that only limits time to first response
        # but allows unlimited time for reading the full response
        client_timeout = aiohttp.ClientTimeout(
            total=None,  # No total timeout
            connect=timeout,  # Timeout for connection establishment
            sock_connect=timeout,  # Socket connection timeout
            sock_read=None,  # No timeout for reading response (can take as long as needed)
        )

        async with aiohttp.ClientSession(timeout=client_timeout) as session:
            async with session.post(url, json=payload) as response:
                if response.status == 200:
                    # Check content type to handle different response formats
                    content_type = response.headers.get("Content-Type", "")

                    if "application/json" in content_type and not use_streaming:
                        # Parse regular JSON response (non-streaming mode)
                        data = await response.json()
                        return data
                    elif "application/x-ndjson" in content_type or use_streaming:
                        # Handle streaming NDJSON response
                        full_text = ""
                        async for line in response.content:
                            if line:
                                try:
                                    # Parse each line as JSON
                                    chunk = json.loads(line)
                                    if "response" in chunk:
                                        # Add to full text
                                        full_text += chunk["response"]

                                        # Call callback if provided
                                        if stream_callback:
                                            await stream_callback(chunk)
                                except json.JSONDecodeError:
                                    if logger:
                                        logger.warning(
                                            f"Failed to parse JSON chunk: {line}"
                                        )

                        return {"response": full_text}
                    else:
                        # Try to handle as text if content type is not recognized
                        text = await response.text()
                        if logger:
                            logger.warning(f"Unexpected content type: {content_type}")
                        return {"response": text}
                else:
                    error_text = await response.text()
                    if logger:
                        logger.error(f"Ollama API error: {error_text}")
                    return {"error": error_text}
    except Exception as e:
        if logger:
            logger.error(f"Error calling Ollama API: {str(e)}")
        return {"error": str(e)}


def format_search_results_for_ollama(
    search_results: List[Dict[str, Any]], logger: Optional[logging.Logger] = None
) -> str:
    """
    Format search results as a JSON string for the LLM prompt.

    Args:
        search_results: List of search result dictionaries.
        logger: Optional logger for warning about non-serializable results.

    Returns:
        JSON string containing clean, serializable search results.
    """
    if not search_results:
        return json.dumps({"results": []})

    # Make sure all the search results are JSON-serializable
    clean_results = []
    for result in search_results:
        try:
            # Try serializing and deserializing to catch any issues
            result_str = json.dumps(result)
            clean_result = json.loads(result_str)
            clean_results.append(clean_result)
        except (TypeError, json.JSONDecodeError) as e:
            if logger:
                logger.warning(f"Skipping non-serializable search result: {e}")
            # Try to create a clean version with just the basic fields
            try:
                clean_result = {
                    "title": str(result.get("title", "")),
                    "url": str(result.get("url", "")),
                    "content": str(result.get("content", "")),
                    "source": str(result.get("source", "")),
                }
                clean_results.append(clean_result)
            except Exception as e2:
                if logger:
                    logger.error(f"Failed to clean search result: {e2}")

    # Return the clean results as a JSON string
    return json.dumps({"results": clean_results}, ensure_ascii=False)


def create_system_prompt() -> str:
    """
    Create a system prompt for the Ollama model that explains how to use the search results.

    Returns:
        A string containing the system prompt.
    """
    # Check if there's a custom system prompt in the config
    try:
        from config import OLLAMA_CONFIG

        custom_prompt = OLLAMA_CONFIG.get("system_prompt", "")
        if custom_prompt:
            return custom_prompt
    except (ImportError, AttributeError):
        pass

    return """
    You are a helpful AI assistant. You have been provided with search results from Baidu.
    Use these search results to answer the user's question as accurately as possible.
    If the search results don't contain relevant information, just say you don't know.
    The search results are provided as a JSON object with results in a 'results' array.
    Each result has 'title', 'url', 'content', and possibly other fields.
    """


def create_full_prompt(system_prompt: str, context: str, question: str) -> str:
    """
    Combine system prompt, context, and user question into a full prompt for the LLM.

    Args:
        system_prompt: Instructions for the model.
        context: Search results or other context.
        question: The user's question.

    Returns:
        Combined prompt string.
    """
    return f"{system_prompt}\n\nSearch Results:\n{context}\n\nQuestion: {question}\n\nAnswer:"


async def check_ollama_status(
    base_url: str = "http://localhost:11434",
    timeout: int = 3,
    logger: Optional[logging.Logger] = None,
) -> bool:
    """
    Check if Ollama server is running and responding.

    Args:
        base_url: Base URL for Ollama API.
        timeout: Timeout for the API request in seconds.
        logger: Optional logger for error reporting.

    Returns:
        True if Ollama is running, False otherwise.
    """
    try:
        client_timeout = aiohttp.ClientTimeout(
            total=timeout,
            connect=timeout,
            sock_connect=timeout,
            sock_read=timeout,
        )

        async with aiohttp.ClientSession(timeout=client_timeout) as session:
            async with session.get(f"{base_url}/api/tags") as response:
                if response.status == 200:
                    if logger:
                        logger.info("Ollama server is running")
                    return True
                else:
                    if logger:
                        logger.warning(
                            f"Ollama server returned status {response.status}"
                        )
                    return False
    except Exception as e:
        if logger:
            logger.warning(f"Failed to connect to Ollama server: {str(e)}")
        return False


async def get_model_info(
    model: str,
    base_url: str = "http://localhost:11434",
    timeout: int = 5,
    logger: Optional[logging.Logger] = None,
) -> Dict[str, Any]:
    """
    Get detailed information about a specific Ollama model.

    Args:
        model: The name of the Ollama model.
        base_url: Base URL for Ollama API.
        timeout: Timeout for the API request in seconds.
        logger: Optional logger for error reporting.

    Returns:
        Dictionary containing model information or an empty dict if the request fails.
    """
    url = f"{base_url}/api/show"
    payload = {"name": model}

    try:
        client_timeout = aiohttp.ClientTimeout(
            total=timeout,
            connect=timeout,
            sock_connect=timeout,
            sock_read=timeout,
        )

        async with aiohttp.ClientSession(timeout=client_timeout) as session:
            async with session.post(url, json=payload) as response:
                if response.status == 200:
                    data = await response.json()
                    return data
                else:
                    error_text = await response.text()
                    if logger:
                        logger.error(
                            f"Failed to get model info for {model}: {error_text}"
                        )
                    return {}
    except Exception as e:
        if logger:
            logger.error(f"Error getting model info for {model}: {str(e)}")
        return {}


def get_recommended_parameters(
    model: str, context_size: Optional[int] = None
) -> Dict[str, Any]:
    """
    Get recommended parameter values for different Ollama models.

    Args:
        model: The name of the Ollama model.
        context_size: Optional context size to override defaults.

    Returns:
        Dictionary with recommended parameter values.
    """
    # Base parameters
    params = {
        "temperature": 0.7,
        "top_p": 0.9,
        "top_k": 40,
        "context_size": 4096,  # Default for most models
        "max_tokens": None,  # Let the model decide
    }

    # Model-specific overrides
    model_lower = model.lower()

    # llama2 variants
    if "llama2" in model_lower:
        params["context_size"] = 4096

    # llama3 variants
    elif "llama3" in model_lower:
        params["context_size"] = 8192
        if "8b" in model_lower:
            params["context_size"] = 8192
        if "70b" in model_lower:
            params["context_size"] = 8192

    # Mistral variants
    elif "mistral" in model_lower:
        params["context_size"] = 8192
        if "7b" in model_lower:
            params["context_size"] = 8192

    # Mixtral variants
    elif "mixtral" in model_lower:
        params["context_size"] = 32768

    # Claude variants
    elif "claude" in model_lower:
        params["context_size"] = 100000

    # GPT variants
    elif "gpt" in model_lower:
        if "4" in model_lower:
            params["context_size"] = 128000
        elif "3.5" in model_lower:
            params["context_size"] = 16000

    # Override with provided context size if specified
    if context_size is not None:
        params["context_size"] = context_size

    return params
