"""
Server startup script for Ollama-Baidu Search API.
Provides dependency checking and server configuration.
"""

import argparse
import asyncio
import os
import sys
from pathlib import Path

import uvicorn

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import OLLAMA_CONFIG, get_logger
from utils.ollama_utils import check_ollama_status

logger = get_logger()


async def check_dependencies():
    """Check if required services are available."""
    ollama_url = OLLAMA_CONFIG.get("base_url", "http://localhost:11434")
    logger.info(f"Checking Ollama service at {ollama_url}")

    try:
        available = await check_ollama_status(ollama_url, timeout=5)
        if available:
            logger.info("Ollama service is available")
        else:
            logger.warning("Ollama service is not available")
        return available
    except Exception as e:
        logger.error(f"Failed to check Ollama service: {e}")
        return False


def main():
    """Main entry point for server startup."""
    parser = argparse.ArgumentParser(description="Ollama-Baidu Search API Server")

    parser.add_argument("--host", default="0.0.0.0", help="Host to bind to")
    parser.add_argument("--port", type=int, default=8000, help="Port to bind to")
    parser.add_argument("--workers", type=int, default=1, help="Number of workers")
    parser.add_argument("--reload", action="store_true", help="Enable auto-reload")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")
    parser.add_argument("--check-deps", action="store_true", help="Check dependencies")

    args = parser.parse_args()

    if args.check_deps:
        try:
            asyncio.run(check_dependencies())
        except Exception as e:
            logger.error(f"Dependency check failed: {e}")
        return

    log_level = "debug" if args.debug else "info"
    workers = 1 if args.reload else args.workers

    logger.info("Starting Ollama-Baidu Search API Server")
    logger.info(f"Configuration: host={args.host}, port={args.port}, workers={workers}")

    try:
        project_root = Path(__file__).parent.parent
        os.chdir(project_root)

        uvicorn.run(
            "api.openai:app",
            host=args.host,
            port=args.port,
            reload=args.reload,
            workers=workers,
            log_level=log_level,
        )
    except KeyboardInterrupt:
        logger.info("Server stopped by user")
    except Exception as e:
        logger.error(f"Server startup failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
