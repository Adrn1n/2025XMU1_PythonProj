import argparse
import sys
import asyncio
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import OLLAMA_CONFIG, get_module_logger
from utils.ollama_utils import check_ollama_status
import uvicorn

logger = get_module_logger(__name__)


async def check_dependencies():
    """Check if Ollama service is available."""
    ollama_url = OLLAMA_CONFIG.get("base_url", "http://localhost:11434")
    logger.info(f"Checking Ollama at {ollama_url}...")
    
    try:
        available = await check_ollama_status(ollama_url, timeout=5)
        logger.info("✅ Ollama service available" if available else "⚠️ Ollama service unavailable")
        return available
    except Exception as e:
        logger.error(f"❌ Ollama check failed: {e}")
        return False


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Ollama-Baidu Search API Server")
    
    # Server options
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind to")
    parser.add_argument("--port", type=int, default=8000, help="Port to bind to")
    parser.add_argument("--workers", type=int, default=1, help="Number of workers")
    parser.add_argument("--reload", action="store_true", help="Enable auto-reload")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")
    parser.add_argument("--check-deps", action="store_true", help="Check dependencies")
    
    args = parser.parse_args()
    
    # Handle utility commands
    if args.check_deps:
        try:
            asyncio.run(check_dependencies())
        except Exception as e:
            logger.error(f"Dependency check failed: {e}")
        return
    
    # Server configuration
    log_level = "debug" if args.debug else "info"
    workers = 1 if args.reload else args.workers
    
    logger.info("Starting Ollama-Baidu Search API Server")
    logger.info(f"Host: {args.host}, Port: {args.port}")
    
    try:
        # Change to project root for consistent imports
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
