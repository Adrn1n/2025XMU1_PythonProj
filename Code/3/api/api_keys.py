"""
API key management command-line tool.
Provides functionality to generate, add, remove, and validate API keys.
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import get_logger
from utils.api_core import get_api_key_manager

logger = get_logger()


def main():
    """Main entry point for API key management."""
    parser = argparse.ArgumentParser(description="API Key Management")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    subparsers.add_parser("list", help="List all API keys")

    gen_parser = subparsers.add_parser("generate", help="Generate new API key")
    gen_parser.add_argument("--prefix", default="sk-ollama", help="Key prefix")
    gen_parser.add_argument("--auto-add", action="store_true", help="Auto add key")

    add_parser = subparsers.add_parser("add", help="Add API key")
    add_parser.add_argument("key", help="API key to add")

    remove_parser = subparsers.add_parser("remove", help="Remove API key")
    remove_parser.add_argument("key", help="API key to remove")

    validate_parser = subparsers.add_parser("validate", help="Validate API key")
    validate_parser.add_argument("key", help="API key to validate")

    subparsers.add_parser("stats", help="Show statistics")

    args = parser.parse_args()
    manager = get_api_key_manager()

    if not manager:
        logger.error("Failed to initialize API key manager")
        print("Error: Failed to initialize API key manager")
        return

    try:
        if args.command == "list":
            keys = manager.load_api_keys(use_cache=False)
            print(f"\nFound {len(keys)} API keys:")
            for key in sorted(keys):
                print(f"  {key}")
            if not keys:
                print("  No API keys found.")
            logger.info(f"Listed {len(keys)} API keys")

        elif args.command == "generate":
            new_key = manager.generate_api_key(prefix=args.prefix)
            print(f"\nGenerated: {new_key}")
            logger.info(f"Generated new API key with prefix: {args.prefix}")
            if args.auto_add:
                success, message = manager.add_api_key(new_key)
                print(f"{'Success' if success else 'Error'}: {message}")
                if success:
                    logger.info(f"Auto-added generated key: {new_key}")
                else:
                    logger.error(f"Failed to auto-add key: {message}")

        elif args.command == "add":
            success, message = manager.add_api_key(args.key)
            print(f"{'Success' if success else 'Error'}: {message}")
            if success:
                logger.info(f"Added API key: {args.key}")
            else:
                logger.error(f"Failed to add key: {message}")

        elif args.command == "remove":
            success, message = manager.remove_api_key(args.key)
            print(f"{'Success' if success else 'Error'}: {message}")
            if success:
                logger.info(f"Removed API key: {args.key}")
            else:
                logger.error(f"Failed to remove key: {message}")

        elif args.command == "validate":
            is_valid = manager.validate_api_key(args.key)
            print(f"Key '{args.key}': {'Valid' if is_valid else 'Invalid'}")
            logger.info(
                f"Validated key {args.key}: {'valid' if is_valid else 'invalid'}"
            )

        elif args.command == "stats":
            stats = manager.get_stats()
            print("\nAPI Key Statistics:")
            for key, value in stats.items():
                print(f"  {key}: {value}")
            logger.info("Displayed API key statistics")

        else:
            parser.print_help()

    except KeyboardInterrupt:
        print("\nOperation cancelled.")
        logger.info("Operation cancelled by user")
    except Exception as e:
        logger.error(f"Error during operation: {e}")
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
