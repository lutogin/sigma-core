"""
Alpha Core Math V2 - Entry Point

Runs cointegration scanner and exits.
"""

import sys
import os
import multiprocessing

from src.app import Application

def log_cpu_info():
    print(f"CPU count (os): {os.cpu_count()}")
    print(f"CPU count (mp): {multiprocessing.cpu_count()}")


def main() -> int:
    """
    Main entry point for the trading bot.

    Starts the bot which runs cointegration scanner and exits.
    Press Ctrl+C to stop.

    Returns:
        Exit code (0 for success, non-zero for error)
    """
    # Set multiprocessing start method to 'spawn' for Docker/Podman compatibility
    # This is required because 'fork' method has issues with shared memory in containers
    try:
        log_cpu_info()
        multiprocessing.set_start_method('spawn', force=True)
    except RuntimeError:
        # Already set, ignore
        pass

    try:
        Application().init().run()
        return 0

    except KeyboardInterrupt:
        print("\nShutdown requested...")
        return 0
    except Exception as e:
        import traceback
        print(f"Error: {e}")
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())

