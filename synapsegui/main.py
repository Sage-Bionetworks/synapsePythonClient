#!/usr/bin/env python3
"""
Main entry point for the refactored Synapse GUI application
"""
import os
import sys

# Add the parent directory to the path so we can import synapsegui
if __name__ == "__main__":
    # Get the directory containing this script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Add the parent directory to Python path
    parent_dir = os.path.dirname(script_dir)
    if parent_dir not in sys.path:
        sys.path.insert(0, parent_dir)

    try:
        from synapsegui.refactored_main import main

        main()
    except Exception as e:
        print(f"Error starting Synapse GUI: {e}")
        sys.exit(1)
