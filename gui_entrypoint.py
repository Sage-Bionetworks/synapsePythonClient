#!/usr/bin/env python3
"""
Main entry point for the refactored Synapse GUI application
"""
import io
import os
import sys


# Fix for PyInstaller windowed mode where stdout/stderr can be None
def _fix_console_streams():
    """Ensure stdout and stderr are not None when running in PyInstaller windowed mode"""
    if sys.stdout is None:
        sys.stdout = io.StringIO()
    if sys.stderr is None:
        sys.stderr = io.StringIO()


# Add the parent directory to the path so we can import synapsegui
if __name__ == "__main__":
    # Fix console streams first for PyInstaller compatibility
    _fix_console_streams()
    
    # Get the directory containing this script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Add the parent directory to Python path
    parent_dir = os.path.dirname(script_dir)
    if parent_dir not in sys.path:
        sys.path.insert(0, parent_dir)

    try:
        from synapsegui.synapse_gui import main

        main()
    except Exception as e:
        print(f"Error starting Synapse GUI: {e}")
        sys.exit(1)
