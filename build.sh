#!/bin/bash

# Build script for minimal Synapse CLI
# This script creates cross-platform binaries using PyInstaller
# Usage: ./build.sh [platform]
# Platforms: linux, macos, windows, all

set -e

# Default to current platform if no argument provided
TARGET_PLATFORM=${1:-"auto"}

echo "Building Minimal Synapse CLI..."

# Install required packages
echo "Installing required packages..."
uv pip install pyinstaller
uv pip install -e .

# Clean previous builds
echo "Cleaning previous builds..."
rm -rf build/ dist/ *.spec

# Function to build for a specific platform
build_for_platform() {
    local platform=$1
    local extension=$2
    local extra_args=$3

    echo "Building for platform: $platform"
    local output_name="minimal-synapse-${platform}${extension}"

    echo "Building executable: $output_name"

    # Build the executable with simplified PyInstaller command (following Windows approach)
    pyinstaller \
        --onefile \
        --name "$output_name" \
        --collect-all=synapseclient \
        --console \
        $extra_args \
        minimal_synapse_cli.py

    # Clean up spec file
    rm -f *.spec

    if [ -f "dist/$output_name" ]; then
        echo "✓ Build successful: dist/$output_name"
        echo "File size: $(du -h dist/$output_name | cut -f1)"

        # Test the executable
        echo "Testing executable..."
        if ./dist/$output_name --help > /dev/null 2>&1; then
            echo "✓ Executable test passed"
        else
            echo "✗ Executable test failed"
            return 1
        fi
    else
        echo "✗ Build failed: dist/$output_name not found"
        return 1
    fi
}

# Determine what to build
case "$TARGET_PLATFORM" in
    "auto")
        # Auto-detect current platform
        if [[ "$OSTYPE" == "linux-gnu"* ]]; then
            build_for_platform "linux" ""
        elif [[ "$OSTYPE" == "darwin"* ]]; then
            build_for_platform "macos" ""
        elif [[ "$OSTYPE" == "msys" ]] || [[ "$OSTYPE" == "win32" ]]; then
            build_for_platform "windows" ".exe"
        else
            echo "Unknown platform: $OSTYPE"
            echo "Please specify platform: linux, macos, windows, or all"
            exit 1
        fi
        ;;
    "linux")
        build_for_platform "linux" ""
        ;;
    "macos")
        build_for_platform "macos" ""
        ;;
    "windows")
        build_for_platform "windows" ".exe"
        ;;
    "all")
        echo "Building for all platforms..."
        build_for_platform "linux" ""
        build_for_platform "macos" ""
        build_for_platform "windows" ".exe"
        ;;
    *)
        echo "Unknown platform: $TARGET_PLATFORM"
        echo "Available platforms: linux, macos, windows, all"
        exit 1
        ;;
esac

echo ""
echo "Build(s) complete!"
echo ""
echo "Available executables:"
ls -la dist/minimal-synapse-* 2>/dev/null || echo "No executables found"

echo ""
echo "Usage examples:"
if [ -f "dist/minimal-synapse-linux" ]; then
    echo "  ./dist/minimal-synapse-linux get syn123"
    echo "  ./dist/minimal-synapse-linux store myfile.txt --parentid syn456"
fi
if [ -f "dist/minimal-synapse-macos" ]; then
    echo "  ./dist/minimal-synapse-macos get syn123"
    echo "  ./dist/minimal-synapse-macos store myfile.txt --parentid syn456"
fi
if [ -f "dist/minimal-synapse-windows.exe" ]; then
    echo "  ./dist/minimal-synapse-windows.exe get syn123"
    echo "  ./dist/minimal-synapse-windows.exe store myfile.txt --parentid syn456"
fi

echo ""
echo "To install system-wide (Linux/macOS):"
echo "  sudo cp dist/minimal-synapse-linux /usr/local/bin/synapse-cli"
echo "  sudo cp dist/minimal-synapse-macos /usr/local/bin/synapse-cli"
echo ""
echo "Cross-platform build notes:"
echo "- Linux binary: Works on most Linux distributions"
echo "- macOS binary: Requires macOS to build, works on macOS 10.15+"
echo "- Windows binary: Built with simplified approach following Windows native script"
