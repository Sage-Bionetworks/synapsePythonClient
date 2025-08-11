#!/bin/bash

# Build script for Synapse Desktop Client
# This script creates cross-platform binaries using PyInstaller
# Usage: ./build.sh [platform] [suffix]
# Platforms: linux, macos, all
# Suffix: optional suffix to add to the output filename

set -e

# Default to current platform if no argument provided
TARGET_PLATFORM=${1:-"auto"}
SUFFIX=${2:-""}

echo "Building Synapse Desktop Client..."

# Clean previous builds
echo "Cleaning previous builds..."
rm -rf build/ dist/ *.spec

# Function to build for a specific platform
build_for_platform() {
    local platform=$1
    local extension=$2
    local extra_args=$3

    echo "Building for platform: $platform"
    local base_name="synapse-desktop-client-${platform}"
    local output_name="${base_name}${SUFFIX}${extension}"

    echo "Building executable: $output_name"

    # Build the executable with simplified PyInstaller command (following Windows approach)
    pyinstaller \
        --onefile \
        --name "$output_name" \
        --collect-all=synapseclient \
        --console \
        $extra_args \
        synapse_gui.py

    # Clean up spec file
    rm -f *.spec

    if [ -f "dist/$output_name" ]; then
        echo "✓ Build successful: dist/$output_name"
        echo "File size: $(du -h dist/$output_name | cut -f1)"
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
        else
            echo "Unsupported platform: $OSTYPE"
            echo "This script only supports Linux and macOS platforms"
            echo "Please specify platform: linux, macos, or all"
            exit 1
        fi
        ;;
    "linux")
        build_for_platform "linux" ""
        ;;
    "macos")
        build_for_platform "macos" ""
        ;;
    "all")
        echo "Building for all supported platforms..."
        build_for_platform "linux" ""
        build_for_platform "macos" ""
        ;;
    *)
        echo "Unknown platform: $TARGET_PLATFORM"
        echo "Available platforms: linux, macos, all"
        exit 1
        ;;
esac

echo ""
echo "Build(s) complete!"
echo ""
echo "Available executables:"
ls -la dist/synapse-desktop-client-* 2>/dev/null || echo "No executables found"
