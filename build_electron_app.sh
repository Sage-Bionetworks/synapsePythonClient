#!/bin/bash

# Build script for Synapse Desktop Client (Electron + Python Backend)
# This script creates a complete packaged application with both frontend and backend
# Usage: ./build_electron_app.sh [platform]
# Platforms: linux, macos, all

set -e

# Default to current platform if no argument provided
TARGET_PLATFORM=${1:-"auto"}

echo "Building Synapse Desktop Client (Electron + Python Backend)..."

# Ensure we're in the project root
cd "$(dirname "$0")"

# Activate virtual environment if it exists
if [ -f ".venv/bin/activate" ]; then
    echo "Activating virtual environment..."
    source .venv/bin/activate
else
    echo "Warning: Virtual environment not found at .venv/bin/activate"
    echo "Continuing with system Python..."
fi

# Check required tools
echo "Checking required tools..."
if ! command -v node &> /dev/null; then
    echo "ERROR: Node.js is not installed or not in PATH"
    echo "Please install Node.js from https://nodejs.org/"
    exit 1
fi

if ! command -v python3 &> /dev/null; then
    echo "ERROR: Python is not installed or not in PATH"
    echo "Please install Python from https://python.org/"
    exit 1
fi

# Install Python dependencies with electron extras
if [ "$SKIP_DEPENDENCY_INSTALL" = "1" ]; then
    echo "Skipping Python dependency installation (CI mode)"
else
    echo "Installing Python dependencies..."
    if command -v uv &> /dev/null; then
        echo "Using uv package manager..."
        uv pip install "pyinstaller>=6.14.0" "pyinstaller-hooks-contrib>=2024.0"
        uv pip install -e ".[electron]"
    else
        echo "Using pip package manager..."
        python3 -m pip install "pyinstaller>=6.14.0" "pyinstaller-hooks-contrib>=2024.0"
        python3 -m pip install -e ".[electron]"
    fi
fi

# Verify PyInstaller version
# echo "Verifying PyInstaller installation..."
# python3 -c "import PyInstaller; print('PyInstaller version:', PyInstaller.__version__)"
# if [ $? -ne 0 ]; then
#     echo "ERROR: PyInstaller not properly installed"
#     exit 1
# fi

# Function to build Python backend for a specific platform
build_python_backend() {
    local platform=$1

    echo "Building Python backend for $platform..."
    cd synapse-electron/backend

    # Clean previous builds
    rm -rf dist/ build/ *.spec

    # Create PyInstaller spec and build
    pyinstaller \
        --onefile \
        --name "synapse-backend" \
        --collect-all=synapseclient \
        --collect-all=fastapi \
        --collect-all=uvicorn \
        --collect-all=starlette \
        --collect-all=pydantic \
        --collect-all=websockets \
        --collect-all=synapsegui \
        --paths "../.." \
        --paths "../../synapsegui" \
        --paths "../../synapseclient" \
        --console \
        server.py

    if [ ! -f "dist/synapse-backend" ]; then
        echo "ERROR: Python backend build failed"
        exit 1
    fi

    echo "Python backend built successfully"
    cd ../..
}

# Function to build Electron app for a specific platform
build_electron_app() {
    local platform=$1

    echo "Building Electron application for $platform..."
    cd synapse-electron

    # # Install Node.js dependencies
    # echo "Installing Node.js dependencies..."
    # npm install

    # Set platform-specific build command
    case "$platform" in
        "linux")
            npm run build -- --linux
            ;;
        "macos")
            npm run build -- --mac
            ;;
        "windows")
            npm run build -- --win
            ;;
        *)
            npm run build
            ;;
    esac

    cd ..
}

# Determine what to build
case "$TARGET_PLATFORM" in
    "auto")
        # Auto-detect current platform
        if [[ "$OSTYPE" == "linux-gnu"* ]]; then
            build_python_backend "linux"
            build_electron_app "linux"
        elif [[ "$OSTYPE" == "darwin"* ]]; then
            build_python_backend "macos"
            build_electron_app "macos"
        else
            echo "Unsupported platform: $OSTYPE"
            echo "This script supports Linux and macOS platforms"
            echo "Please specify platform: linux, macos, or all"
            exit 1
        fi
        ;;
    "linux")
        build_python_backend "linux"
        build_electron_app "linux"
        ;;
    "macos")
        build_python_backend "macos"
        build_electron_app "macos"
        ;;
    "all")
        echo "Building for all supported platforms..."
        build_python_backend "linux"
        build_python_backend "macos"
        build_electron_app "linux"
        build_electron_app "macos"
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
echo "Electron application packages are in: synapse-electron/dist/"
echo "Python backend executables are in: synapse-electron/backend/dist/"

echo ""
echo "Available packages:"
if [ -d "synapse-electron/dist" ]; then
    ls -la synapse-electron/dist/
fi
