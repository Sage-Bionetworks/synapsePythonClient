#!/bin/bash
# Synapse Desktop Client - Unix Startup Script

echo "Starting Synapse Desktop Client..."
echo

# Check if Node.js is installed
if ! command -v node &> /dev/null; then
    echo "ERROR: Node.js is not installed or not in PATH"
    echo "Please install Node.js from https://nodejs.org/"
    exit 1
fi

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    if ! command -v python &> /dev/null; then
        echo "ERROR: Python is not installed or not in PATH"
        echo "Please install Python from https://python.org/"
        exit 1
    fi
    PYTHON_CMD="python"
else
    PYTHON_CMD="python3"
fi

# Change to the synapse-electron directory
cd "$(dirname "$0")"

# Check if node_modules exists
if [ ! -d "node_modules" ]; then
    echo "Installing Node.js dependencies..."
    npm install
    if [ $? -ne 0 ]; then
        echo "ERROR: Failed to install Node.js dependencies"
        exit 1
    fi
fi

# Check if Python backend dependencies are installed
cd backend

# uv pip install -r requirements.txt
cd ..
cd ..

# Activate uv virtual environment and install electron dependencies
echo "Installing Python dependencies with uv..."
source .venv/bin/activate
uv pip install -e .[electron]

cd synapse-electron
cd backend

# if [ ! -d "venv" ]; then
#     echo "Creating Python virtual environment..."
#     $PYTHON_CMD -m venv venv
#     source venv/bin/activate
#     echo "Installing Python dependencies..."
#     pip install -r requirements.txt
#     cd ..
#     pip install -e .
#     cd backend
# else
#     source venv/bin/activate
# fi

cd ..

# Start the application
echo
echo "Starting Synapse Desktop Client..."
echo "Backend will start on http://localhost:8000"
echo "WebSocket will start on ws://localhost:8001"
echo

# Check if we're running in a headless environment (no DISPLAY set)
if [ -z "$DISPLAY" ]; then
    echo "No display detected - starting virtual display for headless operation..."

    # Start Xvfb (X Virtual Framebuffer) in the background
    export DISPLAY=:99
    Xvfb :99 -screen 0 1024x768x24 > /dev/null 2>&1 &
    XVFB_PID=$!

    # Give Xvfb a moment to start
    sleep 2

    echo "Virtual display started on DISPLAY=$DISPLAY"

    # Function to cleanup Xvfb on exit
    cleanup() {
        echo "Cleaning up virtual display..."
        kill $XVFB_PID 2>/dev/null
        exit
    }

    # Set trap to cleanup on script exit
    trap cleanup EXIT INT TERM
fi

npm run dev
