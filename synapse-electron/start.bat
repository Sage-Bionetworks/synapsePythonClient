@echo off
REM Synapse Desktop Client - Windows Startup Script

echo Starting Synapse Desktop Client...
echo.

REM Check if Node.js is installed
where node >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Node.js is not installed or not in PATH
    echo Please install Node.js from https://nodejs.org/
    pause
    exit /b 1
)

REM Check if Python is installed
where python >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Python is not installed or not in PATH
    echo Please install Python from https://python.org/
    pause
    exit /b 1
)

REM Change to the synapse-electron directory
cd /d %~dp0

REM Check if node_modules exists
if not exist "node_modules" (
    echo Installing Node.js dependencies...
    npm install
    if %ERRORLEVEL% NEQ 0 (
        echo ERROR: Failed to install Node.js dependencies
        pause
        exit /b 1
    )
)

REM Check if Python backend dependencies are installed
cd backend
if not exist "venv" (
    echo Creating Python virtual environment...
    python -m venv venv
    call venv\Scripts\activate
    echo Installing Python dependencies...
    cd ..
    pip install -e .[electron]
    cd backend
) else (
    call venv\Scripts\activate
)

cd ..

REM Start the application
echo.
echo Starting Synapse Desktop Client...
echo Backend will start on http://localhost:8000
echo WebSocket will start on ws://localhost:8001
echo.

npm run dev

pause
