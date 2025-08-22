@echo off
REM Build script for Synapse Desktop Client (Electron + Python Backend)
REM This script creates a complete packaged application with both frontend and backend
REM Usage: build_electron_app.bat

echo Building Synapse Desktop Client (Electron + Python Backend)...

REM Ensure we're in the project root
cd /d "%~dp0"

REM Activate virtual environment if it exists
if exist ".venv\Scripts\activate.bat" (
    echo Activating virtual environment...
    call .venv\Scripts\activate.bat
) else (
    echo Warning: Virtual environment not found at .venv\Scripts\activate.bat
    echo Continuing with system Python...
)

REM Check required tools
echo Checking required tools...
where node >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Node.js is not installed or not in PATH
    echo Please install Node.js from https://nodejs.org/
    exit /b 1
)

where python >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Python is not installed or not in PATH
    echo Please install Python from https://python.org/
    exit /b 1
)

REM Install Python dependencies with electron extras
if "%SKIP_DEPENDENCY_INSTALL%"=="1" (
    echo Skipping Python dependency installation (CI mode)
) else (
    echo Installing Python dependencies...
    where uv >nul 2>nul
    if %ERRORLEVEL% EQU 0 (
        echo Using uv package manager...
        uv pip install "pyinstaller>=6.14.0" "pyinstaller-hooks-contrib>=2024.0"
        uv pip install -e .[electron]
    ) else (
        echo Using pip package manager...
        python -m pip install "pyinstaller>=6.14.0" "pyinstaller-hooks-contrib>=2024.0"
        python -m pip install -e .[electron]
    )
    if errorlevel 1 (
        echo ERROR: Failed to install Python dependencies
        exit /b 1
    )
)

@REM REM Verify PyInstaller version
@REM echo Verifying PyInstaller installation...
@REM python -c "import pyinstaller; print('PyInstaller version:', pyinstaller.__version__)"
@REM if errorlevel 1 (
@REM     echo ERROR: PyInstaller not properly installed
@REM     exit /b 1
@REM )

REM Build Python backend with PyInstaller
echo Building Python backend...
cd synapse-electron\backend

REM Clean previous builds
if exist dist rmdir /s /q dist
if exist build rmdir /s /q build
if exist *.spec del *.spec

REM Build using the same pattern as the working build
pyinstaller ^
    --onefile ^
    --name "synapse-backend" ^
    --collect-all=synapseclient ^
    --collect-all=fastapi ^
    --collect-all=uvicorn ^
    --collect-all=starlette ^
    --collect-all=pydantic ^
    --collect-all=websockets ^
    --collect-all=synapsegui ^
    --paths "..\.." ^
    --paths "..\..\synapsegui" ^
    --paths "..\..\synapseclient" ^
    --console ^
    server.py

echo Checking if executable was created...
if not exist "dist\synapse-backend.exe" (
    echo ERROR: PyInstaller failed to create executable
    echo Check the output above for errors
    exit /b 1
)

if errorlevel 1 (
    echo ERROR: Python backend build failed
    exit /b 1
)

echo Python backend built successfully

REM Go back to electron directory
cd ..

@REM REM Install Node.js dependencies
@REM echo.
@REM echo ========================================
@REM echo Installing Node.js dependencies...
@REM echo ========================================
@REM echo DEBUG: About to run npm install
@REM npm install --verbose
@REM echo DEBUG: npm install command completed
@REM set NPM_INSTALL_EXIT=%ERRORLEVEL%
@REM echo DEBUG: npm install exit code: %NPM_INSTALL_EXIT%
@REM REM Check if node_modules exists to verify successful install
@REM if not exist "node_modules" (
@REM     echo ERROR: Failed to install Node.js dependencies - node_modules directory not found
@REM     exit /b 1
@REM )
@REM echo DEBUG: Node.js dependencies installed successfully
@REM echo DEBUG: Continuing to Electron build step...

REM Build Electron application
echo.
echo ========================================
echo Building Electron application...
echo ========================================
echo DEBUG: About to run npm run dist
echo Running: npm run dist
npm run dist --verbose
if errorlevel 1 (
    echo ERROR: Electron build failed
    echo Check the output above for details
    exit /b 1
)

echo.
echo Build complete!
echo.
echo Electron application packages are in: synapse-electron\dist\
echo Python backend executable is in: synapse-electron\backend\dist\

REM Show built files
echo Built files:
if exist dist (
    dir /b dist\*.exe 2>nul
    dir /b dist\*.dmg 2>nul
    dir /b dist\*.AppImage 2>nul
)

echo.
echo SUCCESS: Synapse Desktop Client built!

pause
