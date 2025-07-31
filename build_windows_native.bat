@echo off
REM Fixed Windows build script using the patched CLI script
REM This version pre-loads all dependencies before synapseclient import

echo Building Minimal Synapse CLI for Windows (fixed version)...

REM Install required packages
echo Installing required packages...
call .venv\Scripts\activate
uv pip install pyinstaller
uv pip install -e .

if errorlevel 1 (
    echo ERROR: Failed to install dependencies
    exit /b 1
)

REM Clean previous builds
echo Cleaning previous builds...
if exist build rmdir /s /q build
if exist dist rmdir /s /q dist
if exist *.spec del *.spec

echo Building Windows executable (fixed version)...

REM Build using the fixed CLI script
pyinstaller ^
    --onefile ^
    --name "minimal-synapse-windows.exe" ^
    --collect-all=synapseclient ^
    --console ^
    minimal_synapse_cli.py

if errorlevel 1 (
    echo ERROR: Build failed
    exit /b 1
)

echo Build complete!
echo Executable location: dist\minimal-synapse-windows.exe

REM Show file size
for %%I in (dist\minimal-synapse-windows.exe) do echo File size: %%~zI bytes

REM Test the executable
echo Testing executable...
dist\minimal-synapse-windows.exe --help
if errorlevel 1 (
    echo ✗ Executable test failed
    exit /b 1
) else (
    echo ✓ Executable test passed
)

echo.
echo SUCCESS: Fixed Windows executable built!
echo.
echo Usage:
echo   dist\minimal-synapse-windows.exe get syn123
echo   dist\minimal-synapse-windows.exe store myfile.txt --parentid syn456

pause
