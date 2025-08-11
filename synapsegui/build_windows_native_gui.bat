@echo off
REM Fixed Windows build script using the patched CLI script
REM This version pre-loads all dependencies before synapseclient import
REM Usage: build_windows_native_gui.bat [suffix]

set SUFFIX=%1
if not "%SUFFIX%"=="" set SUFFIX=-%SUFFIX%

echo Building Synapse Desktop Client for Windows...

REM Install required packages
echo Installing required packages...
pip install pyinstaller
pip install -e ..

if errorlevel 1 (
    echo ERROR: Failed to install dependencies
    exit /b 1
)

REM Clean previous builds
echo Cleaning previous builds...
if exist build rmdir /s /q build
if exist dist rmdir /s /q dist
if exist *.spec del *.spec

echo Building Windows executable...

REM Build using the fixed CLI script
pyinstaller ^
    --onefile ^
    --name "synapse-desktop-client%SUFFIX%.exe" ^
    --collect-all=synapseclient ^
    --windowed ^
    synapse_gui.py

if errorlevel 1 (
    echo ERROR: Build failed
    exit /b 1
)

echo Build complete!
echo Executable location: dist\synapse-desktop-client%SUFFIX%.exe

REM Show file size
for %%I in (dist\synapse-desktop-client%SUFFIX%.exe) do echo File size: %%~zI bytes

echo.
echo SUCCESS: Synapse Desktop Client built!
echo.
echo Usage:
echo   Double-click dist\synapse-desktop-client%SUFFIX%.exe to open the GUI interface
echo   Or run from command line: dist\synapse-desktop-client%SUFFIX%.exe

pause
