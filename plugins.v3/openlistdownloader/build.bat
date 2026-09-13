@echo off
chcp 65001 >nul
setlocal enabledelayedexpansion

REM ============================================================
REM  OpenListDownloader frontend build script
REM  Output -> plugin root /dist/assets/
REM  NOTE: always run this script from the real drive path;
REM        building through a symlink / subst drive letter can
REM        make the @originjs federation build fail (drive
REM        letter mismatch).
REM ============================================================

REM Ensure Node.js is on PATH
set "NODE_PATH=C:\Program Files\nodejs"
if exist "%NODE_PATH%\npm.cmd" (
    set "PATH=%NODE_PATH%;%PATH%"
)

REM cd to frontend directory (relative to this script, no hard-coded path)
cd /d "%~dp0frontend"

echo [1/3] Clean old dist ...
if exist "..\dist" (
    rd /s /q "..\dist"
    echo     dist removed
) else (
    echo     dist not exists, skip
)

echo [2/3] Running vite build ...
call npm run build
if errorlevel 1 (
    echo.
    echo [ERROR] Build failed, check output above.
    pause
    exit /b 1
)

echo [3/3] Build OK!
echo.
echo Output: ..\dist\assets\
echo   - remoteEntry.js
echo   - __federation_expose_AppPage*.js
echo   - __federation_expose_Page*.js
echo   - __federation_expose_Config*.js
echo.
echo Package this plugin folder as zip and install into MoviePilot.
echo.
pause
