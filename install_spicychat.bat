@echo off
cls
echo ================================================
echo   SpicyChat Analytics - Windows Setup
echo ================================================
echo.

REM Ensure Python exists
python --version >nul 2>&1
IF %ERRORLEVEL% NEQ 0 (
    echo ERROR: Python is not installed or not in PATH.
    echo Please install Python 3.10+ and try again.
    pause
    exit /b 1
)

echo Installing Python dependencies from requirements.txt...
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
IF %ERRORLEVEL% NEQ 0 (
    echo ERROR: Failed to install required Python packages.
    pause
    exit /b 1
)

echo.
echo Installing Playwright Chromium browser...
python -m playwright install chromium
IF %ERRORLEVEL% NEQ 0 (
    echo ERROR: Failed to install Playwright browser.
    pause
    exit /b 1
)

echo.
echo Initializing database...
python setup_spicychat.py --init-db
IF %ERRORLEVEL% NEQ 0 (
    echo ERROR: Database initialization failed.
    pause
    exit /b 1
)

echo.
echo Setup complete!
echo You may now launch the dashboard with:
echo     python spicychat_analytics.py
echo.
pause
