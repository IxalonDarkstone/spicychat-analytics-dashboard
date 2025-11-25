@echo off
title SpicyChat Analytics Installer

echo ===========================================
echo  SpicyChat Analytics - One Click Installer
echo ===========================================
echo.

REM Ensure Python exists
python --version >nul 2>&1
IF %ERRORLEVEL% NEQ 0 (
    echo Python is not installed or not in PATH.
    echo Install Python 3.10+ and try again.
    pause
    exit /b
)

echo Installing required Python modules...
python -m pip install --upgrade pip
python -m pip install flask pandas numpy matplotlib playwright requests pytz typesense openpyxl

echo Installing Playwright browsers...
python -m playwright install

echo Creating project folders...
mkdir data
mkdir logs
mkdir charts
mkdir static
mkdir static\charts
mkdir templates

echo Creating empty database if not exists...
python setup_spicychat.py --init-db

echo Done!
echo -------------------------------------------
echo You can now run:  python spicychat_analytics.py
echo -------------------------------------------
pause
