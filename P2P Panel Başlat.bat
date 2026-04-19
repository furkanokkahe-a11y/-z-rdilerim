@echo off
title P2P Fatura Paneli
setlocal
cd /d "%~dp0"

set "PYTHON_EXE=C:\Users\dofuk\AppData\Local\Programs\Python\Python312\python.exe"
set "APP_URL=http://localhost:8000"
set "CHROME_EXE="

if exist "C:\Program Files\Google\Chrome\Application\chrome.exe" set "CHROME_EXE=C:\Program Files\Google\Chrome\Application\chrome.exe"
if not defined CHROME_EXE if exist "C:\Program Files (x86)\Google\Chrome\Application\chrome.exe" set "CHROME_EXE=C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"
if not defined CHROME_EXE if exist "%LocalAppData%\Google\Chrome\Application\chrome.exe" set "CHROME_EXE=%LocalAppData%\Google\Chrome\Application\chrome.exe"

if not exist "%PYTHON_EXE%" (
	echo Python 3.12 bulunamadi: %PYTHON_EXE%
	echo Lutfen Python 3.12 kurulumunu kontrol edin.
	pause
	exit /b 1
)

if not defined CHROME_EXE (
	echo Chrome bulunamadi. Lutfen Google Chrome kurun.
	pause
	exit /b 1
)

echo P2P Panel baslatiliyor...
echo Tarayicida: %APP_URL%
echo Python: %PYTHON_EXE%
echo.

set "PORT_STATUS="
for /f %%S in ('powershell -NoProfile -Command "try { $response = Invoke-WebRequest -UseBasicParsing 'http://localhost:8000/api/expenses/months' -TimeoutSec 3; if ($response.StatusCode -eq 200) { 'RUNNING' } else { 'BUSY' } } catch { if (Get-NetTCPConnection -LocalPort 8000 -State Listen -ErrorAction SilentlyContinue) { 'BUSY' } else { 'FREE' } }"') do set "PORT_STATUS=%%S"

if /I "%PORT_STATUS%"=="RUNNING" (
	echo P2P panel zaten calisiyor. Tarayici aciliyor...
	start "Chrome" "%CHROME_EXE%" "%APP_URL%"
	exit /b 0
)

if /I "%PORT_STATUS%"=="BUSY" (
	echo 8000 portu baska bir uygulama tarafindan kullaniliyor.
	echo Lutfen portu bosaltin ya da uygulamayi kapatin.
	pause
	exit /b 1
)

start "Chrome" "%CHROME_EXE%" "%APP_URL%"
"%PYTHON_EXE%" -m uvicorn gib_fatura_api:app --host 0.0.0.0 --port 8000

pause
