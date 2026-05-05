@echo off
echo [*] Dang cai dat thu vien cho Dashboard...
pip install fastapi uvicorn jinja2

echo.
echo [*] Khoi dong Realtime Dashboard tai http://localhost:8080 ...
cd dashboard
python -m uvicorn main:app --port 8080
pause
