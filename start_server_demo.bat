@echo off
echo Starting VexDB Server...
echo ========================

echo Activating virtual environment...
call venv\Scripts\activate

echo Starting server on http://localhost:8000
echo Press Ctrl+C to stop the server
echo.

python -m uvicorn vexdb.main:app --host 0.0.0.0 --port 8000 --reload

pause
