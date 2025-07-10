@echo off
echo Starting VexDB Development Server...
echo.
echo API Documentation will be available at: http://localhost:8000/docs
echo Health Check: http://localhost:8000/health
echo.
echo Press Ctrl+C to stop the server
echo.

venv\Scripts\python.exe -m uvicorn vexdb.main:app --reload --host 0.0.0.0 --port 8000