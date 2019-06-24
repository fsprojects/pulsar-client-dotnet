@echo off

SET TOOL_PATH=.fake

IF NOT EXIST "%TOOL_PATH%\fake.exe" (dotnet tool install fake-cli --tool-path ./%TOOL_PATH%)

".paket\paket.exe" restore
if errorlevel 1 (exit /b %errorlevel%)

setlocal

"%TOOL_PATH%/fake.exe" run build.fsx

endlocal