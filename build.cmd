@echo off

SET FAKE_PATH=.fake
SET PAKET_PATH=.paket

IF NOT EXIST "%PAKET_PATH%\paket.exe" (dotnet tool install paket --tool-path ./%PAKET_PATH%)
IF NOT EXIST "%FAKE_PATH%\fake.exe" (dotnet tool install fake-cli --tool-path ./%FAKE_PATH%)

"%FAKE_PATH%/fake.exe" run build.fsx