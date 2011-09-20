@ECHO OFF
SETLOCAL

REM assumes we're being run from top-level directory as:
REM scripts\build-release.bat

CALL scripts\vc.bat
IF ERRORLEVEL 1 EXIT /B 1

python -u -B scripts\build-release.py %1 %2 %3
