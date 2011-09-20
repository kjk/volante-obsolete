call "%ProgramFiles%\Microsoft Visual Studio 10.0\Common7\Tools\vsvars32.bat"
@IF ERRORLEVEL 1 GOTO TRYX86
@GOTO BUILD

:TRYX86
call "%ProgramFiles(x86)%\Microsoft Visual Studio 10.0\Common7\Tools\vsvars32.bat"
@IF ERRORLEVEL 1 GOTO NEEDSVS

:BUILD
cd csharp
devenv Volante.sln /Rebuild Debug /Project Tests
@IF ERRORLEVEL 1 GOTO FAILEDCOMPILE

@cd bin\Debug
Tests.exe -fast

@goto END

:FAILEDCOMPILE
@echo "Compilcation failed"
@goto END

:NEEDSVS
@echo Visual Studio 2010 doesn't seem to be installed
@goto END

:END
