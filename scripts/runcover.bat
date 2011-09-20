call "%ProgramFiles%\Microsoft Visual Studio 10.0\Common7\Tools\vsvars32.bat"
@IF ERRORLEVEL 1 GOTO TRYX86
@GOTO BUILD

:TRYX86
call "%ProgramFiles(x86)%\Microsoft Visual Studio 10.0\Common7\Tools\vsvars32.bat"
@IF ERRORLEVEL 1 GOTO NEEDSVS

:BUILD
cd csharp
devenv Volante.sln /Rebuild ReleaseFull /Project Tests
@IF ERRORLEVEL 1 GOTO FAILEDCOMPILE


@set O=bin\ReleaseFull
@cd %O%

..\..\..\tools\opencover\OpenCover.Console -target:Tests.exe -register:user -filter:+[Volante*]* -output:opencover.xml >opencover.err.txt
@IF ERRORLEVEL 1 GOTO OPENCOVERFAILED
@cd ..\..\..

python scripts\opencover-to-html.py csharp\%O%\opencover.xml ..\volante-cov
@IF ERRORLEVEL 1 GOTO OPENCOVERTOHTMLFAILED

@goto END

:OPENCOVERFAILED
@echo OpenCover failed
@goto END

:OPENCOVERTOHTMLFAILED
@echo opencover-to-html.py failed
@goto END

:BUILDFAILED
@echo Build failed
@goto END

:NEEDSVS
@echo Visual Studio 2010 doesn't seem to be installed
@goto END

:END
